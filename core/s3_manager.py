"""
Менеджер для работы с S3 хранилищами
Использует miniopy-async с единым event loop для предотвращения
утечки памяти и соединений.
"""

import asyncio
import atexit
import os
import threading
from io import BytesIO
from typing import Dict, Any, Optional, Tuple, List
from urllib.parse import urlparse

from miniopy_async import Minio
from miniopy_async.error import S3Error


# === Глобальный Event Loop Manager ===
class _AsyncLoopManager:
    """
    Менеджер для единого event loop в отдельном потоке.
    Предотвращает создание множества сессий и утечку памяти.
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._clients: Dict[str, Minio] = {}
        self._clients_lock = threading.Lock()
    
    def _start_loop(self):
        """Запустить event loop в текущем потоке"""
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()
    
    def get_loop(self) -> asyncio.AbstractEventLoop:
        """Получить или создать event loop"""
        if self._loop is None or not self._loop.is_running():
            with self._lock:
                if self._loop is None or not self._loop.is_running():
                    self._loop = asyncio.new_event_loop()
                    self._thread = threading.Thread(
                        target=self._start_loop,
                        daemon=True,
                        name="S3AsyncLoop"
                    )
                    self._thread.start()
                    # Ждём запуска loop
                    while not self._loop.is_running():
                        pass
        return self._loop
    
    def run_coroutine(self, coro, timeout: float = 300):
        """
        Выполнить корутину в глобальном event loop.
        
        Args:
            coro: Корутина для выполнения
            timeout: Таймаут в секундах (по умолчанию 5 минут)
        
        Returns:
            Результат выполнения корутины
        """
        loop = self.get_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result(timeout=timeout)
    
    def get_client(
        self,
        access_key: str,
        secret_key: str,
        region: str,
        endpoint: Optional[str]
    ) -> Minio:
        """
        Получить или создать клиент из кэша.
        Один клиент переиспользуется для всех операций с одинаковыми credentials.
        """
        from core.logger import setup_logger
        logger = setup_logger("S3Manager")
        
        if endpoint:
            host, secure = normalize_endpoint(endpoint)
            logger.debug(f"S3 endpoint: {endpoint} -> host={host}, secure={secure}")
        else:
            host = f"s3.{region}.amazonaws.com"
            secure = True
            logger.debug(f"S3 AWS endpoint: host={host}, region={region}")
        
        if not host:
            raise ValueError(f"Неверный endpoint: {endpoint}")
        
        client_key = f"{access_key}:{host}:{region}"
        
        with self._clients_lock:
            if client_key not in self._clients:
                logger.info(f"Создаю S3 клиент для {host}")
                self._clients[client_key] = Minio(
                    endpoint=host,
                    access_key=access_key,
                    secret_key=secret_key,
                    secure=secure,
                    region=region,
                )
            return self._clients[client_key]
    
    async def _close_clients_async(self):
        """Асинхронно закрыть все клиенты"""
        clients_to_close = []
        with self._clients_lock:
            clients_to_close = list(self._clients.values())
        
        for client in clients_to_close:
            try:
                # Закрываем внутреннюю aiohttp сессию клиента
                if hasattr(client, '_http') and client._http:
                    await client._http.close()
            except Exception:
                pass
    
    def shutdown(self):
        """Остановить event loop и очистить ресурсы"""
        # Сначала закрываем все клиенты
        if self._loop and self._loop.is_running():
            try:
                # Закрываем клиенты асинхронно
                future = asyncio.run_coroutine_threadsafe(
                    self._close_clients_async(), 
                    self._loop
                )
                future.result(timeout=10)
            except Exception:
                pass
            
            # Останавливаем event loop
            self._loop.call_soon_threadsafe(self._loop.stop)
            if self._thread:
                self._thread.join(timeout=5)
        
        with self._clients_lock:
            self._clients.clear()
        
        self._loop = None
        self._thread = None


# Глобальный менеджер
_manager = _AsyncLoopManager()

# Регистрируем очистку при выходе
atexit.register(_manager.shutdown)


def clear_all_clients():
    """Очистить все кэшированные клиенты (вызывать при старте для сброса)"""
    _manager.shutdown()
    _manager._initialized = False
    _manager.__init__()


def normalize_endpoint(endpoint: str) -> Tuple[str, bool]:
    """
    Нормализовать endpoint URL
    
    Args:
        endpoint: URL endpoint (может быть с протоколом или без)
    
    Returns:
        Tuple[str, bool]: (host:port, secure)
    """
    if not endpoint or not isinstance(endpoint, str):
        return "", True
    
    endpoint = endpoint.strip()
    if not endpoint:
        return "", True
    
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        parsed = urlparse(endpoint)
        host = parsed.netloc
        secure = parsed.scheme == "https"
    else:
        host = endpoint
        if ":443" in endpoint:
            secure = True
        elif ":80" in endpoint:
            secure = False
        else:
            secure = True
    
    return host, secure


def create_minio_client(
    access_key: str,
    secret_key: str,
    region: str = 'us-east-1',
    endpoint: Optional[str] = None
) -> Minio:
    """
    Получить или создать клиент MinIO/S3 из кэша
    
    Returns:
        Minio client
    """
    return _manager.get_client(access_key, secret_key, region, endpoint)


def clear_client_pool():
    """Очистить пул клиентов"""
    _manager.shutdown()


def shutdown_s3_connections():
    """Закрыть все S3 соединения перед выходом из программы"""
    _manager.shutdown()


async def _check_bucket_async(
    client: Minio,
    bucket_name: str
) -> Tuple[bool, str, str]:
    """Асинхронная проверка доступности бакета"""
    test_key = '__backup_manager_test__'
    test_content = b'backup_manager_test'
    
    try:
        # 1. Загружаем тестовый файл
        data = BytesIO(test_content)
        await client.put_object(
            bucket_name=bucket_name,
            object_name=test_key,
            data=data,
            length=len(test_content),
        )
        
        # 2. Проверяем метаданные
        stat = await client.stat_object(bucket_name, test_key)
        if stat.size != len(test_content):
            raise Exception("Размер файла не совпадает")
        
        # 3. Скачиваем и проверяем содержимое
        response = await client.get_object(bucket_name, test_key)
        downloaded = await response.read()
        # Закрываем response (close() не корутина, release() - корутина)
        response.close()
        await response.release()
        
        if downloaded != test_content:
            raise Exception("Содержимое файла не совпадает")
        
        # 4. Удаляем тестовый файл
        await client.remove_object(bucket_name, test_key)
        
        return True, "Успешно", f"Бакет '{bucket_name}' доступен.\n\nПроверка выполнена: загрузка, чтение и удаление тестового файла прошли успешно."
        
    except S3Error as e:
        error_code = e.code
        if error_code == 'NoSuchBucket':
            return False, "Ошибка", f"Бакет '{bucket_name}' не найден."
        elif error_code in ['AccessDenied', '403']:
            return False, "Ошибка доступа", f"Доступ к бакету '{bucket_name}' запрещён."
        else:
            return False, "Ошибка", f"Ошибка S3: {error_code} - {e.message}"
    except Exception as e:
        return False, "Ошибка", f"Ошибка: {type(e).__name__} - {str(e)}"


def check_bucket_availability(
    bucket_name: str,
    access_key: str,
    secret_key: str,
    region: str = 'us-east-1',
    endpoint: Optional[str] = None,
    timeout: int = 30
) -> Tuple[bool, str, str]:
    """
    Проверить доступность S3 бакета
    
    Returns:
        Tuple[bool, str, str]: (успех, результат, детали)
    """
    if not bucket_name or not bucket_name.strip():
        return False, "Ошибка", "Имя бакета не указано."
    if not access_key:
        return False, "Ошибка", "Access Key не указан."
    if not secret_key:
        return False, "Ошибка", "Secret Key не указан."
    
    try:
        client = create_minio_client(access_key, secret_key, region, endpoint)
        return _manager.run_coroutine(
            _check_bucket_async(client, bucket_name.strip()),
            timeout=timeout
        )
    except Exception as e:
        return False, "Ошибка", f"Ошибка подключения: {type(e).__name__} - {str(e)}"


# Алиас для обратной совместимости
check_bucket_availability_sync = check_bucket_availability


async def _list_objects_async(
    client: Minio,
    bucket_name: str,
    prefix: str = ""
) -> List[Dict[str, Any]]:
    """Асинхронное получение списка объектов"""
    objects = []
    async for obj in client.list_objects(bucket_name, prefix=prefix, recursive=True):
        objects.append({
            "key": obj.object_name,
            "last_modified": obj.last_modified,
            "size": obj.size or 0,
            "etag": obj.etag,
        })
    return objects


def list_s3_objects(
    bucket_name: str,
    access_key: str,
    secret_key: str,
    region: str = 'us-east-1',
    endpoint: Optional[str] = None,
    prefix: str = ""
) -> List[Dict[str, Any]]:
    """
    Получить список объектов в S3 бакете
    
    Returns:
        Список словарей: [{"key": str, "last_modified": datetime, "size": int, "etag": str}, ...]
    """
    try:
        client = create_minio_client(access_key, secret_key, region, endpoint)
        return _manager.run_coroutine(
            _list_objects_async(client, bucket_name, prefix)
        )
    except Exception as e:
        from core.logger import setup_logger
        logger = setup_logger()
        logger.error(f"Ошибка при получении списка объектов: {e}", exc_info=True)
        return []


async def _get_metadata_async(
    client: Minio,
    bucket_name: str,
    object_key: str
) -> Optional[Dict[str, Any]]:
    """Асинхронное получение метаданных объекта"""
    try:
        stat = await client.stat_object(bucket_name, object_key)
        return {
            "last_modified": stat.last_modified,
            "size": stat.size,
            "etag": stat.etag,
        }
    except S3Error as e:
        if e.code in ['NoSuchKey', '404']:
            return None
        raise


def get_s3_object_metadata(
    bucket_name: str,
    object_key: str,
    access_key: str,
    secret_key: str,
    region: str = 'us-east-1',
    endpoint: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Получить метаданные объекта в S3
    
    Returns:
        Словарь с метаданными или None
    """
    try:
        client = create_minio_client(access_key, secret_key, region, endpoint)
        return _manager.run_coroutine(
            _get_metadata_async(client, bucket_name, object_key)
        )
    except Exception as e:
        from core.logger import setup_logger
        logger = setup_logger()
        logger.error(f"Ошибка при получении метаданных: {e}", exc_info=True)
        return None


async def _upload_file_async(
    client: Minio,
    bucket_name: str,
    object_key: str,
    file_path: str,
    progress_callback=None
) -> Tuple[bool, Optional[str]]:
    """Асинхронная загрузка файла"""
    try:
        # Уведомляем о начале загрузки
        if progress_callback:
            file_size = os.path.getsize(file_path)
            filename = os.path.basename(file_path)
            # Вызываем callback с начальным прогрессом
            progress_callback(filename, 0, file_size)
        
        await client.fput_object(
            bucket_name=bucket_name,
            object_name=object_key,
            file_path=file_path,
        )
        
        # Уведомляем о завершении загрузки
        if progress_callback:
            progress_callback(filename, file_size, file_size)
        
        return True, None
    except S3Error as e:
        return False, f"S3 Error: {e.code} - {e.message}"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)}"


def upload_file_to_s3(
    file_path: str,
    bucket_name: str,
    object_key: str,
    access_key: str,
    secret_key: str,
    region: str = 'us-east-1',
    endpoint: Optional[str] = None,
    progress_callback=None
) -> Tuple[bool, Optional[str]]:
    """
    Загрузить файл в S3
    
    Args:
        progress_callback: Функция callback(filename, uploaded, total) для отслеживания прогресса
    
    Returns:
        Tuple[bool, Optional[str]]: (успех, сообщение_об_ошибке)
    """
    from core.logger import setup_logger
    logger = setup_logger("S3Upload")
    
    logger.debug(f"upload_file_to_s3: bucket={bucket_name}, region={region}, endpoint={endpoint}")
    
    if not os.path.exists(file_path):
        return False, f"Файл не найден: {file_path}"
    
    try:
        client = create_minio_client(access_key, secret_key, region, endpoint)
        return _manager.run_coroutine(
            _upload_file_async(client, bucket_name, object_key, file_path, progress_callback)
        )
    except Exception as e:
        logger.error(f"Ошибка при загрузке файла в S3: {e}")
        return False, f"{type(e).__name__}: {str(e)}"


def format_size(size_bytes: int) -> str:
    """Форматировать размер в человекочитаемый вид"""
    if size_bytes < 1024:
        return f"{size_bytes} Б"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} КБ"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} МБ"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} ГБ"


async def _download_file_async(
    client: Minio,
    bucket_name: str,
    object_key: str,
    file_path: str
) -> Tuple[bool, Optional[str]]:
    """Асинхронное скачивание файла"""
    try:
        await client.fget_object(
            bucket_name=bucket_name,
            object_name=object_key,
            file_path=file_path,
        )
        return True, None
    except S3Error as e:
        return False, f"S3 Error: {e.code} - {e.message}"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)}"


def download_file_from_s3(
    bucket_name: str,
    object_key: str,
    file_path: str,
    access_key: str,
    secret_key: str,
    region: str = 'us-east-1',
    endpoint: Optional[str] = None
) -> Tuple[bool, Optional[str]]:
    """
    Скачать файл из S3
    
    Returns:
        Tuple[bool, Optional[str]]: (успех, сообщение_об_ошибке)
    """
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        client = create_minio_client(access_key, secret_key, region, endpoint)
        return _manager.run_coroutine(
            _download_file_async(client, bucket_name, object_key, file_path)
        )
    except Exception as e:
        from core.logger import setup_logger
        logger = setup_logger()
        logger.error(f"Ошибка при скачивании файла: {e}", exc_info=True)
        return False, f"{type(e).__name__}: {str(e)}"


async def _delete_object_async(
    client: Minio,
    bucket_name: str,
    object_key: str
) -> Tuple[bool, Optional[str]]:
    """Асинхронное удаление объекта"""
    try:
        await client.remove_object(bucket_name, object_key)
        return True, None
    except S3Error as e:
        return False, f"S3 Error: {e.code} - {e.message}"
    except Exception as e:
        return False, f"{type(e).__name__}: {str(e)}"


def delete_s3_object(
    bucket_name: str,
    object_key: str,
    access_key: str,
    secret_key: str,
    region: str = 'us-east-1',
    endpoint: Optional[str] = None
) -> Tuple[bool, Optional[str]]:
    """
    Удалить объект из S3
    
    Returns:
        Tuple[bool, Optional[str]]: (успех, сообщение_об_ошибке)
    """
    try:
        client = create_minio_client(access_key, secret_key, region, endpoint)
        return _manager.run_coroutine(
            _delete_object_async(client, bucket_name, object_key)
        )
    except Exception as e:
        from core.logger import setup_logger
        logger = setup_logger()
        logger.error(f"Ошибка при удалении объекта: {e}", exc_info=True)
        return False, f"{type(e).__name__}: {str(e)}"
