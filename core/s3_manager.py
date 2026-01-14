"""
Менеджер для работы с S3 хранилищами
"""
import boto3
import os
import threading
from typing import Dict, Any, Optional, Tuple, List
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError
import urllib3

# Подавляем предупреждения о небезопасных HTTPS запросах
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def normalize_endpoint(endpoint: str) -> str:
    """
    Нормализовать endpoint URL - добавить протокол если его нет
    
    Args:
        endpoint: URL endpoint (может быть с протоколом или без)
    
    Returns:
        Нормализованный endpoint URL с протоколом
    """
    if not endpoint or not isinstance(endpoint, str):
        return ""
    
    endpoint = endpoint.strip()
    
    if not endpoint:
        return ""
    
    # Проверяем, есть ли уже протокол
    has_http = endpoint.startswith("http://")
    has_https = endpoint.startswith("https://")
    
    # Если endpoint уже содержит протокол, проверяем соответствие порта и протокола
    if has_http or has_https:
        # Убираем протокол для проверки порта
        if has_https:
            endpoint_without_proto = endpoint[8:]  # "https://" = 8 символов
        else:
            endpoint_without_proto = endpoint[7:]   # "http://" = 7 символов
        
        # Проверяем порт и исправляем протокол если нужно
        if ":443" in endpoint_without_proto and has_http:
            # Порт 443, но протокол HTTP - исправляем на HTTPS
            return f"https://{endpoint_without_proto}"
        elif ":80" in endpoint_without_proto and has_https:
            # Порт 80, но протокол HTTPS - исправляем на HTTP
            return f"http://{endpoint_without_proto}"
        else:
            # Протокол и порт соответствуют, возвращаем как есть
            return endpoint
    
    # Если протокола нет, определяем по порту
    if ":443" in endpoint:
        # Порт 443 - используем HTTPS
        return f"https://{endpoint}"
    elif ":80" in endpoint:
        # Порт 80 - используем HTTP
        return f"http://{endpoint}"
    elif ":" in endpoint:
        # Есть другой порт, предполагаем HTTP
        return f"http://{endpoint}"
    else:
        # Нет порта, предполагаем HTTPS
        return f"https://{endpoint}"


def create_s3_client(
    access_key: str,
    secret_key: str,
    region: str = 'us-east-1',
    endpoint: Optional[str] = None,
    config: Optional[Any] = None
) -> Any:
    """
    Создать клиент S3
    
    Args:
        access_key: Access Key ID
        secret_key: Secret Access Key
        region: Регион (по умолчанию us-east-1)
        endpoint: Endpoint URL (опционально)
        config: Конфигурация boto3 (опционально)
    
    Returns:
        boto3 S3 client
    """
    s3_client_kwargs = {
        'aws_access_key_id': access_key,
        'aws_secret_access_key': secret_key,
        'region_name': region
    }
    
    if endpoint:
        normalized_endpoint = normalize_endpoint(endpoint)
        if normalized_endpoint:
            s3_client_kwargs['endpoint_url'] = normalized_endpoint
            # В новых версиях boto3 (>=1.26.0) параметр use_ssl устарел
            # boto3 автоматически определяет SSL на основе протокола в endpoint_url
            # Если endpoint начинается с http://, SSL не используется
            # Если https:// - используется SSL
    
    if config:
        s3_client_kwargs['config'] = config
    
    return boto3.client('s3', **s3_client_kwargs)


def check_bucket_availability(
    bucket_name: str,
    access_key: str,
    secret_key: str,
    region: str = 'us-east-1',
    endpoint: Optional[str] = None,
    timeout: int = 15
) -> Tuple[bool, str, str]:
    """
    Проверить доступность S3 бакета
    
    Выполняет полную проверку: загрузка тестового файла, чтение и удаление.
    Использует прямой HTTP-запрос для загрузки (обход проблемы с chunked encoding)
    и boto3 для чтения и удаления.
    
    Args:
        bucket_name: Имя бакета
        access_key: Access Key ID
        secret_key: Secret Access Key
        region: Регион (по умолчанию us-east-1)
        endpoint: Endpoint URL (опционально)
        timeout: Таймаут операции в секундах (по умолчанию 15)
    
    Returns:
        Tuple[bool, str, str]: (успех, результат, детали)
        - успех: True если проверка прошла успешно, False в противном случае
        - результат: "Успешно", "Ошибка", "Ошибка доступа", "Ошибка подключения" и т.д.
        - детали: Подробное описание результата или ошибки
    """
    # Валидация входных данных
    if not bucket_name or not isinstance(bucket_name, str) or not bucket_name.strip():
        return False, "Ошибка конфигурации", "Имя бакета не указано или имеет неверный формат."
    
    if not access_key or not isinstance(access_key, str):
        return False, "Ошибка конфигурации", "Access Key ID не указан или имеет неверный формат."
    
    if not secret_key or not isinstance(secret_key, str):
        return False, "Ошибка конфигурации", "Secret Access Key не указан или имеет неверный формат."
    
    bucket_name_str = bucket_name.strip()
    test_key = '__backup_manager_test_file__'
    test_content = b'backup_manager_test'
    
    # Выполняем операцию в отдельном потоке с таймаутом
    operation_result = {'success': False, 'error': None}
    operation_complete = threading.Event()
    
    def perform_operation():
        """Выполнить операцию проверки в отдельном потоке"""
        try:
            from botocore.config import Config as BotoConfig
            from io import BytesIO
            
            # Определяем, является ли endpoint HTTP
            is_http_endpoint = False
            if endpoint:
                normalized_endpoint = normalize_endpoint(endpoint)
                if normalized_endpoint and normalized_endpoint.startswith('http://'):
                    is_http_endpoint = True
            
            # Проверяем HTTP endpoints - boto3 не поддерживает HTTP endpoints из-за ошибки рекурсии
            if is_http_endpoint:
                operation_result['error'] = Exception(
                    "HTTP endpoints не поддерживаются через boto3 из-за известной проблемы "
                    "(maximum recursion depth exceeded) в новых версиях библиотеки.\n\n"
                    "Рекомендации:\n"
                    "1. Используйте HTTPS endpoint вместо HTTP\n"
                    "2. Если HTTP обязателен, используйте старую версию boto3 (<1.26.0) или другую библиотеку"
                )
                return
            
            # Создаём конфигурацию с отключенным chunked encoding для совместимости
            upload_config = BotoConfig(
                s3={
                    'addressing_style': 'path',  # Path-style для совместимости
                    'payload_signing_enabled': False,  # Отключаем подпись payload
                },
                parameter_validation=False,  # Отключаем валидацию параметров
                retries={'max_attempts': 1, 'mode': 'standard'}
            )
            
            # Создаём клиент S3 для чтения и удаления (стандартная конфигурация)
            s3_client = create_s3_client(access_key, secret_key, region, endpoint)
            upload_client = create_s3_client(access_key, secret_key, region, endpoint, config=upload_config)
            
            # 1. Загружаем тестовый файл - пробуем разные методы
            upload_success = False
            upload_error = None
            
            # Метод 1: Прямой HTTP-запрос через requests (только для HTTPS)
            try:
                from requests_aws4auth import AWS4Auth
                import requests
                from urllib.parse import urlparse, quote
                
                if endpoint:
                    normalized_endpoint = normalize_endpoint(endpoint)
                    if normalized_endpoint:
                        parsed = urlparse(normalized_endpoint)
                        base_url = f"{parsed.scheme}://{parsed.netloc}"
                        object_url = f"{base_url}/{bucket_name_str}/{quote(test_key)}"
                    else:
                        object_url = f"https://{bucket_name_str}.s3.{region}.amazonaws.com/{quote(test_key)}"
                else:
                    object_url = f"https://{bucket_name_str}.s3.{region}.amazonaws.com/{quote(test_key)}"
                
                aws_auth = AWS4Auth(access_key, secret_key, region, 's3')
                verify_ssl = True
                
                put_response = requests.put(
                    object_url,
                    data=test_content,
                    auth=aws_auth,
                    timeout=10,
                    verify=verify_ssl,
                    headers={
                        'Content-Length': str(len(test_content)),
                        'Content-Type': 'application/octet-stream'
                    }
                )
                
                if put_response.status_code in [200, 204]:
                    upload_success = True
                else:
                    upload_error = Exception(f"HTTP {put_response.status_code}: {put_response.text[:200]}")
            except ImportError:
                # requests-aws4auth не установлена, используем boto3
                pass
            except Exception as e1:
                upload_error = e1
            else:
                # Для HTTPS: сначала пробуем requests (опционально), затем boto3
                # Метод 1: Прямой HTTP-запрос через requests (только для HTTPS)
                try:
                    from requests_aws4auth import AWS4Auth
                    import requests
                    from urllib.parse import urlparse, quote
                    
                    if endpoint:
                        normalized_endpoint = normalize_endpoint(endpoint)
                        if normalized_endpoint:
                            parsed = urlparse(normalized_endpoint)
                            base_url = f"{parsed.scheme}://{parsed.netloc}"
                            object_url = f"{base_url}/{bucket_name_str}/{quote(test_key)}"
                        else:
                            object_url = f"https://{bucket_name_str}.s3.{region}.amazonaws.com/{quote(test_key)}"
                    else:
                        object_url = f"https://{bucket_name_str}.s3.{region}.amazonaws.com/{quote(test_key)}"
                    
                    aws_auth = AWS4Auth(access_key, secret_key, region, 's3')
                    verify_ssl = True
                    
                    put_response = requests.put(
                        object_url,
                        data=test_content,
                        auth=aws_auth,
                        timeout=10,
                        verify=verify_ssl,
                        headers={
                            'Content-Length': str(len(test_content)),
                            'Content-Type': 'application/octet-stream'
                        }
                    )
                    
                    if put_response.status_code in [200, 204]:
                        upload_success = True
                    else:
                        upload_error = Exception(f"HTTP {put_response.status_code}: {put_response.text[:200]}")
                except ImportError:
                    # requests-aws4auth не установлена, используем boto3
                    pass
                except Exception as e1:
                    upload_error = e1
            
            # Метод 2-4: Используем boto3 (для HTTPS если requests не сработал, для HTTP уже использовали)
            if not upload_success and upload_client:
                try:
                    upload_client.put_object(
                        Bucket=bucket_name_str,
                        Key=test_key,
                        Body=test_content,
                        ContentLength=len(test_content)
                    )
                    upload_success = True
                except Exception as e2:
                    upload_error = e2
                    # Метод 3: Через upload_fileobj (BytesIO)
                    try:
                        upload_client.upload_fileobj(
                            BytesIO(test_content),
                            bucket_name_str,
                            test_key
                        )
                        upload_success = True
                    except Exception as e3:
                        upload_error = e3
                        # Метод 4: С конфигурацией S3v2 (для старых S3-совместимых сервисов)
                        try:
                            from botocore.config import Config as BotoConfig
                            s3v2_config = BotoConfig(
                                s3={
                                    'addressing_style': 'path',
                                    'payload_signing_enabled': False,
                                    'signature_version': 's3'  # S3v2 вместо S3v4
                                },
                                parameter_validation=False,
                                retries={'max_attempts': 1, 'mode': 'standard'}
                            )
                            s3v2_client = create_s3_client(access_key, secret_key, region, endpoint, config=s3v2_config)
                            s3v2_client.put_object(
                                Bucket=bucket_name_str,
                                Key=test_key,
                                Body=test_content,
                                ContentLength=len(test_content)
                            )
                            upload_success = True
                        except Exception as e4:
                            upload_error = e4
            
            if not upload_success:
                error_msg = f"Не удалось загрузить файл: {type(upload_error).__name__}"
                if upload_error:
                    error_msg += f" - {str(upload_error)}"
                operation_result['error'] = Exception(error_msg)
                return
            
            # 2. Читаем тестовый файл через boto3
            response = s3_client.get_object(
                Bucket=bucket_name_str,
                Key=test_key
            )
            read_content = response['Body'].read()
            
            # 3. Проверяем содержимое
            if read_content != test_content:
                # Пытаемся удалить файл
                try:
                    s3_client.delete_object(Bucket=bucket_name_str, Key=test_key)
                except:
                    pass
                operation_result['error'] = Exception("Содержимое файла не совпадает")
                return
            
            # 4. Удаляем тестовый файл через boto3
            s3_client.delete_object(
                Bucket=bucket_name_str,
                Key=test_key
            )
            
            # Все успешно
            operation_result['success'] = True
            
        except Exception as e:
            operation_result['error'] = e
        finally:
            operation_complete.set()
    
    # Запускаем операцию в отдельном потоке
    op_thread = threading.Thread(target=perform_operation, daemon=True)
    op_thread.start()
    
    # Ждём завершения операции с таймаутом
    if not operation_complete.wait(timeout=timeout):
        return False, "Ошибка", f"Таймаут при проверке доступности бакета '{bucket_name}'.\n\nВозможные причины:\n1. Проблемы с сетевым подключением\n2. Endpoint недоступен\n\nПопробуйте:\n- Проверить правильность endpoint URL\n- Проверить доступность сервера\n- Проверить настройки прокси (если используется)"
    
    # Проверяем результат операции
    if operation_result['success']:
        return True, "Успешно", f"Бакет '{bucket_name}' доступен.\n\nПроверка выполнена: загрузка, чтение и удаление тестового файла прошли успешно.\n\nУчётные данные корректны, подключение к S3 работает."
    elif operation_result['error']:
        error = operation_result['error']
        if isinstance(error, ClientError):
            error_code = error.response.get('Error', {}).get('Code', 'Unknown')
            error_message = error.response.get('Error', {}).get('Message', '')
            if error_code == 'NoSuchBucket':
                return False, "Ошибка", f"Бакет '{bucket_name}' не найден.\n\nПроверьте правильность имени бакета."
            elif error_code == '403' or error_code == 'AccessDenied':
                return False, "Ошибка доступа", f"Доступ к бакету '{bucket_name}' запрещён.\n\nПроверьте права доступа для указанных учётных данных."
            else:
                return False, "Ошибка", f"Ошибка при проверке доступности бакета '{bucket_name}': {error_code}\n\nДетали: {error_message}"
        elif isinstance(error, EndpointConnectionError):
            return False, "Ошибка подключения", f"Не удалось подключиться к endpoint.\n\nПроверьте URL endpoint и доступность сервера.\n\nОшибка: {str(error)}"
        elif isinstance(error, Exception) and ("ConnectionClosedError" in str(type(error)) or "Connection was closed" in str(error)):
            return False, "Ошибка подключения", f"Соединение было закрыто до получения ответа от сервера.\n\nВозможные причины:\n1. Неправильный протокол (HTTP вместо HTTPS или наоборот)\n2. Проблемы с сетевым подключением\n3. Endpoint недоступен\n\nПроверьте:\n- Правильность endpoint URL (протокол должен соответствовать порту: 443 = HTTPS, 80 = HTTP)\n- Доступность сервера\n- Настройки прокси (если используется)\n\nОшибка: {str(error)}"
        else:
            return False, "Ошибка", f"Ошибка при проверке доступности бакета '{bucket_name}': {type(error).__name__}\n\nДетали: {str(error)}"
    else:
        return False, "Ошибка", f"Неожиданная ошибка при проверке доступности бакета '{bucket_name}'."


def check_bucket_availability_sync(
    bucket_name: str,
    access_key: str,
    secret_key: str,
    region: str = 'us-east-1',
    endpoint: Optional[str] = None
) -> Tuple[bool, str, str]:
    """
    Проверить доступность S3 бакета (синхронная версия)
    
    Используется для тестирования. Не использует отдельный поток.
    
    Args:
        bucket_name: Имя бакета
        access_key: Access Key ID
        secret_key: Secret Access Key
        region: Регион (по умолчанию us-east-1)
        endpoint: Endpoint URL (опционально)
    
    Returns:
        Tuple[bool, str, str]: (успех, результат, детали)
    """
    # Валидация входных данных
    if not bucket_name or not isinstance(bucket_name, str) or not bucket_name.strip():
        return False, "Ошибка конфигурации", "Имя бакета не указано или имеет неверный формат."
    
    if not access_key or not isinstance(access_key, str):
        return False, "Ошибка конфигурации", "Access Key ID не указан или имеет неверный формат."
    
    if not secret_key or not isinstance(secret_key, str):
        return False, "Ошибка конфигурации", "Secret Access Key не указан или имеет неверный формат."
    
    bucket_name_str = bucket_name.strip()
    test_key = '__backup_manager_test_file__'
    test_content = b'backup_manager_test'
    
    try:
        from botocore.config import Config as BotoConfig
        from io import BytesIO
            
        # Создаём конфигурацию с отключенным chunked encoding для совместимости
        upload_config = BotoConfig(
            s3={
                'addressing_style': 'path',  # Path-style для совместимости
                'payload_signing_enabled': False,  # Отключаем подпись payload
            },
            parameter_validation=False,  # Отключаем валидацию параметров
            retries={'max_attempts': 1, 'mode': 'standard'}
        )
        
        # Создаём клиент S3 для чтения и удаления (стандартная конфигурация)
        s3_client = create_s3_client(access_key, secret_key, region, endpoint)
        
        # Создаём клиент S3 для загрузки с специальной конфигурацией
        upload_client = create_s3_client(access_key, secret_key, region, endpoint, config=upload_config)
        
        # 1. Загружаем тестовый файл - пробуем разные методы
        upload_success = False
        upload_error = None
        
        # Метод 1: Прямой HTTP-запрос через requests с правильной подписью (без chunked encoding)
        try:
            from requests_aws4auth import AWS4Auth
            import requests
            from urllib.parse import urlparse, quote
            
            if endpoint:
                normalized_endpoint = normalize_endpoint(endpoint)
                if normalized_endpoint:
                    parsed = urlparse(normalized_endpoint)
                    base_url = f"{parsed.scheme}://{parsed.netloc}"
                    object_url = f"{base_url}/{bucket_name_str}/{quote(test_key)}"
                else:
                    object_url = f"https://{bucket_name_str}.s3.{region}.amazonaws.com/{quote(test_key)}"
            else:
                object_url = f"https://{bucket_name_str}.s3.{region}.amazonaws.com/{quote(test_key)}"
            
            aws_auth = AWS4Auth(access_key, secret_key, region, 's3')
            verify_ssl = (endpoint and normalize_endpoint(endpoint).startswith('https://')) if endpoint else True
            
            # Используем requests с явным Content-Length и без chunked encoding
            put_response = requests.put(
                object_url,
                data=test_content,
                auth=aws_auth,
                timeout=10,
                verify=verify_ssl,
                headers={
                    'Content-Length': str(len(test_content)),
                    'Content-Type': 'application/octet-stream'
                }
            )
            
            if put_response.status_code in [200, 204]:
                upload_success = True
            else:
                upload_error = Exception(f"HTTP {put_response.status_code}: {put_response.text[:200]}")
        except ImportError:
            # requests-aws4auth не установлена, пробуем через boto3
            pass
        except Exception as e1:
            upload_error = e1
        
        # Метод 2: С явным ContentLength через upload_client (S3v4)
        if not upload_success:
            try:
                upload_client.put_object(
                    Bucket=bucket_name_str,
                    Key=test_key,
                    Body=test_content,
                    ContentLength=len(test_content)
                )
                upload_success = True
            except Exception as e2:
                upload_error = e2
                # Метод 3: Через upload_fileobj (BytesIO)
                try:
                    upload_client.upload_fileobj(
                        BytesIO(test_content),
                        bucket_name_str,
                        test_key
                    )
                    upload_success = True
                except Exception as e3:
                    upload_error = e3
                    # Метод 4: С конфигурацией S3v2 (для старых S3-совместимых сервисов)
                    try:
                        from botocore.config import Config as BotoConfig
                        s3v2_config = BotoConfig(
                            s3={
                                'addressing_style': 'path',
                                'payload_signing_enabled': False,
                                'signature_version': 's3'  # S3v2 вместо S3v4
                            },
                            parameter_validation=False,
                            retries={'max_attempts': 1, 'mode': 'standard'}
                        )
                        s3v2_client = create_s3_client(access_key, secret_key, region, endpoint, config=s3v2_config)
                        s3v2_client.put_object(
                            Bucket=bucket_name_str,
                            Key=test_key,
                            Body=test_content,
                            ContentLength=len(test_content)
                        )
                        upload_success = True
                    except Exception as e4:
                        upload_error = e4
        
        if not upload_success:
            error_msg = f"Ошибка при загрузке файла: {type(upload_error).__name__}"
            if upload_error:
                error_msg += f" - {str(upload_error)}"
            return False, "Ошибка", error_msg
        
        # 2. Читаем тестовый файл через boto3
        response = s3_client.get_object(
            Bucket=bucket_name_str,
            Key=test_key
        )
        read_content = response['Body'].read()
        
        # 3. Проверяем содержимое
        if read_content != test_content:
            # Пытаемся удалить файл
            try:
                s3_client.delete_object(Bucket=bucket_name_str, Key=test_key)
            except:
                pass
            return False, "Ошибка", "Содержимое файла не совпадает"
        
        # 4. Удаляем тестовый файл через boto3
        s3_client.delete_object(
            Bucket=bucket_name_str,
            Key=test_key
        )
        
        # Все успешно
        return True, "Успешно", f"Бакет '{bucket_name}' доступен.\n\nПроверка выполнена: загрузка, чтение и удаление тестового файла прошли успешно.\n\nУчётные данные корректны, подключение к S3 работает."
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_message = e.response.get('Error', {}).get('Message', '')
        if error_code == 'NoSuchBucket':
            return False, "Ошибка", f"Бакет '{bucket_name}' не найден.\n\nПроверьте правильность имени бакета."
        elif error_code == '403' or error_code == 'AccessDenied':
            return False, "Ошибка доступа", f"Доступ к бакету '{bucket_name}' запрещён.\n\nПроверьте права доступа для указанных учётных данных."
        else:
            return False, "Ошибка", f"Ошибка при проверке доступности бакета '{bucket_name}': {error_code}\n\nДетали: {error_message}"
    except EndpointConnectionError as e:
        return False, "Ошибка подключения", f"Не удалось подключиться к endpoint.\n\nПроверьте URL endpoint и доступность сервера.\n\nОшибка: {str(e)}"
    except Exception as e:
        error_str = str(e)
        if "ConnectionClosedError" in str(type(e)) or "Connection was closed" in error_str:
            return False, "Ошибка подключения", f"Соединение было закрыто до получения ответа от сервера.\n\nВозможные причины:\n1. Неправильный протокол (HTTP вместо HTTPS или наоборот)\n2. Проблемы с сетевым подключением\n3. Endpoint недоступен\n\nПроверьте:\n- Правильность endpoint URL (протокол должен соответствовать порту: 443 = HTTPS, 80 = HTTP)\n- Доступность сервера\n- Настройки прокси (если используется)\n\nОшибка: {error_str}"
        return False, "Ошибка", f"Ошибка при проверке доступности бакета '{bucket_name}': {type(e).__name__}\n\nДетали: {error_str}"


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
    
    Args:
        bucket_name: Имя бакета
        access_key: Access Key ID
        secret_key: Secret Access Key
        region: Регион (по умолчанию us-east-1)
        endpoint: Endpoint URL (опционально)
        prefix: Префикс для фильтрации объектов (опционально)
    
    Returns:
        Список словарей с информацией об объектах: [{"key": "...", "last_modified": datetime, "size": int}, ...]
    """
    try:
        s3_client = create_s3_client(access_key, secret_key, region, endpoint)
        objects = []
        
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects.append({
                        "key": obj['Key'],
                        "last_modified": obj['LastModified'],
                        "size": obj['Size']
                    })
        
        return objects
    except Exception as e:
        logger = __import__('core.logger', fromlist=['setup_logger']).setup_logger()
        logger.error(f"Ошибка при получении списка объектов из S3: {e}", exc_info=True)
        return []


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
    
    Args:
        bucket_name: Имя бакета
        object_key: Ключ объекта в S3
        access_key: Access Key ID
        secret_key: Secret Access Key
        region: Регион (по умолчанию us-east-1)
        endpoint: Endpoint URL (опционально)
    
    Returns:
        Словарь с метаданными или None если объект не найден
        {"last_modified": datetime, "size": int, "etag": str}
    """
    try:
        s3_client = create_s3_client(access_key, secret_key, region, endpoint)
        response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
        return {
            "last_modified": response['LastModified'],
            "size": response['ContentLength'],
            "etag": response.get('ETag', '')
        }
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == '404' or error_code == 'NoSuchKey':
            return None
        raise
    except Exception as e:
        logger = __import__('core.logger', fromlist=['setup_logger']).setup_logger()
        logger.error(f"Ошибка при получении метаданных объекта из S3: {e}", exc_info=True)
        return None


def upload_file_to_s3(
    file_path: str,
    bucket_name: str,
    object_key: str,
    access_key: str,
    secret_key: str,
    region: str = 'us-east-1',
    endpoint: Optional[str] = None
) -> Tuple[bool, Optional[str]]:
    """
    Загрузить файл в S3
    
    Использует прямой HTTP-запрос через requests для обхода проблем с trailing headers в boto3.
    Это единственный надежный способ для некоторых S3-совместимых хранилищ.
    
    Args:
        file_path: Путь к локальному файлу
        bucket_name: Имя бакета
        object_key: Ключ объекта в S3
        access_key: Access Key ID
        secret_key: Secret Access Key
        region: Регион (по умолчанию us-east-1)
        endpoint: Endpoint URL (опционально)
    
    Returns:
        Tuple[bool, Optional[str]]: (успех, сообщение_об_ошибке)
    """
    logger = __import__('core.logger', fromlist=['setup_logger']).setup_logger()
    
    try:
        # Получаем размер файла
        file_size = os.path.getsize(file_path)
        
        # Метод 1: Прямой HTTP-запрос через requests (обход проблемы с trailing headers)
        try:
            from requests_aws4auth import AWS4Auth
            import requests
            from urllib.parse import urlparse, quote
            import xml.etree.ElementTree as ET
            
            # Определяем базовый URL
            if endpoint:
                normalized_endpoint = normalize_endpoint(endpoint)
                if normalized_endpoint:
                    parsed = urlparse(normalized_endpoint)
                    base_url = f"{parsed.scheme}://{parsed.netloc}"
                else:
                    base_url = f"https://s3.{region}.amazonaws.com"
            else:
                base_url = f"https://s3.{region}.amazonaws.com"
            
            # Определяем, нужно ли проверять SSL
            verify_ssl = True
            if endpoint:
                normalized_endpoint = normalize_endpoint(endpoint)
                if normalized_endpoint and normalized_endpoint.startswith('http://'):
                    verify_ssl = False
            
            # Создаем аутентификацию AWS4
            aws_auth = AWS4Auth(access_key, secret_key, region, 's3')
            
            # Для файлов больше 50 МБ используем multipart upload через requests
            # Для меньших файлов используем простой PUT
            if file_size > 50 * 1024 * 1024:  # 50 МБ
                # Multipart upload через requests
                try:
                    # Шаг 1: Создать multipart upload
                    multipart_url = f"{base_url}/{bucket_name}/{quote(object_key)}?uploads"
                    init_response = requests.post(
                        multipart_url,
                        auth=aws_auth,
                        timeout=(30, 60),
                        verify=verify_ssl
                    )
                    
                    if init_response.status_code != 200:
                        raise Exception(f"Не удалось создать multipart upload: HTTP {init_response.status_code}")
                    
                    # Парсим uploadId из XML ответа
                    root = ET.fromstring(init_response.text)
                    upload_id = root.findtext('{http://s3.amazonaws.com/doc/2006-03-01/}UploadId')
                    if not upload_id:
                        raise Exception("Не удалось получить UploadId из ответа")
                    
                    # Шаг 2: Загружаем части файла
                    part_size = 10 * 1024 * 1024  # 10 МБ на часть
                    parts = []
                    part_number = 1
                    
                    with open(file_path, 'rb') as f:
                        while True:
                            part_data = f.read(part_size)
                            if not part_data:
                                break
                            
                            # Загружаем часть
                            part_url = f"{base_url}/{bucket_name}/{quote(object_key)}?partNumber={part_number}&uploadId={upload_id}"
                            # Таймаут для части: 60 сек на подключение, 600 сек на загрузку
                            part_response = requests.put(
                                part_url,
                                data=part_data,
                                auth=aws_auth,
                                timeout=(60, 600),
                                verify=verify_ssl,
                                headers={
                                    'Content-Length': str(len(part_data)),
                                    'Content-Type': 'application/octet-stream'
                                }
                            )
                            
                            if part_response.status_code not in [200, 204]:
                                raise Exception(f"Не удалось загрузить часть {part_number}: HTTP {part_response.status_code}")
                            
                            # Сохраняем ETag части
                            etag = part_response.headers.get('ETag', '').strip('"')
                            parts.append({'PartNumber': part_number, 'ETag': etag})
                            
                            part_number += 1
                    
                    # Шаг 3: Завершаем multipart upload
                    complete_url = f"{base_url}/{bucket_name}/{quote(object_key)}?uploadId={upload_id}"
                    
                    # Формируем XML для завершения
                    complete_xml = ET.Element('CompleteMultipartUpload')
                    for part in parts:
                        part_elem = ET.SubElement(complete_xml, 'Part')
                        ET.SubElement(part_elem, 'PartNumber').text = str(part['PartNumber'])
                        ET.SubElement(part_elem, 'ETag').text = part['ETag']
                    
                    complete_xml_str = ET.tostring(complete_xml, encoding='utf-8', method='xml')
                    
                    complete_response = requests.post(
                        complete_url,
                        data=complete_xml_str,
                        auth=aws_auth,
                        timeout=(30, 60),
                        verify=verify_ssl,
                        headers={
                            'Content-Type': 'application/xml',
                            'Content-Length': str(len(complete_xml_str))
                        }
                    )
                    
                    if complete_response.status_code in [200, 204]:
                        return True, None
                    else:
                        raise Exception(f"Не удалось завершить multipart upload: HTTP {complete_response.status_code}")
                        
                except Exception as e_mp:
                    logger.error(f"Multipart upload через requests не удался: {e_mp}", exc_info=True)
                    return False, f"Ошибка multipart upload: {e_mp}"
            
            # Простой PUT для маленьких файлов
            # Читаем файл в память, чтобы избежать chunked encoding
            with open(file_path, 'rb') as f:
                file_content = f.read()
            
            object_url = f"{base_url}/{bucket_name}/{quote(object_key)}"
            # Увеличиваем таймаут: минимум 600 секунд, плюс 2 секунды на каждый МБ
            timeout_value = max(600, 120 + (file_size // (512 * 1024)))  # 2 сек на 512 КБ
            
            put_response = requests.put(
                object_url,
                data=file_content,
                auth=aws_auth,
                timeout=(60, timeout_value),  # Увеличиваем connect timeout до 60 сек
                verify=verify_ssl,
                headers={
                    'Content-Length': str(file_size),
                    'Content-Type': 'application/octet-stream'
                }
            )
            
            if put_response.status_code in [200, 204]:
                return True, None
            else:
                error_msg = f"HTTP {put_response.status_code}: {put_response.text[:200]}"
                logger.error(f"Ошибка при загрузке файла в S3 через requests: {error_msg}")
                return False, error_msg
                
        except ImportError:
            # requests-aws4auth не установлена
            error_msg = "requests-aws4auth не установлена. Установите: pip install requests-aws4auth"
            logger.error(error_msg)
            return False, error_msg
        except Exception as e1:
            # Если прямой HTTP-запрос не сработал
            error_msg = f"Ошибка при загрузке через requests: {e1}"
            logger.error(error_msg, exc_info=True)
            return False, error_msg
                    
    except FileNotFoundError:
        error_msg = f"Файл не найден: {file_path}"
        logger.error(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"Ошибка при загрузке файла в S3: {e}"
        logger.error(error_msg, exc_info=True)
        return False, error_msg
