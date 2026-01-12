"""
Менеджер для работы с S3 хранилищами
"""
import boto3
import threading
from typing import Dict, Any, Optional, Tuple
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
            if normalized_endpoint.startswith('http://'):
                s3_client_kwargs['use_ssl'] = False
    
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
