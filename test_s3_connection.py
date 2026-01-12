#!/usr/bin/env python3
"""
Скрипт для тестирования подключения к S3
Использование: python test_s3_connection.py
"""

import sys
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError
import urllib3

# Подавляем предупреждения о небезопасных HTTPS запросах
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def test_s3_connection():
    """Тестирование подключения к S3"""
    
    print("=" * 60)
    print("Тестирование подключения к S3")
    print("=" * 60)
    print()
    
    # Ввод параметров подключения
    print("Введите параметры подключения:")
    print()
    
    endpoint = input("Endpoint URL (оставьте пустым для AWS S3): ").strip()
    if not endpoint:
        endpoint = None
    else:
        # Нормализация endpoint
        if not endpoint.startswith("http://") and not endpoint.startswith("https://"):
            # Проверяем порт - 443 обычно для HTTPS, 80 для HTTP
            if ":443" in endpoint:
                endpoint = f"https://{endpoint}"
            elif ":80" in endpoint:
                endpoint = f"http://{endpoint}"
            elif ":" in endpoint:
                # Если есть другой порт, предполагаем HTTP
                endpoint = f"http://{endpoint}"
            else:
                # Нет порта - предполагаем HTTPS
                endpoint = f"https://{endpoint}"
        print(f"Нормализованный endpoint: {endpoint}")
    
    bucket_name = input("Имя бакета: ").strip()
    if not bucket_name:
        print("Ошибка: Имя бакета обязательно!")
        return
    
    access_key = input("Access Key ID: ").strip()
    if not access_key:
        print("Ошибка: Access Key ID обязателен!")
        return
    
    secret_key = input("Secret Access Key: ").strip()
    if not secret_key:
        print("Ошибка: Secret Access Key обязателен!")
        return
    
    region = input("Регион (по умолчанию us-east-1): ").strip()
    if not region:
        region = 'us-east-1'
    
    print()
    print("=" * 60)
    print("Параметры подключения:")
    print(f"  Endpoint: {endpoint or 'По умолчанию (AWS S3)'}")
    print(f"  Bucket: {bucket_name}")
    print(f"  Access Key: {access_key[:8]}...")
    print(f"  Region: {region}")
    print("=" * 60)
    print()
    
    # Создаём клиент S3
    s3_client_kwargs = {
        'aws_access_key_id': access_key,
        'aws_secret_access_key': secret_key,
        'region_name': region
    }
    
    if endpoint:
        s3_client_kwargs['endpoint_url'] = endpoint
        if endpoint.startswith('http://'):
            s3_client_kwargs['use_ssl'] = False
        else:
            s3_client_kwargs['use_ssl'] = True
    
    print("Создание клиента S3...")
    try:
        from botocore.config import Config
        
        # Создаём конфигурацию, которая отключает chunked encoding
        # Это нужно для некоторых S3-совместимых сервисов
        boto_config = Config(
            s3={
                'addressing_style': 'path',  # Path-style для совместимости
                'payload_signing_enabled': False,  # Отключаем подпись payload
            },
            # Отключаем multipart upload для маленьких файлов
            retries={'max_attempts': 3, 'mode': 'standard'},
            # Отключаем параметрическую валидацию, чтобы избежать проблем с chunked encoding
            parameter_validation=False
        )
        
        # Конфигурация с минимальными настройками для максимальной совместимости
        minimal_config = Config(
            s3={
                'addressing_style': 'path',
                'payload_signing_enabled': False,
            },
            parameter_validation=False,
            retries={'max_attempts': 1, 'mode': 'standard'}
        )
        
        # Пробуем разные конфигурации
        configs_to_try = [
            ("Стандартная конфигурация", s3_client_kwargs.copy()),
            ("С отключенным chunked encoding", {**s3_client_kwargs.copy(), 'config': boto_config}),
            ("Минимальная конфигурация", {**s3_client_kwargs.copy(), 'config': minimal_config}),
        ]
        
        # Если endpoint указан, пробуем разные варианты
        if endpoint:
            # Вариант: Без порта (если порт стандартный)
            if ":443" in endpoint and endpoint.startswith("https://"):
                endpoint_no_port = endpoint.replace(":443", "")
                configs_to_try.append(("Без порта 443", {**s3_client_kwargs.copy(), 'endpoint_url': endpoint_no_port}))
                configs_to_try.append(("Без порта 443 + config", {**s3_client_kwargs.copy(), 'endpoint_url': endpoint_no_port, 'config': boto_config}))
            elif ":80" in endpoint and endpoint.startswith("http://"):
                endpoint_no_port = endpoint.replace(":80", "")
                configs_to_try.append(("Без порта 80", {**s3_client_kwargs.copy(), 'endpoint_url': endpoint_no_port}))
                configs_to_try.append(("Без порта 80 + config", {**s3_client_kwargs.copy(), 'endpoint_url': endpoint_no_port, 'config': boto_config}))
        
        s3_client = None
        working_config = None
        
        for config_name, config_kwargs in configs_to_try:
            try:
                print(f"  Пробуем: {config_name}...")
                test_client = boto3.client('s3', **config_kwargs)
                # Пробуем простой запрос
                test_client.list_objects_v2(Bucket=bucket_name, MaxKeys=0)
                s3_client = test_client
                working_config = config_name
                print(f"✓ Успешно с конфигурацией: {config_name}")
                break
            except Exception as e:
                print(f"  ✗ Не работает: {type(e).__name__}")
                continue
        
        if not s3_client:
            # Если ничего не сработало, используем стандартную конфигурацию
            print("  Используем стандартную конфигурацию (может не работать)")
            s3_client = boto3.client('s3', **s3_client_kwargs)
    except Exception as e:
        print(f"✗ Ошибка при создании клиента: {e}")
        return
    
    print()
    
    # Тест 1: Простая проверка - list_objects_v2
    print("Тест 1: list_objects_v2 (MaxKeys=0)")
    print("-" * 60)
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=0)
        print("✓ Успешно! Бакет доступен для чтения")
        print(f"  Количество объектов: {response.get('KeyCount', 0)}")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_message = e.response.get('Error', {}).get('Message', '')
        print(f"✗ Ошибка: {error_code}")
        print(f"  Сообщение: {error_message}")
        if error_code == 'NoSuchBucket':
            print("  → Бакет не найден. Проверьте имя бакета.")
        elif error_code == '403' or error_code == 'AccessDenied':
            print("  → Доступ запрещён. Проверьте права доступа.")
    except Exception as e:
        print(f"✗ Ошибка: {type(e).__name__}")
        print(f"  Сообщение: {str(e)}")
    
    print()
    
    # Тест 2: Загрузка тестового файла
    print("Тест 2: Загрузка тестового файла (put_object)")
    print("-" * 60)
    test_key = '__test_file_backup_manager__'
    test_content = b'backup_manager_test_content'
    
    # Пробуем разные способы загрузки
    upload_success = False
    
    # Метод 1: Прямой HTTP-запрос с подписью AWS (обход boto3)
    print("  Пробуем: Прямой HTTP-запрос с подписью AWS...")
    try:
        from requests_aws4auth import AWS4Auth
        import requests
        from urllib.parse import urlparse, quote
        
        if endpoint:
            parsed = urlparse(endpoint)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            object_url = f"{base_url}/{bucket_name}/{quote(test_key)}"
        else:
            object_url = f"https://{bucket_name}.s3.{region}.amazonaws.com/{quote(test_key)}"
        
        aws_auth = AWS4Auth(access_key, secret_key, region, 's3')
        verify_ssl = endpoint.startswith('https://') if endpoint else True
        
        put_response = requests.put(
            object_url,
            data=test_content,
            auth=aws_auth,
            timeout=10,
            verify=verify_ssl,
            headers={'Content-Length': str(len(test_content))}
        )
        
        if put_response.status_code in [200, 204]:
            print(f"✓ Успешно! Файл загружен через прямой HTTP-запрос")
            upload_success = True
        else:
            print(f"  ✗ HTTP {put_response.status_code}: {put_response.text[:200]}")
    except ImportError:
        print("  → requests-aws4auth не установлена, пропускаем")
    except Exception as e:
        print(f"  ✗ {type(e).__name__} - {str(e)}")
    
    # Метод 2: boto3 с разными конфигурациями
    if not upload_success:
        from botocore.config import Config
        upload_config = Config(
            s3={
                'addressing_style': 'path',
                'payload_signing_enabled': False,
            },
            parameter_validation=False,
            retries={'max_attempts': 1, 'mode': 'standard'}
        )
        
        upload_client_kwargs = s3_client_kwargs.copy()
        upload_client_kwargs['config'] = upload_config
        upload_client = boto3.client('s3', **upload_client_kwargs)
        
        upload_methods = [
            ("Стандартный метод", lambda: s3_client.put_object(Bucket=bucket_name, Key=test_key, Body=test_content)),
            ("С минимальной конфигурацией", lambda: upload_client.put_object(Bucket=bucket_name, Key=test_key, Body=test_content)),
            ("С явным ContentLength", lambda: upload_client.put_object(
                Bucket=bucket_name, 
                Key=test_key, 
                Body=test_content,
                ContentLength=len(test_content)
            )),
            ("Через upload_fileobj (BytesIO)", lambda: upload_client.upload_fileobj(
                __import__('io').BytesIO(test_content),
                bucket_name,
                test_key
            )),
        ]
        
        for method_name, upload_func in upload_methods:
            try:
                print(f"  Пробуем: {method_name}...")
                upload_func()
                print(f"✓ Успешно! Файл загружен методом: {method_name}")
                upload_success = True
                break
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                error_message = e.response.get('Error', {}).get('Message', '')
                print(f"  ✗ {method_name}: {error_code} - {error_message}")
                if error_code == '403' or error_code == 'AccessDenied':
                    print("  → Доступ запрещён. Проверьте права доступа для записи.")
                    break
            except Exception as e:
                print(f"  ✗ {method_name}: {type(e).__name__} - {str(e)}")
    
    if not upload_success:
        print("✗ Все методы загрузки не сработали")
        test_key = None  # Не пытаемся удалять, если не загрузили
    
    print()
    
    # Тест 3: Чтение тестового файла
    if test_key:
        print("Тест 3: Чтение тестового файла (get_object)")
        print("-" * 60)
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=test_key)
            read_content = response['Body'].read()
            if read_content == test_content:
                print("✓ Успешно! Файл прочитан, содержимое совпадает")
            else:
                print("✗ Ошибка: Содержимое файла не совпадает")
                print(f"  Ожидалось: {test_content}")
                print(f"  Получено: {read_content}")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', '')
            print(f"✗ Ошибка: {error_code}")
            print(f"  Сообщение: {error_message}")
        except Exception as e:
            print(f"✗ Ошибка: {type(e).__name__}")
            print(f"  Сообщение: {str(e)}")
        
        print()
        
        # Тест 4: Удаление тестового файла
        print("Тест 4: Удаление тестового файла (delete_object)")
        print("-" * 60)
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=test_key)
            print("✓ Успешно! Файл удалён")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', '')
            print(f"✗ Ошибка: {error_code}")
            print(f"  Сообщение: {error_message}")
        except Exception as e:
            print(f"✗ Ошибка: {type(e).__name__}")
            print(f"  Сообщение: {str(e)}")
        
        print()
    
    # Итоговый результат
    print("=" * 60)
    print("Тестирование завершено")
    print("=" * 60)

if __name__ == "__main__":
    try:
        test_s3_connection()
    except KeyboardInterrupt:
        print("\n\nПрервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nКритическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
