"""
Тесты для модуля s3_manager
"""
import unittest
from core.s3_manager import (
    normalize_endpoint,
    create_s3_client,
    check_bucket_availability_sync
)


class TestS3Manager(unittest.TestCase):
    """Тесты для S3 менеджера"""
    
    # Тестовые учётные данные
    TEST_ENDPOINT = "s3-minsk.cloud.mts.by:443"
    TEST_BUCKET = "bck"
    TEST_ACCESS_KEY = "009099d4042e2c0d7cd4"
    TEST_SECRET_KEY = "JLGtNYzxPq4PXAoASgp4kTBgZBArj4NZP+iP5cxN"
    TEST_REGION = "minsk"
    
    def test_normalize_endpoint_with_port_443(self):
        """Тест нормализации endpoint с портом 443"""
        endpoint = "s3-minsk.cloud.mts.by:443"
        normalized = normalize_endpoint(endpoint)
        self.assertEqual(normalized, "https://s3-minsk.cloud.mts.by:443")
    
    def test_normalize_endpoint_with_port_80(self):
        """Тест нормализации endpoint с портом 80"""
        endpoint = "s3.example.com:80"
        normalized = normalize_endpoint(endpoint)
        self.assertEqual(normalized, "http://s3.example.com:80")
    
    def test_normalize_endpoint_with_https(self):
        """Тест нормализации endpoint с уже указанным HTTPS"""
        endpoint = "https://s3.example.com:443"
        normalized = normalize_endpoint(endpoint)
        self.assertEqual(normalized, "https://s3.example.com:443")
    
    def test_normalize_endpoint_with_http(self):
        """Тест нормализации endpoint с уже указанным HTTP"""
        endpoint = "http://s3.example.com:80"
        normalized = normalize_endpoint(endpoint)
        self.assertEqual(normalized, "http://s3.example.com:80")
    
    def test_normalize_endpoint_http_with_port_443(self):
        """Тест нормализации endpoint с HTTP, но портом 443 (должен исправить на HTTPS)"""
        endpoint = "http://s3-minsk.cloud.mts.by:443"
        normalized = normalize_endpoint(endpoint)
        self.assertEqual(normalized, "https://s3-minsk.cloud.mts.by:443")
    
    def test_normalize_endpoint_https_with_port_80(self):
        """Тест нормализации endpoint с HTTPS, но портом 80 (должен исправить на HTTP)"""
        endpoint = "https://s3.example.com:80"
        normalized = normalize_endpoint(endpoint)
        self.assertEqual(normalized, "http://s3.example.com:80")
    
    def test_normalize_endpoint_without_port(self):
        """Тест нормализации endpoint без порта"""
        endpoint = "s3.example.com"
        normalized = normalize_endpoint(endpoint)
        self.assertEqual(normalized, "https://s3.example.com")
    
    def test_normalize_endpoint_with_custom_port(self):
        """Тест нормализации endpoint с кастомным портом"""
        endpoint = "s3.example.com:9000"
        normalized = normalize_endpoint(endpoint)
        self.assertEqual(normalized, "http://s3.example.com:9000")
    
    def test_normalize_endpoint_empty(self):
        """Тест нормализации пустого endpoint"""
        self.assertEqual(normalize_endpoint(""), "")
        self.assertEqual(normalize_endpoint(None), "")
    
    def test_create_s3_client_with_endpoint(self):
        """Тест создания клиента S3 с endpoint"""
        client = create_s3_client(
            self.TEST_ACCESS_KEY,
            self.TEST_SECRET_KEY,
            self.TEST_REGION,
            self.TEST_ENDPOINT
        )
        self.assertIsNotNone(client)
        # Проверяем, что клиент имеет нужные методы
        self.assertTrue(hasattr(client, 'list_objects_v2'))
        self.assertTrue(hasattr(client, 'put_object'))
        self.assertTrue(hasattr(client, 'get_object'))
        self.assertTrue(hasattr(client, 'delete_object'))
    
    def test_create_s3_client_without_endpoint(self):
        """Тест создания клиента S3 без endpoint (AWS S3)"""
        client = create_s3_client(
            self.TEST_ACCESS_KEY,
            self.TEST_SECRET_KEY,
            "us-east-1",
            None
        )
        self.assertIsNotNone(client)
    
    def test_check_bucket_availability_sync_success(self):
        """Тест проверки доступности бакета (успешный случай)"""
        success, result, details = check_bucket_availability_sync(
            self.TEST_BUCKET,
            self.TEST_ACCESS_KEY,
            self.TEST_SECRET_KEY,
            self.TEST_REGION,
            self.TEST_ENDPOINT
        )
        
        # Проверяем результат
        self.assertTrue(success, f"Проверка не прошла: {result} - {details}")
        self.assertEqual(result, "Успешно")
        self.assertIn("доступен", details.lower())
    
    def test_check_bucket_availability_sync_invalid_bucket(self):
        """Тест проверки доступности несуществующего бакета"""
        success, result, details = check_bucket_availability_sync(
            "nonexistent_bucket_12345",
            self.TEST_ACCESS_KEY,
            self.TEST_SECRET_KEY,
            self.TEST_REGION,
            self.TEST_ENDPOINT
        )
        
        # Должна быть ошибка
        self.assertFalse(success)
        self.assertIn("Ошибка", result)
    
    def test_check_bucket_availability_sync_invalid_credentials(self):
        """Тест проверки доступности с неверными учётными данными"""
        success, result, details = check_bucket_availability_sync(
            self.TEST_BUCKET,
            "invalid_access_key",
            "invalid_secret_key",
            self.TEST_REGION,
            self.TEST_ENDPOINT
        )
        
        # Должна быть ошибка
        self.assertFalse(success)
        self.assertIn("Ошибка", result)
    
    def test_check_bucket_availability_sync_empty_bucket_name(self):
        """Тест проверки доступности с пустым именем бакета"""
        success, result, details = check_bucket_availability_sync(
            "",
            self.TEST_ACCESS_KEY,
            self.TEST_SECRET_KEY,
            self.TEST_REGION,
            self.TEST_ENDPOINT
        )
        
        self.assertFalse(success)
        self.assertEqual(result, "Ошибка конфигурации")
        self.assertIn("Имя бакета", details)
    
    def test_check_bucket_availability_sync_empty_access_key(self):
        """Тест проверки доступности с пустым Access Key"""
        success, result, details = check_bucket_availability_sync(
            self.TEST_BUCKET,
            "",
            self.TEST_SECRET_KEY,
            self.TEST_REGION,
            self.TEST_ENDPOINT
        )
        
        self.assertFalse(success)
        self.assertEqual(result, "Ошибка конфигурации")
        self.assertIn("Access Key", details)
    
    def test_check_bucket_availability_sync_empty_secret_key(self):
        """Тест проверки доступности с пустым Secret Key"""
        success, result, details = check_bucket_availability_sync(
            self.TEST_BUCKET,
            self.TEST_ACCESS_KEY,
            "",
            self.TEST_REGION,
            self.TEST_ENDPOINT
        )
        
        self.assertFalse(success)
        self.assertEqual(result, "Ошибка конфигурации")
        self.assertIn("Secret Access Key", details)


if __name__ == '__main__':
    unittest.main()
