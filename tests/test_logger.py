"""
Юнит-тесты для модуля логирования
"""
import unittest
import tempfile
import shutil
import sys
from pathlib import Path

# Настройка путей для импорта
project_root = Path(__file__).parent.parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.logger import setup_logger, get_log_file_path
from tests.test_runner import print_test_info


class TestLogger(unittest.TestCase):
    """Тесты для модуля логирования"""
    
    def setUp(self):
        """Настройка перед каждым тестом"""
        # Сохраняем оригинальный путь
        from core import logger as logger_module
        self.original_log_dir = logger_module.LOG_DIR
        self.original_log_file = logger_module.LOG_FILE
    
    def tearDown(self):
        """Очистка после каждого теста"""
        # Восстанавливаем оригинальные пути
        from core import logger as logger_module
        logger_module.LOG_DIR = self.original_log_dir
        logger_module.LOG_FILE = self.original_log_file
    
    def test_setup_logger(self):
        """Тест настройки логгера"""
        logger_name = "TestLogger"
        
        input_data = {
            "logger_name": logger_name
        }
        
        logger = setup_logger(logger_name)
        
        result = {
            "logger_not_none": logger is not None,
            "logger_name": logger.name if logger else None,
            "logger_level": logger.level if logger else None
        }
        
        expected = {
            "logger_not_none": True,
            "logger_name": "TestLogger",
            "logger_level": 20  # INFO level
        }
        
        print_test_info("Настройка логгера", input_data, result, expected)
        
        self.assertIsNotNone(logger)
        self.assertEqual(logger.name, "TestLogger")
        self.assertEqual(logger.level, 20)  # INFO level
    
    def test_setup_logger_multiple_calls(self):
        """Тест множественных вызовов setup_logger"""
        logger1 = setup_logger("TestLogger")
        logger2 = setup_logger("TestLogger")
        
        # Должен вернуться тот же логгер
        self.assertIs(logger1, logger2)
    
    def test_get_log_file_path(self):
        """Тест получения пути к файлу логов"""
        log_path = get_log_file_path()
        
        self.assertIsInstance(log_path, str)
        self.assertIn(".backup_manager", log_path)
        self.assertIn("backup_manager.log", log_path)


if __name__ == '__main__':
    unittest.main()

