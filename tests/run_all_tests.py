"""
Скрипт для запуска всех тестов с подробным выводом
"""
import sys
import unittest
from pathlib import Path

# Настройка путей для импорта
project_root = Path(__file__).parent.parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from tests.test_runner import DetailedTestRunner

if __name__ == '__main__':
    # Загружаем все тесты
    loader = unittest.TestLoader()
    start_dir = Path(__file__).parent
    suite = loader.discover(start_dir, pattern='test_*.py')
    
    # Запускаем с подробным выводом
    runner = DetailedTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Выводим итоговую статистику
    print(f"\n{'='*80}")
    print(f"ИТОГОВАЯ СТАТИСТИКА")
    print(f"{'='*80}")
    print(f"Всего тестов: {result.testsRun}")
    print(f"Успешно: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Провалов: {len(result.failures)}")
    print(f"Ошибок: {len(result.errors)}")
    print(f"{'='*80}\n")
    
    # Выходим с кодом ошибки, если были неудачные тесты
    sys.exit(0 if result.wasSuccessful() else 1)

