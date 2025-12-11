"""
Кастомный TestRunner для подробного вывода результатов тестирования
"""
import unittest
import sys
import inspect
from typing import Any, Dict, Optional

# Настройка кодировки для Windows (безопасный способ)
if sys.platform == 'win32':
    try:
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'replace')
        sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'replace')
    except:
        pass  # Если не удалось настроить, используем по умолчанию


class DetailedTestResult(unittest.TextTestResult):
    """Расширенный результат теста с подробным выводом"""
    
    def __init__(self, stream, descriptions, verbosity):
        """Инициализация с сохранением verbosity"""
        super().__init__(stream, descriptions, verbosity)
        self.verbosity = verbosity
        self.current_test = None
        self.test_input_data = {}
        self.test_results = {}
    
    def startTest(self, test):
        """Начало теста"""
        super().startTest(test)
        self.current_test = test
        test_name = self.getDescription(test)
        
        # Получаем docstring теста
        test_method = getattr(test, test._testMethodName, None)
        test_doc = test_method.__doc__ if test_method and hasattr(test_method, '__doc__') else None
        
        print(f"\n{'='*80}")
        print(f"ТЕСТ: {test_name}")
        if test_doc:
            print(f"ОПИСАНИЕ: {test_doc.strip()}")
        print(f"{'='*80}")
        
        # Пытаемся извлечь входные данные из теста
        self._extract_input_data(test)
    
    def _extract_input_data(self, test):
        """Извлечение входных данных из теста"""
        test_method = getattr(test, test._testMethodName, None)
        if not test_method:
            return
        
        # Получаем локальные переменные из setUp
        if hasattr(test, 'setUp'):
            try:
                # Сохраняем состояние после setUp
                if hasattr(test, 'test_dir'):
                    self.test_input_data['test_dir'] = str(test.test_dir)
                if hasattr(test, 'config'):
                    self.test_input_data['config_type'] = type(test.config).__name__
                if hasattr(test, 'backup_manager'):
                    self.test_input_data['backup_manager'] = 'BackupManager instance'
            except:
                pass
        
        # Пытаемся найти входные данные в коде теста
        try:
            source = inspect.getsource(test_method)
            # Ищем паттерны создания данных
            if 'test_file' in source or 'test_folder' in source:
                if hasattr(test, 'test_dir'):
                    self.test_input_data['base_test_dir'] = str(test.test_dir)
        except:
            pass
    
    def addSuccess(self, test):
        """Успешный тест"""
        super().addSuccess(test)
        self._print_test_summary(test, success=True)
    
    def addError(self, test, err):
        """Ошибка в тесте"""
        super().addError(test, err)
        self._print_test_summary(test, success=False, error=err)
    
    def addFailure(self, test, err):
        """Провал теста"""
        super().addFailure(test, err)
        self._print_test_summary(test, success=False, error=err)
    
    def _print_test_summary(self, test, success: bool, error: Optional[tuple] = None):
        """Вывод подробной информации о тесте"""
        print(f"\n{'-'*80}")
        print("ВХОДНЫЕ ДАННЫЕ:")
        print(f"{'-'*80}")
        
        if self.test_input_data:
            for key, value in self.test_input_data.items():
                print(f"  {key}: {value}")
        else:
            print("  (входные данные не определены автоматически)")
        
        # Пытаемся получить результаты из теста
        print(f"\n{'-'*80}")
        print("РЕЗУЛЬТАТ:")
        print(f"{'-'*80}")
        
        if success:
            print("  Статус: [OK] УСПЕШНО")
            print("  Все проверки пройдены")
        else:
            print("  Статус: [FAIL] ОШИБКА/ПРОВАЛ")
            if error:
                print(f"  Тип ошибки: {type(error[1]).__name__}")
                print(f"  Сообщение: {error[1]}")
                if hasattr(self, 'verbosity') and self.verbosity > 1:
                    import traceback
                    print(f"\n  Трассировка:")
                    traceback.print_exception(*error)
        
        print(f"{'='*80}\n")
        
        # Очищаем данные для следующего теста
        self.test_input_data = {}
        self.test_results = {}


class DetailedTestRunner(unittest.TextTestRunner):
    """Кастомный TestRunner с подробным выводом"""
    
    def __init__(self, *args, **kwargs):
        kwargs['resultclass'] = DetailedTestResult
        kwargs['verbosity'] = 2
        super().__init__(*args, **kwargs)


def print_test_info(test_name: str, input_data: Dict[str, Any] = None, result: Any = None, expected: Any = None):
    """
    Вспомогательная функция для вывода информации о тесте
    
    Args:
        test_name: Название теста
        input_data: Входные данные (словарь)
        result: Результат выполнения
        expected: Ожидаемый результат
    """
    print(f"\n{'-'*80}")
    print(f"ЧТО ТЕСТИРУЕТСЯ: {test_name}")
    print(f"{'-'*80}")
    
    if input_data:
        print(f"\nВХОДНЫЕ ДАННЫЕ:")
        for key, value in input_data.items():
            if isinstance(value, (list, dict)):
                print(f"  {key}:")
                if isinstance(value, list):
                    for i, item in enumerate(value):
                        print(f"    [{i}]: {_format_value(item)}")
                else:
                    for k, v in value.items():
                        print(f"    {k}: {_format_value(v)}")
            else:
                print(f"  {key}: {_format_value(value)}")
    
    if result is not None:
        print(f"\nРЕЗУЛЬТАТ:")
        print(f"  {_format_result(result)}")
    
    if expected is not None:
        print(f"\nОЖИДАЕМЫЙ РЕЗУЛЬТАТ:")
        print(f"  {_format_result(expected)}")
        
        if result is not None and expected is not None:
            match = result == expected
            print(f"\nСРАВНЕНИЕ:")
            print(f"  Результат {'==' if match else '!='} Ожидаемый: {match}")
    
    print(f"{'-'*80}")


def _format_value(value: Any) -> str:
    """Форматирование значения для вывода"""
    if isinstance(value, (list, dict)):
        return f"{type(value).__name__} (длина: {len(value)})"
    elif isinstance(value, str) and len(value) > 100:
        return f"{value[:100]}... (длина: {len(value)})"
    elif hasattr(value, '__class__'):
        return f"{value} (тип: {type(value).__name__})"
    return str(value)


def _format_result(result: Any) -> str:
    """Форматирование результата для вывода"""
    if isinstance(result, dict):
        lines = [f"Тип: {type(result).__name__}"]
        lines.append(f"Ключи: {list(result.keys())}")
        for key, value in list(result.items())[:10]:
            if isinstance(value, (list, dict)):
                lines.append(f"  {key}: {type(value).__name__} (длина: {len(value) if isinstance(value, (list, dict)) else 'N/A'})")
            else:
                lines.append(f"  {key}: {_format_value(value)}")
        if len(result) > 10:
            lines.append(f"  ... (ещё {len(result) - 10} ключей)")
        return "\n".join(lines)
    elif isinstance(result, list):
        lines = [f"Тип: {type(result).__name__}"]
        lines.append(f"Длина: {len(result)}")
        if len(result) <= 10:
            for i, item in enumerate(result):
                lines.append(f"  [{i}]: {_format_value(item)}")
        else:
            for i, item in enumerate(result[:5]):
                lines.append(f"  [{i}]: {_format_value(item)}")
            lines.append(f"  ... (ещё {len(result) - 5} элементов)")
        return "\n".join(lines)
    else:
        return f"Значение: {_format_value(result)}\n  Тип: {type(result).__name__}"
