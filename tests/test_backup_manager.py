"""
Юнит-тесты для BackupManager
"""
import unittest
import tempfile
import shutil
import time
import sys
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock

# Настройка путей для импорта
project_root = Path(__file__).parent.parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.backup_manager import BackupManager
from core.config_manager import ConfigManager
from tests.test_runner import print_test_info


class TestBackupManager(unittest.TestCase):
    """Тесты для BackupManager"""
    
    def setUp(self):
        """Настройка перед каждым тестом"""
        # Создаём временную директорию для тестов
        self.test_dir = Path(tempfile.mkdtemp())
        self.config = Mock(spec=ConfigManager)
        self.config.config = {
            "check_interval_minutes": 60
        }
        self.backup_manager = BackupManager(self.config)
    
    def tearDown(self):
        """Очистка после каждого теста"""
        # Удаляем временную директорию
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
    
    def test_init(self):
        """Тест инициализации BackupManager"""
        input_data = {
            "config": "Mock объект ConfigManager",
            "config.config": self.config.config
        }
        
        result = {
            "running": self.backup_manager.running,
            "config_not_none": self.backup_manager.config is not None,
            "lock_not_none": self.backup_manager._lock is not None
        }
        
        expected = {
            "running": False,
            "config_not_none": True,
            "lock_not_none": True
        }
        
        print_test_info("Инициализация BackupManager", input_data, result, expected)
        
        self.assertFalse(self.backup_manager.running)
        self.assertIsNotNone(self.backup_manager.config)
        self.assertIsNotNone(self.backup_manager._lock)
    
    def test_rule_applies_to_folder_all_folders(self):
        """Тест применения правила ко всем папкам"""
        rule = {"folders": ["*"]}
        folder = Path("/some/path")
        
        print(f"ВХОДНЫЕ ДАННЫЕ:")
        print(f"  Правило: {rule}")
        print(f"  Папка: {folder}")
        
        result = self.backup_manager._rule_applies_to_folder(folder, rule)
        
        print(f"РЕЗУЛЬТАТ:")
        print(f"  Применяется: {result}")
        print(f"  Ожидалось: True")
        
        self.assertTrue(result)
    
    def test_rule_applies_to_folder_empty_list(self):
        """Тест применения правила с пустым списком папок"""
        rule = {"folders": []}
        folder = Path("/some/path")
        
        print(f"ВХОДНЫЕ ДАННЫЕ:")
        print(f"  Правило: {rule}")
        print(f"  Папка: {folder}")
        
        result = self.backup_manager._rule_applies_to_folder(folder, rule)
        
        print(f"РЕЗУЛЬТАТ:")
        print(f"  Применяется: {result}")
        print(f"  Ожидалось: False")
        
        self.assertFalse(result)
    
    def test_rule_applies_to_folder_exact_match(self):
        """Тест применения правила с точным совпадением папки"""
        test_folder = self.test_dir / "test_folder"
        test_folder.mkdir()
        
        rule = {"folders": [str(test_folder)]}
        
        print(f"ВХОДНЫЕ ДАННЫЕ:")
        print(f"  Правило: {rule}")
        print(f"  Папка: {test_folder}")
        
        result = self.backup_manager._rule_applies_to_folder(test_folder, rule)
        
        print(f"РЕЗУЛЬТАТ:")
        print(f"  Применяется: {result}")
        print(f"  Ожидалось: True")
        
        self.assertTrue(result)
    
    def test_rule_applies_to_folder_subfolder(self):
        """Тест применения правила к подпапке"""
        parent_folder = self.test_dir / "parent"
        parent_folder.mkdir()
        sub_folder = parent_folder / "sub"
        sub_folder.mkdir()
        
        rule = {"folders": [str(parent_folder)]}
        result = self.backup_manager._rule_applies_to_folder(sub_folder, rule)
        self.assertTrue(result)
    
    def test_matches_rule_wildcard(self):
        """Тест совпадения файла с wildcard паттерном"""
        test_file = self.test_dir / "backup.bak"
        test_file.touch()
        
        print(f"ВХОДНЫЕ ДАННЫЕ:")
        print(f"  Файл: {test_file.name}")
        print(f"  Полный путь: {test_file}")
        
        # Тест 1: совпадение
        rule = {"pattern": "*.bak", "pattern_type": "wildcard"}
        print(f"  Правило 1: {rule}")
        result1 = self.backup_manager._matches_rule(test_file, rule)
        print(f"РЕЗУЛЬТАТ 1:")
        print(f"  Совпадает: {result1}")
        print(f"  Ожидалось: True")
        self.assertTrue(result1)
        
        # Тест 2: несовпадение
        rule = {"pattern": "*.sql", "pattern_type": "wildcard"}
        print(f"  Правило 2: {rule}")
        result2 = self.backup_manager._matches_rule(test_file, rule)
        print(f"РЕЗУЛЬТАТ 2:")
        print(f"  Совпадает: {result2}")
        print(f"  Ожидалось: False")
        self.assertFalse(result2)
    
    def test_matches_rule_regex(self):
        """Тест совпадения файла с regex паттерном"""
        test_file = self.test_dir / "backup_2024.bak"
        test_file.touch()
        
        rule = {"pattern": r"backup_\d+\.bak", "pattern_type": "regex"}
        result = self.backup_manager._matches_rule(test_file, rule)
        self.assertTrue(result)
        
        rule = {"pattern": r"backup_\d+\.sql", "pattern_type": "regex"}
        result = self.backup_manager._matches_rule(test_file, rule)
        self.assertFalse(result)
    
    def test_matches_rule_invalid_regex(self):
        """Тест обработки невалидного regex"""
        test_file = self.test_dir / "test.txt"
        test_file.touch()
        
        rule = {"pattern": "[invalid", "pattern_type": "regex"}
        result = self.backup_manager._matches_rule(test_file, rule)
        self.assertFalse(result)
    
    def test_should_delete_old_file(self):
        """Тест проверки удаления старого файла"""
        test_file = self.test_dir / "old_file.txt"
        test_file.touch()
        
        # Устанавливаем время модификации на 31 день назад
        old_time = (datetime.now() - timedelta(days=31)).timestamp()
        test_file.touch()
        import os
        os.utime(test_file, (old_time, old_time))
        
        # Используем новый формат с минутами
        rule = {"max_age_minutes": 43200}  # 30 дней = 43200 минут
        
        file_age_minutes = (datetime.now() - datetime.fromtimestamp(old_time)).total_seconds() / 60
        
        print(f"ВХОДНЫЕ ДАННЫЕ:")
        print(f"  Файл: {test_file}")
        print(f"  Возраст файла: {file_age_minutes:.0f} минут")
        print(f"  Правило: {rule}")
        print(f"  Максимальный возраст для удаления: {rule['max_age_minutes']} минут")
        
        result = self.backup_manager._should_delete(test_file, rule)
        
        print(f"РЕЗУЛЬТАТ:")
        print(f"  Нужно удалить: {result}")
        print(f"  Ожидалось: True (файл старше {rule['max_age_minutes']} минут)")
        
        self.assertTrue(result)
    
    def test_should_delete_new_file(self):
        """Тест проверки удаления нового файла"""
        test_file = self.test_dir / "new_file.txt"
        test_file.touch()
        
        rule = {"max_age_minutes": 43200}  # 30 дней = 43200 минут
        
        file_age_minutes = (datetime.now() - datetime.fromtimestamp(test_file.stat().st_mtime)).total_seconds() / 60
        
        print(f"ВХОДНЫЕ ДАННЫЕ:")
        print(f"  Файл: {test_file}")
        print(f"  Возраст файла: {file_age_minutes:.2f} минут")
        print(f"  Правило: {rule}")
        print(f"  Максимальный возраст для удаления: {rule['max_age_minutes']} минут")
        
        result = self.backup_manager._should_delete(test_file, rule)
        
        print(f"РЕЗУЛЬТАТ:")
        print(f"  Нужно удалить: {result}")
        print(f"  Ожидалось: False (файл новее {rule['max_age_minutes']} минут)")
        
        self.assertFalse(result)
    
    def test_should_delete_nonexistent_file(self):
        """Тест проверки удаления несуществующего файла"""
        test_file = self.test_dir / "nonexistent.txt"
        
        rule = {"max_age_minutes": 43200}  # 30 дней = 43200 минут
        result = self.backup_manager._should_delete(test_file, rule)
        self.assertFalse(result)
    
    def test_scan_and_clean_no_folders(self):
        """Тест сканирования без папок"""
        self.config.get_watch_folders.return_value = []
        self.config.get_rules.return_value = []
        
        print(f"ВХОДНЫЕ ДАННЫЕ:")
        print(f"  Отслеживаемых папок: 0")
        print(f"  Правил: 0")
        
        results = self.backup_manager.scan_and_clean()
        
        print(f"РЕЗУЛЬТАТ:")
        print(f"  Удалено файлов: {len(results['deleted'])}")
        print(f"  Ошибок: {len(results['errors'])}")
        print(f"  Проверено файлов: {results['total_scanned']}")
        print(f"  Ожидалось: все значения = 0")
        
        self.assertEqual(len(results["deleted"]), 0)
        self.assertEqual(len(results["errors"]), 0)
        self.assertEqual(results["total_scanned"], 0)
    
    def test_scan_and_clean_nonexistent_folder(self):
        """Тест сканирования несуществующей папки"""
        nonexistent_folder = Path("/nonexistent/path")
        self.config.get_watch_folders.return_value = [nonexistent_folder]
        self.config.get_rules.return_value = []
        
        results = self.backup_manager.scan_and_clean()
        
        self.assertEqual(len(results["deleted"]), 0)
        self.assertEqual(len(results["errors"]), 1)
        self.assertIn("не существует", results["errors"][0])
    
    def test_scan_and_clean_no_rules(self):
        """Тест сканирования без правил"""
        test_folder = self.test_dir / "test"
        test_folder.mkdir()
        
        self.config.get_watch_folders.return_value = [test_folder]
        self.config.get_rules.return_value = []
        
        results = self.backup_manager.scan_and_clean()
        
        self.assertEqual(len(results["deleted"]), 0)
        self.assertEqual(len(results["errors"]), 0)
    
    def test_scan_and_clean_disabled_rule(self):
        """Тест сканирования с отключенным правилом"""
        test_folder = self.test_dir / "test"
        test_folder.mkdir()
        test_file = test_folder / "old.bak"
        test_file.touch()
        
        # Устанавливаем старое время
        old_time = (datetime.now() - timedelta(days=31)).timestamp()
        import os
        os.utime(test_file, (old_time, old_time))
        
        rule = {
            "pattern": "*.bak",
            "pattern_type": "wildcard",
            "max_age_minutes": 43200,  # 30 дней = 43200 минут
            "enabled": False,
            "folders": ["*"]
        }
        
        self.config.get_watch_folders.return_value = [test_folder]
        self.config.get_rules.return_value = [rule]
        
        results = self.backup_manager.scan_and_clean()
        
        self.assertEqual(len(results["deleted"]), 0)
    
    @patch('core.backup_manager.SEND2TRASH_AVAILABLE', True)
    @patch('core.backup_manager.send2trash')
    def test_delete_path_to_trash(self, mock_send2trash):
        """Тест удаления файла в корзину"""
        test_file = self.test_dir / "test.txt"
        test_file.touch()
        
        rule = {"permanent_delete": False}
        results = {"deleted": [], "errors": []}
        
        self.backup_manager._delete_path(test_file, results, rule)
        
        mock_send2trash.send2trash.assert_called_once()
        self.assertEqual(len(results["deleted"]), 1)
        self.assertIn("в корзину", results["deleted"][0])
    
    def test_delete_path_permanent(self):
        """Тест постоянного удаления файла"""
        test_file = self.test_dir / "test.txt"
        test_file.touch()
        file_existed = test_file.exists()
        
        rule = {"permanent_delete": True}
        results = {"deleted": [], "errors": []}
        
        print(f"ВХОДНЫЕ ДАННЫЕ:")
        print(f"  Файл: {test_file}")
        print(f"  Файл существует: {file_existed}")
        print(f"  Правило: {rule}")
        print(f"  Тип удаления: постоянное (навсегда)")
        
        self.backup_manager._delete_path(test_file, results, rule)
        
        print(f"РЕЗУЛЬТАТ:")
        print(f"  Файл удалён: {not test_file.exists()} (ожидалось: True)")
        print(f"  Количество удалённых: {len(results['deleted'])} (ожидалось: 1)")
        print(f"  Ошибок: {len(results['errors'])} (ожидалось: 0)")
        if results["deleted"]:
            print(f"  Сообщение: {results['deleted'][0]}")
        
        self.assertFalse(test_file.exists())
        self.assertEqual(len(results["deleted"]), 1)
        self.assertIn("навсегда", results["deleted"][0])
    
    def test_delete_path_nonexistent(self):
        """Тест удаления несуществующего файла"""
        test_file = self.test_dir / "nonexistent.txt"
        
        rule = {"permanent_delete": True}
        results = {"deleted": [], "errors": []}
        
        self.backup_manager._delete_path(test_file, results, rule)
        
        self.assertEqual(len(results["deleted"]), 0)
        self.assertEqual(len(results["errors"]), 0)
    
    def test_start_stop_monitoring(self):
        """Тест запуска и остановки мониторинга"""
        # Настраиваем Mock для работы в потоке
        self.config.get_watch_folders.return_value = []
        self.config.get_rules.return_value = []
        
        self.assertFalse(self.backup_manager.running)
        
        self.backup_manager.start_monitoring()
        # Даём немного времени на запуск потока
        time.sleep(0.1)
        self.assertTrue(self.backup_manager.running)
        
        self.backup_manager.stop_monitoring()
        time.sleep(0.1)
        self.assertFalse(self.backup_manager.running)
    
    def test_process_folder_keep_latest(self):
        """Тест обработки папки с сохранением N самых свежих"""
        test_folder = self.test_dir / "test"
        test_folder.mkdir()
        
        # Создаём несколько файлов с разным временем
        files = []
        for i in range(5):
            test_file = test_folder / f"backup_{i}.bak"
            test_file.touch()
            # Устанавливаем время модификации
            file_time = (datetime.now() - timedelta(days=31, hours=i)).timestamp()
            import os
            os.utime(test_file, (file_time, file_time))
            files.append(test_file)
        
        rule = {
            "pattern": "*.bak",
            "pattern_type": "wildcard",
            "max_age_minutes": 43200,  # 30 дней = 43200 минут
            "enabled": True,
            "folders": ["*"],
            "keep_latest": 2,
            "permanent_delete": True
        }
        
        with patch.object(self.backup_manager, '_delete_path') as mock_delete:
            results = self.backup_manager._process_folder(test_folder, [rule])
            
            # Должно быть удалено 3 файла (5 - 2 = 3)
            self.assertEqual(mock_delete.call_count, 3)
            self.assertEqual(results["total_scanned"], 5)
    
    def test_check_schedule_disabled(self):
        """Тест проверки расписания когда оно отключено"""
        self.config.config["schedule_enabled"] = False
        
        input_data = {
            "schedule_enabled": False,
            "check_interval_minutes": 60
        }
        
        result = self.backup_manager._check_schedule(60)
        
        print_test_info("Проверка расписания (отключено)", input_data, {"result": result}, {"result": True})
        
        self.assertTrue(result)  # Когда расписание отключено, всегда разрешаем сканирование
    
    def test_check_schedule_enabled_no_schedules(self):
        """Тест проверки расписания когда оно включено, но расписаний нет"""
        self.config.config["schedule_enabled"] = True
        self.config.config["schedules"] = []
        
        result = self.backup_manager._check_schedule(60)
        
        self.assertTrue(result)  # Если расписаний нет, разрешаем сканирование
    
    def test_check_schedule_enabled_matching_day_and_time(self):
        """Тест проверки расписания когда день и время совпадают"""
        now = datetime.now()
        current_day = now.weekday()  # 0=понедельник, 6=воскресенье
        current_time = now.strftime("%H:%M")
        
        self.config.config["schedule_enabled"] = True
        self.config.config["schedules"] = [
            {
                "days": [current_day],
                "time": current_time
            }
        ]
        
        input_data = {
            "schedule_enabled": True,
            "current_day": current_day,
            "current_time": current_time,
            "schedule": self.config.config["schedules"][0],
            "check_interval_minutes": 60
        }
        
        result = self.backup_manager._check_schedule(60)
        
        print_test_info("Проверка расписания (совпадение дня и времени)", input_data, {"result": result}, {"result": True})
        
        self.assertTrue(result)
    
    def test_check_schedule_enabled_wrong_day(self):
        """Тест проверки расписания когда день не совпадает"""
        now = datetime.now()
        current_day = now.weekday()
        wrong_day = (current_day + 1) % 7  # Следующий день
        current_time = now.strftime("%H:%M")
        
        self.config.config["schedule_enabled"] = True
        self.config.config["schedules"] = [
            {
                "days": [wrong_day],
                "time": current_time
            }
        ]
        
        input_data = {
            "schedule_enabled": True,
            "current_day": current_day,
            "wrong_day": wrong_day,
            "current_time": current_time,
            "schedule": self.config.config["schedules"][0],
            "check_interval_minutes": 60
        }
        
        result = self.backup_manager._check_schedule(60)
        
        print_test_info("Проверка расписания (неправильный день)", input_data, {"result": result}, {"result": False})
        
        self.assertFalse(result)
    
    def test_check_schedule_enabled_wrong_time(self):
        """Тест проверки расписания когда время не совпадает"""
        now = datetime.now()
        current_day = now.weekday()
        # Устанавливаем время на час вперед
        future_time = (now + timedelta(hours=1)).strftime("%H:%M")
        
        self.config.config["schedule_enabled"] = True
        self.config.config["schedules"] = [
            {
                "days": [current_day],
                "time": future_time
            }
        ]
        
        input_data = {
            "schedule_enabled": True,
            "current_day": current_day,
            "current_time": now.strftime("%H:%M"),
            "schedule_time": future_time,
            "schedule": self.config.config["schedules"][0],
            "check_interval_minutes": 60
        }
        
        result = self.backup_manager._check_schedule(60)
        
        print_test_info("Проверка расписания (неправильное время)", input_data, {"result": result}, {"result": False})
        
        self.assertFalse(result)
    
    def test_check_schedule_multiple_schedules_one_matches(self):
        """Тест проверки расписания с несколькими расписаниями, одно совпадает"""
        now = datetime.now()
        current_day = now.weekday()
        current_time = now.strftime("%H:%M")
        wrong_day = (current_day + 1) % 7
        
        self.config.config["schedule_enabled"] = True
        self.config.config["schedules"] = [
            {
                "days": [wrong_day],
                "time": current_time
            },
            {
                "days": [current_day],
                "time": current_time
            }
        ]
        
        input_data = {
            "schedule_enabled": True,
            "current_day": current_day,
            "current_time": current_time,
            "schedules_count": len(self.config.config["schedules"]),
            "check_interval_minutes": 60
        }
        
        result = self.backup_manager._check_schedule(60)
        
        print_test_info("Проверка расписания (несколько расписаний, одно совпадает)", input_data, {"result": result}, {"result": True})
        
        self.assertTrue(result)  # Хотя бы одно расписание совпадает
    
    def test_check_schedule_multiple_schedules_none_match(self):
        """Тест проверки расписания с несколькими расписаниями, ни одно не совпадает"""
        now = datetime.now()
        current_day = now.weekday()
        wrong_day1 = (current_day + 1) % 7
        wrong_day2 = (current_day + 2) % 7
        future_time = (now + timedelta(hours=1)).strftime("%H:%M")
        
        self.config.config["schedule_enabled"] = True
        self.config.config["schedules"] = [
            {
                "days": [wrong_day1],
                "time": now.strftime("%H:%M")
            },
            {
                "days": [wrong_day2],
                "time": future_time
            }
        ]
        
        input_data = {
            "schedule_enabled": True,
            "current_day": current_day,
            "current_time": now.strftime("%H:%M"),
            "schedules_count": len(self.config.config["schedules"]),
            "check_interval_minutes": 60
        }
        
        result = self.backup_manager._check_schedule(60)
        
        print_test_info("Проверка расписания (несколько расписаний, ни одно не совпадает)", input_data, {"result": result}, {"result": False})
        
        self.assertFalse(result)
    
    def test_check_schedule_time_within_interval(self):
        """Тест проверки расписания с учетом интервала проверки"""
        now = datetime.now()
        current_day = now.weekday()
        # Время расписания на 25 минут назад (в пределах половины интервала 60 минут)
        schedule_time = (now - timedelta(minutes=25)).strftime("%H:%M")
        
        self.config.config["schedule_enabled"] = True
        self.config.config["schedules"] = [
            {
                "days": [current_day],
                "time": schedule_time
            }
        ]
        
        input_data = {
            "schedule_enabled": True,
            "current_day": current_day,
            "current_time": now.strftime("%H:%M"),
            "schedule_time": schedule_time,
            "time_diff_minutes": 25,
            "check_interval_minutes": 60
        }
        
        result = self.backup_manager._check_schedule(60)
        
        print_test_info("Проверка расписания (время в пределах интервала)", input_data, {"result": result}, {"result": True})
        
        self.assertTrue(result)  # Время в пределах половины интервала (30 минут)


if __name__ == '__main__':
    from tests.test_runner import DetailedTestRunner
    unittest.main(testRunner=DetailedTestRunner)

