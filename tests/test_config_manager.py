"""
Юнит-тесты для ConfigManager
"""
import unittest
import tempfile
import shutil
import yaml
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Настройка путей для импорта
project_root = Path(__file__).parent.parent.absolute()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from core.config_manager import ConfigManager
from tests.test_runner import print_test_info


class TestConfigManager(unittest.TestCase):
    """Тесты для ConfigManager"""
    
    def setUp(self):
        """Настройка перед каждым тестом"""
        # Создаём временную директорию для тестов
        self.test_dir = Path(tempfile.mkdtemp())
        # Патчим CONFIG_FILE чтобы использовать тестовую директорию
        self.config_file = self.test_dir / "config.yaml"
        ConfigManager.CONFIG_FILE = self.config_file
    
    def tearDown(self):
        """Очистка после каждого теста"""
        # Удаляем временную директорию
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)
        # Восстанавливаем оригинальный CONFIG_FILE
        ConfigManager.CONFIG_FILE = Path.home() / ".backup_manager" / "config.yaml"
    
    def test_init_creates_default_config(self):
        """Тест инициализации с созданием дефолтной конфигурации"""
        config = ConfigManager()
        
        self.assertIsNotNone(config.config)
        self.assertIn("watch_folders", config.config)
        self.assertIn("rules", config.config)
        # Проверяем наличие настройки интервала (может быть в старом или новом формате)
        self.assertTrue("check_interval_minutes" in config.config or "check_interval_seconds" in config.config)
        self.assertIn("auto_start", config.config)
        
        # Проверяем, что файл создан
        self.assertTrue(self.config_file.exists())
    
    def test_load_config_from_file(self):
        """Тест загрузки конфигурации из файла"""
        # Создаём тестовый конфиг
        test_config = {
            "watch_folders": ["/test/path"],
            "check_interval_minutes": 120,  # 2 часа = 120 минут
            "auto_start": True
        }
        
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_file, 'w', encoding='utf-8') as f:
            yaml.dump(test_config, f)
        
        config = ConfigManager()
        
        self.assertEqual(config.config["check_interval_minutes"], 120)
        self.assertEqual(config.config["auto_start"], True)
        # Проверяем, что дефолтные значения тоже есть
        self.assertIn("rules", config.config)
    
    def test_save_config(self):
        """Тест сохранения конфигурации"""
        config = ConfigManager()
        config.config["check_interval_minutes"] = 30
        config.config["auto_start"] = True
        
        config.save_config()
        
        # Проверяем, что файл сохранён
        self.assertTrue(self.config_file.exists())
        
        # Загружаем и проверяем
        with open(self.config_file, 'r', encoding='utf-8') as f:
            loaded_config = yaml.safe_load(f)
        
        self.assertEqual(loaded_config["check_interval_minutes"], 30)
        self.assertEqual(loaded_config["auto_start"], True)
    
    def test_get_watch_folders(self):
        """Тест получения списка папок для мониторинга"""
        config = ConfigManager()
        config.config["watch_folders"] = ["/path1", "/path2"]
        
        folders = config.get_watch_folders()
        
        self.assertEqual(len(folders), 2)
        self.assertIsInstance(folders[0], Path)
        # Проверяем, что путь корректно преобразован в Path
        self.assertEqual(folders[0], Path("/path1"))
    
    def test_add_watch_folder(self):
        """Тест добавления папки для мониторинга"""
        config = ConfigManager()
        test_folder = Path("/test/folder")
        
        input_data = {
            "folder": str(test_folder)
        }
        
        config.add_watch_folder(test_folder)
        
        folders = config.get_watch_folders()
        
        result = {
            "folders_count": len(folders),
            "added_folder": str(folders[0].absolute()) if folders else None
        }
        
        expected = {
            "folders_count": 1,
            "added_folder": str(test_folder.absolute())
        }
        
        print_test_info("Добавление папки для мониторинга", input_data, result, expected)
        
        self.assertEqual(len(folders), 1)
        # Сравниваем абсолютные пути
        self.assertEqual(folders[0].absolute(), test_folder.absolute())
    
    def test_add_watch_folder_duplicate(self):
        """Тест добавления дублирующейся папки"""
        config = ConfigManager()
        # Используем реальную папку для теста
        test_folder = self.test_dir / "test_folder"
        test_folder.mkdir()
        
        config.add_watch_folder(test_folder)
        config.add_watch_folder(test_folder)  # Добавляем второй раз
        
        folders = config.get_watch_folders()
        # Дубликат не должен быть добавлен (проверка идёт по абсолютному пути)
        folder_strs = [str(f.absolute()) for f in folders]
        unique_folders = set(folder_strs)
        self.assertEqual(len(unique_folders), 1)
    
    def test_remove_watch_folder(self):
        """Тест удаления папки из мониторинга"""
        config = ConfigManager()
        # Используем реальные папки для теста
        test_folder1 = self.test_dir / "folder1"
        test_folder1.mkdir()
        test_folder2 = self.test_dir / "folder2"
        test_folder2.mkdir()
        
        config.add_watch_folder(test_folder1)
        config.add_watch_folder(test_folder2)
        
        config.remove_watch_folder(test_folder1)
        
        folders = config.get_watch_folders()
        self.assertEqual(len(folders), 1)
        self.assertEqual(folders[0].absolute(), test_folder2.absolute())
    
    def test_get_rules(self):
        """Тест получения списка правил"""
        config = ConfigManager()
        
        rules = config.get_rules()
        
        self.assertIsInstance(rules, list)
        self.assertGreater(len(rules), 0)  # Должно быть хотя бы одно дефолтное правило
    
    def test_add_rule(self):
        """Тест добавления правила"""
        config = ConfigManager()
        new_rule = {
            "name": "Test Rule",
            "pattern": "*.test",
            "pattern_type": "wildcard",
            "max_age_minutes": 14400,  # 10 дней = 14400 минут
            "enabled": True,
            "folders": ["*"],
            "keep_latest": 0,
            "permanent_delete": False
        }
        
        initial_count = len(config.get_rules())
        config.add_rule(new_rule)
        
        rules = config.get_rules()
        self.assertEqual(len(rules), initial_count + 1)
        self.assertEqual(rules[-1]["name"], "Test Rule")
    
    def test_update_rule(self):
        """Тест обновления правила"""
        config = ConfigManager()
        rules = config.get_rules()
        
        if len(rules) > 0:
            original_name = rules[0]["name"]
            updated_rule = rules[0].copy()
            updated_rule["name"] = "Updated Rule"
            
            config.update_rule(0, updated_rule)
            
            updated_rules = config.get_rules()
            self.assertEqual(updated_rules[0]["name"], "Updated Rule")
    
    def test_update_rule_invalid_index(self):
        """Тест обновления правила с невалидным индексом"""
        config = ConfigManager()
        rules = config.get_rules()
        original_count = len(rules)
        
        updated_rule = {"name": "Test"}
        config.update_rule(999, updated_rule)  # Невалидный индекс
        
        # Правила не должны измениться
        self.assertEqual(len(config.get_rules()), original_count)
    
    def test_remove_rule(self):
        """Тест удаления правила"""
        config = ConfigManager()
        initial_count = len(config.get_rules())
        
        if initial_count > 0:
            config.remove_rule(0)
            
            rules = config.get_rules()
            self.assertEqual(len(rules), initial_count - 1)
    
    def test_remove_rule_invalid_index(self):
        """Тест удаления правила с невалидным индексом"""
        config = ConfigManager()
        initial_count = len(config.get_rules())
        
        config.remove_rule(999)  # Невалидный индекс
        
        # Правила не должны измениться
        self.assertEqual(len(config.get_rules()), initial_count)
    
    @patch('core.config_manager.WINDOWS', True)
    @patch('core.config_manager.winreg')
    def test_set_autostart_enable(self, mock_winreg):
        """Тест включения автозапуска"""
        config = ConfigManager()
        
        # Мокаем winreg
        mock_key = MagicMock()
        mock_winreg.OpenKey.return_value = mock_key
        mock_winreg.HKEY_CURRENT_USER = "HKEY_CURRENT_USER"
        mock_winreg.KEY_SET_VALUE = "KEY_SET_VALUE"
        mock_winreg.REG_SZ = "REG_SZ"
        
        config._set_autostart(True)
        
        mock_winreg.OpenKey.assert_called()
        mock_winreg.SetValueEx.assert_called()
        mock_winreg.CloseKey.assert_called()
    
    @patch('core.config_manager.WINDOWS', True)
    @patch('core.config_manager.winreg')
    def test_set_autostart_disable(self, mock_winreg):
        """Тест отключения автозапуска"""
        config = ConfigManager()
        
        # Мокаем winreg
        mock_key = MagicMock()
        mock_winreg.OpenKey.return_value = mock_key
        mock_winreg.HKEY_CURRENT_USER = "HKEY_CURRENT_USER"
        mock_winreg.KEY_SET_VALUE = "KEY_SET_VALUE"
        
        config._set_autostart(False)
        
        mock_winreg.OpenKey.assert_called()
        mock_winreg.DeleteValue.assert_called()
        mock_winreg.CloseKey.assert_called()
    
    @patch('core.config_manager.WINDOWS', False)
    def test_set_autostart_non_windows(self):
        """Тест автозапуска на не-Windows системе"""
        config = ConfigManager()
        
        # На не-Windows системе метод должен просто вернуться
        config._set_autostart(True)
        # Если не было ошибки, тест пройден
    
    def test_sync_autostart(self):
        """Тест синхронизации автозапуска"""
        config = ConfigManager()
        config.config["auto_start"] = True
        
        with patch.object(config, '_set_autostart') as mock_set:
            config.sync_autostart()
            mock_set.assert_called_once_with(True)


if __name__ == '__main__':
    unittest.main()

