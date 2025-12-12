"""
Менеджер конфигурации приложения
"""
import yaml
import sys
import os
from pathlib import Path
from typing import List, Dict, Any
from core.logger import setup_logger

# Импорт для работы с реестром Windows
try:
    import winreg
    WINDOWS = True
except ImportError:
    WINDOWS = False

logger = setup_logger("ConfigManager")

class ConfigManager:
    """Класс для управления конфигурацией приложения"""
    
    CONFIG_FILE = Path.home() / ".backup_manager" / "config.yaml"
    
    DEFAULT_CONFIG = {
        "watch_folders": [],
        "rules": [
            {
                "name": "Удалить файлы старше 43200 минут (30 дней)",
                "pattern": "*",
                "pattern_type": "wildcard",  # "wildcard" или "regex"
                "max_age_minutes": 43200,  # 30 дней = 30 * 24 * 60 минут
                "enabled": True,
                "folders": [],  # Пустой список означает "ничего не выбрано"
                "keep_latest": 0,  # Сколько самых свежих объектов оставить (0 = оставить все подходящие)
                "permanent_delete": False  # False = удалять в корзину, True = удалять навсегда
            }
        ],
        "check_interval_minutes": 60,  # Проверка каждые 60 минут (1 час)
        "auto_start": False,
        "schedule_enabled": False,  # Включено ли использование расписаний
        "schedules": [
            {
                "days": [0, 1, 2, 3, 4, 5, 6],  # Дни недели: 0=понедельник, 6=воскресенье
                "time": "00:00"  # Время в формате HH:MM
            }
        ]
    }
    
    def __init__(self):
        """Инициализация менеджера конфигурации"""
        self.config = self.load_config()
    
    def load_config(self) -> Dict[str, Any]:
        """Загрузка конфигурации из файла"""
        if self.CONFIG_FILE.exists():
            try:
                with open(self.CONFIG_FILE, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f) or {}
                    
                    # Миграция старого формата расписания
                    old_schedule = config.get("schedule", {})
                    if old_schedule and "enabled" in old_schedule:
                        # Мигрируем старый формат в новый
                        if "schedules" not in config or not config.get("schedules"):
                            config["schedule_enabled"] = old_schedule.get("enabled", False)
                            if old_schedule.get("enabled", False):
                                config["schedules"] = [{
                                    "days": old_schedule.get("days", [0, 1, 2, 3, 4, 5, 6]),
                                    "time": old_schedule.get("time", "00:00")
                                }]
                            else:
                                config["schedules"] = [{
                                    "days": [0, 1, 2, 3, 4, 5, 6],
                                    "time": "00:00"
                                }]
                        # Удаляем старое поле
                        if "schedule" in config:
                            del config["schedule"]
                    
                    # Объединяем с дефолтной конфигурацией
                    return {**self.DEFAULT_CONFIG, **config}
            except Exception as e:
                logger.error(f"Ошибка загрузки конфигурации: {e}", exc_info=True)
        
        # Создаём директорию для конфига если её нет
        self.CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
        self.save_config(self.DEFAULT_CONFIG)
        return self.DEFAULT_CONFIG.copy()
    
    def save_config(self, config: Dict[str, Any] = None):
        """Сохранение конфигурации в файл"""
        if config is None:
            config = self.config
        
        # Сохраняем настройку автозапуска в реестр
        auto_start = config.get("auto_start", False)
        self._set_autostart(auto_start)
        
        self.CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(self.CONFIG_FILE, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        
        self.config = config
    
    def _get_executable_path(self) -> str:
        """Получить путь к исполняемому файлу для автозапуска"""
        if getattr(sys, 'frozen', False):
            # Если приложение упаковано (например, PyInstaller)
            return sys.executable
        else:
            # Проверяем наличие launcher.pyw (запуск без консоли)
            launcher_path = Path(__file__).parent.parent / "launcher.pyw"
            if launcher_path.exists():
                # Используем pythonw.exe для запуска .pyw файла
                python_exe = sys.executable
                if python_exe.endswith('python.exe'):
                    python_exe = python_exe.replace('python.exe', 'pythonw.exe')
                elif python_exe.endswith('pythonw.exe'):
                    pass  # Уже pythonw.exe
                else:
                    # Пробуем найти pythonw.exe в той же директории
                    python_dir = Path(python_exe).parent
                    pythonw_exe = python_dir / "pythonw.exe"
                    if pythonw_exe.exists():
                        python_exe = str(pythonw_exe)
                
                return f'"{python_exe}" "{launcher_path}"'
            else:
                # Если launcher.pyw нет, используем pythonw.exe с main.py
                script_path = Path(__file__).parent.parent / "main.py"
                python_exe = sys.executable
                if python_exe.endswith('python.exe'):
                    python_exe = python_exe.replace('python.exe', 'pythonw.exe')
                elif python_exe.endswith('pythonw.exe'):
                    pass  # Уже pythonw.exe
                else:
                    # Пробуем найти pythonw.exe в той же директории
                    python_dir = Path(python_exe).parent
                    pythonw_exe = python_dir / "pythonw.exe"
                    if pythonw_exe.exists():
                        python_exe = str(pythonw_exe)
                
                return f'"{python_exe}" "{script_path}"'
    
    def _set_autostart(self, enable: bool):
        """Установить или удалить автозапуск в реестре Windows"""
        if not WINDOWS:
            return  # Автозапуск работает только на Windows
        
        app_name = "BackupManager"
        registry_key = winreg.HKEY_CURRENT_USER
        registry_path = r"Software\Microsoft\Windows\CurrentVersion\Run"
        
        try:
            if enable:
                # Добавляем в автозапуск
                key = winreg.OpenKey(registry_key, registry_path, 0, winreg.KEY_SET_VALUE)
                executable_path = self._get_executable_path()
                winreg.SetValueEx(key, app_name, 0, winreg.REG_SZ, executable_path)
                winreg.CloseKey(key)
            else:
                # Удаляем из автозапуска
                try:
                    key = winreg.OpenKey(registry_key, registry_path, 0, winreg.KEY_SET_VALUE)
                    winreg.DeleteValue(key, app_name)
                    winreg.CloseKey(key)
                except FileNotFoundError:
                    # Ключ не существует, ничего не делаем
                    pass
        except Exception as e:
            logger.error(f"Ошибка при работе с реестром автозапуска: {e}", exc_info=True)
    
    def sync_autostart(self):
        """Синхронизировать настройку автозапуска с реестром"""
        auto_start = self.config.get("auto_start", False)
        self._set_autostart(auto_start)
    
    def get_watch_folders(self) -> List[Path]:
        """Получить список папок для мониторинга"""
        return [Path(folder) for folder in self.config.get("watch_folders", [])]
    
    def add_watch_folder(self, folder: Path):
        """Добавить папку для мониторинга"""
        folders = self.get_watch_folders()
        folder_str = str(folder.absolute())
        if folder_str not in [str(f) for f in folders]:
            folders.append(folder)
            self.config["watch_folders"] = [str(f) for f in folders]
            self.save_config()
    
    def remove_watch_folder(self, folder: Path):
        """Удалить папку из мониторинга"""
        folders = self.get_watch_folders()
        folders = [f for f in folders if str(f) != str(folder)]
        self.config["watch_folders"] = [str(f) for f in folders]
        self.save_config()
    
    def get_rules(self) -> List[Dict[str, Any]]:
        """Получить список правил"""
        return self.config.get("rules", [])
    
    def add_rule(self, rule: Dict[str, Any]):
        """Добавить правило"""
        rules = self.get_rules()
        rules.append(rule)
        self.config["rules"] = rules
        self.save_config()
    
    def update_rule(self, index: int, rule: Dict[str, Any]):
        """Обновить правило по индексу"""
        rules = self.get_rules()
        if 0 <= index < len(rules):
            rules[index] = rule
            self.config["rules"] = rules
            self.save_config()
    
    def remove_rule(self, index: int):
        """Удалить правило по индексу"""
        rules = self.get_rules()
        if 0 <= index < len(rules):
            rules.pop(index)
            self.config["rules"] = rules
            self.save_config()

