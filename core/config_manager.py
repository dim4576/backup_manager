"""
Менеджер конфигурации приложения с хранением в SQLite
"""
import sqlite3
import json
import sys
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from core.logger import setup_logger

# Импорт для работы с реестром Windows
try:
    import winreg
    WINDOWS = True
except ImportError:
    WINDOWS = False

logger = setup_logger("ConfigManager")


class ConfigManager:
    """Класс для управления конфигурацией приложения с хранением в SQLite"""
    
    CONFIG_DIR = Path.home() / ".backup_manager"
    DB_FILE = CONFIG_DIR / "config.db"
    OLD_YAML_FILE = CONFIG_DIR / "config.yaml"
    
    DEFAULT_SETTINGS = {
        "check_interval_minutes": 60,
        "auto_start": False,
        "schedule_enabled": False,
    }
    
    def __init__(self):
        """Инициализация менеджера конфигурации"""
        self.CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        self._init_database()
        self._migrate_from_yaml()
        self.config = self._load_config_dict()
    
    def _get_connection(self) -> sqlite3.Connection:
        """Получить соединение с базой данных"""
        conn = sqlite3.connect(str(self.DB_FILE))
        conn.row_factory = sqlite3.Row
        return conn
    
    def _init_database(self):
        """Инициализация структуры базы данных"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Таблица общих настроек (key-value)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        
        # Таблица папок для мониторинга
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS watch_folders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT UNIQUE NOT NULL
            )
        """)
        
        # Таблица правил удаления
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                pattern TEXT DEFAULT '*',
                pattern_type TEXT DEFAULT 'wildcard',
                max_age_minutes INTEGER DEFAULT 43200,
                enabled INTEGER DEFAULT 1,
                folders TEXT DEFAULT '[]',
                keep_latest INTEGER DEFAULT 0,
                permanent_delete INTEGER DEFAULT 0,
                copy_enabled INTEGER DEFAULT 0,
                copy_s3_bucket_name TEXT
            )
        """)
        
        # Таблица правил синхронизации
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                bucket_name TEXT,
                enabled INTEGER DEFAULT 1,
                folders TEXT DEFAULT '[]',
                schedule_type TEXT DEFAULT 'interval',
                interval_minutes INTEGER DEFAULT 60,
                schedule_days TEXT DEFAULT '[]',
                schedule_time TEXT DEFAULT '03:00',
                versioning_enabled INTEGER DEFAULT 0,
                max_versions INTEGER DEFAULT 5,
                max_version_age_days INTEGER DEFAULT 30,
                delete_after_sync INTEGER DEFAULT 0,
                sync_deletions INTEGER DEFAULT 0,
                pattern TEXT DEFAULT '*',
                pattern_type TEXT DEFAULT 'wildcard',
                last_sync TEXT
            )
        """)
        
        # Таблица S3 бакетов
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS s3_buckets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                endpoint TEXT,
                access_key TEXT,
                secret_key TEXT,
                region TEXT DEFAULT 'us-east-1'
            )
        """)
        
        # Таблица расписаний
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                days TEXT DEFAULT '[0,1,2,3,4,5,6]',
                time TEXT DEFAULT '00:00'
            )
        """)
        
        conn.commit()
        conn.close()
    
    def _migrate_from_yaml(self):
        """Миграция данных из старого YAML файла в SQLite"""
        if not self.OLD_YAML_FILE.exists():
            return
        
        # Проверяем, была ли уже миграция
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM settings WHERE key = 'migrated_from_yaml'")
        row = cursor.fetchone()
        if row:
            conn.close()
            return
        
        try:
            import yaml
            with open(self.OLD_YAML_FILE, 'r', encoding='utf-8') as f:
                old_config = yaml.safe_load(f) or {}
            
            logger.info("Начинаю миграцию из YAML в SQLite...")
            
            # Миграция общих настроек
            for key in ['check_interval_minutes', 'auto_start', 'schedule_enabled']:
                if key in old_config:
                    value = old_config[key]
                    cursor.execute(
                        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                        (key, json.dumps(value))
                    )
            
            # Миграция папок
            for folder in old_config.get('watch_folders', []):
                cursor.execute(
                    "INSERT OR IGNORE INTO watch_folders (path) VALUES (?)",
                    (folder,)
                )
            
            # Миграция правил удаления
            for rule in old_config.get('rules', []):
                cursor.execute("""
                    INSERT INTO rules (name, pattern, pattern_type, max_age_minutes, 
                        enabled, folders, keep_latest, permanent_delete, 
                        copy_enabled, copy_s3_bucket_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    rule.get('name', 'Без названия'),
                    rule.get('pattern', '*'),
                    rule.get('pattern_type', 'wildcard'),
                    rule.get('max_age_minutes', rule.get('max_age_days', 30) * 24 * 60),
                    1 if rule.get('enabled', True) else 0,
                    json.dumps(rule.get('folders', [])),
                    rule.get('keep_latest', 0),
                    1 if rule.get('permanent_delete', False) else 0,
                    1 if rule.get('copy_enabled', False) else 0,
                    rule.get('copy_s3_bucket_name')
                ))
            
            # Миграция правил синхронизации
            for rule in old_config.get('sync_rules', []):
                cursor.execute("""
                    INSERT INTO sync_rules (name, bucket_name, enabled, folders, 
                        schedule_type, interval_minutes, schedule_days, schedule_time,
                        versioning_enabled, max_versions, max_version_age_days,
                        delete_after_sync, sync_deletions, pattern, pattern_type, last_sync)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    rule.get('name', 'Без названия'),
                    rule.get('bucket_name'),
                    1 if rule.get('enabled', True) else 0,
                    json.dumps(rule.get('folders', [])),
                    rule.get('schedule_type', 'interval'),
                    rule.get('interval_minutes', 60),
                    json.dumps(rule.get('schedule_days', [])),
                    rule.get('schedule_time', '03:00'),
                    1 if rule.get('versioning_enabled', False) else 0,
                    rule.get('max_versions', 5),
                    rule.get('max_version_age_days', 30),
                    1 if rule.get('delete_after_sync', False) else 0,
                    1 if rule.get('sync_deletions', False) else 0,
                    rule.get('pattern', '*'),
                    rule.get('pattern_type', 'wildcard'),
                    rule.get('last_sync')
                ))
            
            # Миграция S3 бакетов
            for bucket in old_config.get('s3_buckets', []):
                cursor.execute("""
                    INSERT OR IGNORE INTO s3_buckets (name, endpoint, access_key, secret_key, region)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    bucket.get('name'),
                    bucket.get('endpoint'),
                    bucket.get('access_key'),
                    bucket.get('secret_key'),
                    bucket.get('region', 'us-east-1')
                ))
            
            # Миграция расписаний
            for schedule in old_config.get('schedules', []):
                cursor.execute(
                    "INSERT INTO schedules (days, time) VALUES (?, ?)",
                    (json.dumps(schedule.get('days', [0,1,2,3,4,5,6])), schedule.get('time', '00:00'))
                )
            
            # Если расписаний нет, добавляем дефолтное
            cursor.execute("SELECT COUNT(*) FROM schedules")
            if cursor.fetchone()[0] == 0:
                cursor.execute(
                    "INSERT INTO schedules (days, time) VALUES (?, ?)",
                    (json.dumps([0,1,2,3,4,5,6]), '00:00')
                )
            
            # Помечаем, что миграция выполнена
            cursor.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?)",
                ('migrated_from_yaml', json.dumps(True))
            )
            
            conn.commit()
            logger.info("Миграция из YAML в SQLite завершена успешно")
            
            # Переименовываем старый файл
            backup_file = self.OLD_YAML_FILE.with_suffix('.yaml.bak')
            self.OLD_YAML_FILE.rename(backup_file)
            logger.info(f"Старый файл конфигурации переименован в {backup_file}")
            
        except Exception as e:
            logger.error(f"Ошибка миграции из YAML: {e}", exc_info=True)
            conn.rollback()
        finally:
            conn.close()
    
    def _load_config_dict(self) -> Dict[str, Any]:
        """Загрузить конфигурацию в виде словаря (для обратной совместимости)"""
        config = dict(self.DEFAULT_SETTINGS)
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Загружаем настройки
        cursor.execute("SELECT key, value FROM settings")
        for row in cursor.fetchall():
            try:
                config[row['key']] = json.loads(row['value'])
            except:
                config[row['key']] = row['value']
        
        # Загружаем папки
        cursor.execute("SELECT path FROM watch_folders")
        config['watch_folders'] = [row['path'] for row in cursor.fetchall()]
        
        # Загружаем правила удаления
        cursor.execute("SELECT * FROM rules")
        config['rules'] = []
        for row in cursor.fetchall():
            rule = {
                'id': row['id'],
                'name': row['name'],
                'pattern': row['pattern'],
                'pattern_type': row['pattern_type'],
                'max_age_minutes': row['max_age_minutes'],
                'enabled': bool(row['enabled']),
                'folders': json.loads(row['folders']) if row['folders'] else [],
                'keep_latest': row['keep_latest'],
                'permanent_delete': bool(row['permanent_delete']),
                'copy_enabled': bool(row['copy_enabled']),
                'copy_s3_bucket_name': row['copy_s3_bucket_name']
            }
            config['rules'].append(rule)
        
        # Загружаем правила синхронизации
        cursor.execute("SELECT * FROM sync_rules")
        config['sync_rules'] = []
        for row in cursor.fetchall():
            rule = {
                'id': row['id'],
                'name': row['name'],
                'bucket_name': row['bucket_name'],
                'enabled': bool(row['enabled']),
                'folders': json.loads(row['folders']) if row['folders'] else [],
                'schedule_type': row['schedule_type'],
                'interval_minutes': row['interval_minutes'],
                'schedule_days': json.loads(row['schedule_days']) if row['schedule_days'] else [],
                'schedule_time': row['schedule_time'],
                'versioning_enabled': bool(row['versioning_enabled']),
                'max_versions': row['max_versions'],
                'max_version_age_days': row['max_version_age_days'],
                'delete_after_sync': bool(row['delete_after_sync']),
                'sync_deletions': bool(row['sync_deletions']),
                'pattern': row['pattern'],
                'pattern_type': row['pattern_type'],
                'last_sync': row['last_sync']
            }
            config['sync_rules'].append(rule)
        
        # Загружаем S3 бакеты
        cursor.execute("SELECT * FROM s3_buckets")
        config['s3_buckets'] = []
        for row in cursor.fetchall():
            bucket = {
                'id': row['id'],
                'name': row['name'],
                'endpoint': row['endpoint'],
                'access_key': row['access_key'],
                'secret_key': row['secret_key'],
                'region': row['region']
            }
            config['s3_buckets'].append(bucket)
        
        # Загружаем расписания
        cursor.execute("SELECT * FROM schedules")
        config['schedules'] = []
        for row in cursor.fetchall():
            schedule = {
                'id': row['id'],
                'days': json.loads(row['days']) if row['days'] else [0,1,2,3,4,5,6],
                'time': row['time']
            }
            config['schedules'].append(schedule)
        
        # Если расписаний нет, добавляем дефолтное
        if not config['schedules']:
            config['schedules'] = [{'days': [0,1,2,3,4,5,6], 'time': '00:00'}]
        
        conn.close()
        return config
    
    def save_config(self, config: Dict[str, Any] = None):
        """Сохранение конфигурации"""
        if config is None:
            config = self.config
        
        # Сохраняем настройку автозапуска в реестр
        auto_start = config.get("auto_start", False)
        self._set_autostart(auto_start)
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Сохраняем общие настройки
        for key in ['check_interval_minutes', 'auto_start', 'schedule_enabled']:
            if key in config:
                cursor.execute(
                    "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                    (key, json.dumps(config[key]))
                )
        
        # Сохраняем расписания
        cursor.execute("DELETE FROM schedules")
        for schedule in config.get('schedules', []):
            cursor.execute(
                "INSERT INTO schedules (days, time) VALUES (?, ?)",
                (json.dumps(schedule.get('days', [0,1,2,3,4,5,6])), schedule.get('time', '00:00'))
            )
        
        conn.commit()
        conn.close()
        self.config = config
    
    def _get_executable_path(self) -> str:
        """Получить путь к исполняемому файлу для автозапуска"""
        if getattr(sys, 'frozen', False):
            return sys.executable
        else:
            launcher_path = Path(__file__).parent.parent / "launcher.pyw"
            if launcher_path.exists():
                python_exe = sys.executable
                if python_exe.endswith('python.exe'):
                    python_exe = python_exe.replace('python.exe', 'pythonw.exe')
                elif not python_exe.endswith('pythonw.exe'):
                    python_dir = Path(python_exe).parent
                    pythonw_exe = python_dir / "pythonw.exe"
                    if pythonw_exe.exists():
                        python_exe = str(pythonw_exe)
                return f'"{python_exe}" "{launcher_path}"'
            else:
                script_path = Path(__file__).parent.parent / "main.py"
                python_exe = sys.executable
                if python_exe.endswith('python.exe'):
                    python_exe = python_exe.replace('python.exe', 'pythonw.exe')
                elif not python_exe.endswith('pythonw.exe'):
                    python_dir = Path(python_exe).parent
                    pythonw_exe = python_dir / "pythonw.exe"
                    if pythonw_exe.exists():
                        python_exe = str(pythonw_exe)
                return f'"{python_exe}" "{script_path}"'
    
    def _set_autostart(self, enable: bool):
        """Установить или удалить автозапуск в реестре Windows"""
        if not WINDOWS:
            return
        
        app_name = "BackupManager"
        registry_key = winreg.HKEY_CURRENT_USER
        registry_path = r"Software\Microsoft\Windows\CurrentVersion\Run"
        
        try:
            if enable:
                key = winreg.OpenKey(registry_key, registry_path, 0, winreg.KEY_SET_VALUE)
                executable_path = self._get_executable_path()
                winreg.SetValueEx(key, app_name, 0, winreg.REG_SZ, executable_path)
                winreg.CloseKey(key)
            else:
                try:
                    key = winreg.OpenKey(registry_key, registry_path, 0, winreg.KEY_SET_VALUE)
                    winreg.DeleteValue(key, app_name)
                    winreg.CloseKey(key)
                except FileNotFoundError:
                    pass
        except Exception as e:
            logger.error(f"Ошибка при работе с реестром автозапуска: {e}", exc_info=True)
    
    def sync_autostart(self):
        """Синхронизировать настройку автозапуска с реестром"""
        auto_start = self.config.get("auto_start", False)
        self._set_autostart(auto_start)
    
    # === Методы для работы с папками ===
    
    def get_watch_folders(self) -> List[Path]:
        """Получить список папок для мониторинга"""
        return [Path(folder) for folder in self.config.get("watch_folders", [])]
    
    def add_watch_folder(self, folder: Path):
        """Добавить папку для мониторинга"""
        folder_str = str(folder.absolute())
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("INSERT OR IGNORE INTO watch_folders (path) VALUES (?)", (folder_str,))
            conn.commit()
            self.config = self._load_config_dict()
        finally:
            conn.close()
    
    def remove_watch_folder(self, folder: Path):
        """Удалить папку из мониторинга"""
        folder_str = str(folder)
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM watch_folders WHERE path = ?", (folder_str,))
        conn.commit()
        conn.close()
        self.config = self._load_config_dict()
    
    # === Методы для работы с правилами удаления ===
    
    def get_rules(self) -> List[Dict[str, Any]]:
        """Получить список правил"""
        return self.config.get("rules", [])
    
    def add_rule(self, rule: Dict[str, Any]):
        """Добавить правило"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO rules (name, pattern, pattern_type, max_age_minutes, 
                enabled, folders, keep_latest, permanent_delete, 
                copy_enabled, copy_s3_bucket_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rule.get('name', 'Без названия'),
            rule.get('pattern', '*'),
            rule.get('pattern_type', 'wildcard'),
            rule.get('max_age_minutes', 43200),
            1 if rule.get('enabled', True) else 0,
            json.dumps(rule.get('folders', [])),
            rule.get('keep_latest', 0),
            1 if rule.get('permanent_delete', False) else 0,
            1 if rule.get('copy_enabled', False) else 0,
            rule.get('copy_s3_bucket_name')
        ))
        conn.commit()
        conn.close()
        self.config = self._load_config_dict()
    
    def update_rule(self, index: int, rule: Dict[str, Any]):
        """Обновить правило по индексу"""
        rules = self.get_rules()
        if 0 <= index < len(rules):
            rule_id = rules[index].get('id')
            if rule_id:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE rules SET name=?, pattern=?, pattern_type=?, max_age_minutes=?,
                        enabled=?, folders=?, keep_latest=?, permanent_delete=?,
                        copy_enabled=?, copy_s3_bucket_name=?
                    WHERE id=?
                """, (
                    rule.get('name', 'Без названия'),
                    rule.get('pattern', '*'),
                    rule.get('pattern_type', 'wildcard'),
                    rule.get('max_age_minutes', 43200),
                    1 if rule.get('enabled', True) else 0,
                    json.dumps(rule.get('folders', [])),
                    rule.get('keep_latest', 0),
                    1 if rule.get('permanent_delete', False) else 0,
                    1 if rule.get('copy_enabled', False) else 0,
                    rule.get('copy_s3_bucket_name'),
                    rule_id
                ))
                conn.commit()
                conn.close()
                self.config = self._load_config_dict()
    
    def remove_rule(self, index: int):
        """Удалить правило по индексу"""
        rules = self.get_rules()
        if 0 <= index < len(rules):
            rule_id = rules[index].get('id')
            if rule_id:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute("DELETE FROM rules WHERE id = ?", (rule_id,))
                conn.commit()
                conn.close()
                self.config = self._load_config_dict()
    
    # === Методы для работы с S3 бакетами ===
    
    def get_s3_buckets(self) -> List[Dict[str, Any]]:
        """Получить список S3 бакетов"""
        return self.config.get("s3_buckets", [])
    
    def add_s3_bucket(self, bucket: Dict[str, Any]):
        """Добавить S3 бакет"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO s3_buckets (name, endpoint, access_key, secret_key, region)
            VALUES (?, ?, ?, ?, ?)
        """, (
            bucket.get('name'),
            bucket.get('endpoint'),
            bucket.get('access_key'),
            bucket.get('secret_key'),
            bucket.get('region', 'us-east-1')
        ))
        conn.commit()
        conn.close()
        self.config = self._load_config_dict()
    
    def update_s3_bucket(self, index: int, bucket: Dict[str, Any]):
        """Обновить S3 бакет по индексу"""
        buckets = self.get_s3_buckets()
        if 0 <= index < len(buckets):
            bucket_id = buckets[index].get('id')
            if bucket_id:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE s3_buckets SET name=?, endpoint=?, access_key=?, secret_key=?, region=?
                    WHERE id=?
                """, (
                    bucket.get('name'),
                    bucket.get('endpoint'),
                    bucket.get('access_key'),
                    bucket.get('secret_key'),
                    bucket.get('region', 'us-east-1'),
                    bucket_id
                ))
                conn.commit()
                conn.close()
                self.config = self._load_config_dict()
    
    def remove_s3_bucket(self, index: int):
        """Удалить S3 бакет по индексу"""
        buckets = self.get_s3_buckets()
        if 0 <= index < len(buckets):
            bucket_id = buckets[index].get('id')
            if bucket_id:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute("DELETE FROM s3_buckets WHERE id = ?", (bucket_id,))
                conn.commit()
                conn.close()
                self.config = self._load_config_dict()
    
    def get_s3_bucket_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Получить S3 бакет по имени"""
        for bucket in self.get_s3_buckets():
            if bucket.get("name") == name:
                return bucket
        return None
    
    # === Методы для работы с правилами синхронизации ===
    
    def get_sync_rules(self) -> List[Dict[str, Any]]:
        """Получить список правил синхронизации"""
        return self.config.get("sync_rules", [])
    
    def add_sync_rule(self, rule: Dict[str, Any]):
        """Добавить правило синхронизации"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO sync_rules (name, bucket_name, enabled, folders, 
                schedule_type, interval_minutes, schedule_days, schedule_time,
                versioning_enabled, max_versions, max_version_age_days,
                delete_after_sync, sync_deletions, pattern, pattern_type, last_sync)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rule.get('name', 'Без названия'),
            rule.get('bucket_name'),
            1 if rule.get('enabled', True) else 0,
            json.dumps(rule.get('folders', [])),
            rule.get('schedule_type', 'interval'),
            rule.get('interval_minutes', 60),
            json.dumps(rule.get('schedule_days', [])),
            rule.get('schedule_time', '03:00'),
            1 if rule.get('versioning_enabled', False) else 0,
            rule.get('max_versions', 5),
            rule.get('max_version_age_days', 30),
            1 if rule.get('delete_after_sync', False) else 0,
            1 if rule.get('sync_deletions', False) else 0,
            rule.get('pattern', '*'),
            rule.get('pattern_type', 'wildcard'),
            rule.get('last_sync')
        ))
        conn.commit()
        conn.close()
        self.config = self._load_config_dict()
    
    def update_sync_rule(self, index: int, rule: Dict[str, Any]):
        """Обновить правило синхронизации по индексу"""
        rules = self.get_sync_rules()
        if 0 <= index < len(rules):
            rule_id = rules[index].get('id')
            if rule_id:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE sync_rules SET name=?, bucket_name=?, enabled=?, folders=?,
                        schedule_type=?, interval_minutes=?, schedule_days=?, schedule_time=?,
                        versioning_enabled=?, max_versions=?, max_version_age_days=?,
                        delete_after_sync=?, sync_deletions=?, pattern=?, pattern_type=?, last_sync=?
                    WHERE id=?
                """, (
                    rule.get('name', 'Без названия'),
                    rule.get('bucket_name'),
                    1 if rule.get('enabled', True) else 0,
                    json.dumps(rule.get('folders', [])),
                    rule.get('schedule_type', 'interval'),
                    rule.get('interval_minutes', 60),
                    json.dumps(rule.get('schedule_days', [])),
                    rule.get('schedule_time', '03:00'),
                    1 if rule.get('versioning_enabled', False) else 0,
                    rule.get('max_versions', 5),
                    rule.get('max_version_age_days', 30),
                    1 if rule.get('delete_after_sync', False) else 0,
                    1 if rule.get('sync_deletions', False) else 0,
                    rule.get('pattern', '*'),
                    rule.get('pattern_type', 'wildcard'),
                    rule.get('last_sync'),
                    rule_id
                ))
                conn.commit()
                conn.close()
                self.config = self._load_config_dict()
    
    def remove_sync_rule(self, index: int):
        """Удалить правило синхронизации по индексу"""
        rules = self.get_sync_rules()
        if 0 <= index < len(rules):
            rule_id = rules[index].get('id')
            if rule_id:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute("DELETE FROM sync_rules WHERE id = ?", (rule_id,))
                conn.commit()
                conn.close()
                self.config = self._load_config_dict()
    
    def get_sync_rule_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Получить правило синхронизации по имени"""
        for rule in self.get_sync_rules():
            if rule.get("name") == name:
                return rule
        return None
