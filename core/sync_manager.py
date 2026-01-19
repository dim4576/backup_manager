"""
Менеджер синхронизации файлов с S3
Отдельный от удаления модуль для синхронизации папок с S3 бакетами.

Версионирование: каждая папка загружается с датой в названии.
Например: backups_2026-01-16_14-30/file.txt
"""

import os
import re
import fnmatch
import threading
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone

from core.config_manager import ConfigManager
from core.logger import setup_logger
from core.s3_manager import (
    upload_file_to_s3,
    list_s3_objects,
    delete_s3_object,
    format_size
)

logger = setup_logger("SyncManager")


class SyncManager:
    """Менеджер синхронизации файлов с S3"""
    
    def __init__(self, config: ConfigManager):
        """
        Инициализация менеджера синхронизации
        
        Args:
            config: Менеджер конфигурации
        """
        self.config = config
        self.running = False
        self._thread: Optional[threading.Thread] = None
        self._active_tasks: Dict[str, Dict[str, Any]] = {}
        self._tasks_lock = threading.Lock()
        
        # Время последней синхронизации для каждого правила
        self._last_sync: Dict[str, datetime] = {}
    
    def start(self):
        """Запустить синхронизацию в фоновом режиме"""
        if self.running:
            logger.warning("Менеджер синхронизации уже запущен")
            return
        
        self.running = True
        self._thread = threading.Thread(target=self._sync_loop, daemon=True, name="SyncManager")
        self._thread.start()
        logger.info("Менеджер синхронизации запущен")
    
    def stop(self):
        """Остановить синхронизацию"""
        if not self.running:
            return
        
        self.running = False
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("Менеджер синхронизации остановлен")
    
    def _sync_loop(self):
        """Основной цикл синхронизации"""
        while self.running:
            try:
                self._check_and_run_sync()
            except Exception as e:
                logger.error(f"Ошибка в цикле синхронизации: {e}", exc_info=True)
            
            # Проверяем каждую минуту
            time.sleep(60)
    
    def _check_and_run_sync(self):
        """Проверить правила и запустить синхронизацию если пора"""
        rules = self.config.get_sync_rules()
        now = datetime.now(timezone.utc)
        now_local = datetime.now()
        
        for i, rule in enumerate(rules):
            if not rule.get("enabled", True):
                continue
            
            rule_name = rule.get("name", f"rule_{i}")
            schedule_type = rule.get("schedule_type", "interval")
            
            # Получаем время последней синхронизации
            last_sync = self._last_sync.get(rule_name)
            if last_sync is None:
                last_sync_str = rule.get("last_sync")
                if last_sync_str:
                    try:
                        last_sync = datetime.fromisoformat(last_sync_str)
                    except:
                        last_sync = None
            
            # Проверяем, пора ли синхронизировать
            should_sync = False
            
            if schedule_type == "schedule":
                # Расписание по дням недели и времени
                should_sync = self._check_schedule(rule, last_sync, now_local)
            else:
                # По интервалу
                interval_minutes = rule.get("interval_minutes", 60)
                if last_sync is None:
                    should_sync = True
                else:
                    elapsed_minutes = (now - last_sync).total_seconds() / 60
                    if elapsed_minutes >= interval_minutes:
                        should_sync = True
            
            if should_sync:
                # Запускаем синхронизацию в отдельном потоке
                threading.Thread(
                    target=self._sync_rule,
                    args=(rule, i),
                    daemon=True,
                    name=f"Sync_{rule_name}"
                ).start()
                
                # Обновляем время последней синхронизации
                self._last_sync[rule_name] = now
                self._update_rule_last_sync(i, now)
    
    def _check_schedule(self, rule: Dict[str, Any], last_sync: Optional[datetime], now_local: datetime) -> bool:
        """
        Проверить, наступило ли время по расписанию
        
        Args:
            rule: Правило синхронизации
            last_sync: Время последней синхронизации
            now_local: Текущее локальное время
            
        Returns:
            True если пора запускать синхронизацию
        """
        # Маппинг дней недели: Python weekday() -> наши ключи
        day_map = {0: "mon", 1: "tue", 2: "wed", 3: "thu", 4: "fri", 5: "sat", 6: "sun"}
        
        # Получаем настройки расписания
        schedule_days = rule.get("schedule_days", [])
        schedule_time_str = rule.get("schedule_time", "03:00")
        
        if not schedule_days:
            return False
        
        # Текущий день недели
        current_day = day_map.get(now_local.weekday())
        if current_day not in schedule_days:
            return False
        
        # Парсим время расписания
        try:
            time_parts = schedule_time_str.split(":")
            schedule_hour = int(time_parts[0])
            schedule_minute = int(time_parts[1])
        except:
            schedule_hour = 3
            schedule_minute = 0
        
        # Время запланированной синхронизации сегодня
        scheduled_today = now_local.replace(
            hour=schedule_hour, 
            minute=schedule_minute, 
            second=0, 
            microsecond=0
        )
        
        # Проверяем: текущее время >= запланированного?
        if now_local < scheduled_today:
            return False
        
        # Окно запуска: синхронизация запускается только в течение 5 минут после запланированного времени
        # Это предотвращает запуск при создании правила, если текущее время далеко после расписания
        from datetime import timedelta
        schedule_window_end = scheduled_today + timedelta(minutes=5)
        
        # Если текущее время вне окна запуска и last_sync не задан - не запускаем
        # (правило скорее всего только создано)
        if now_local > schedule_window_end and last_sync is None:
            return False
        
        # Проверяем: не было ли синхронизации после запланированного времени сегодня?
        if last_sync:
            # Конвертируем в локальное время если нужно
            if last_sync.tzinfo is not None:
                last_sync_local = last_sync.astimezone().replace(tzinfo=None)
            else:
                last_sync_local = last_sync
            
            # Если синхронизация была сегодня после запланированного времени - не запускаем
            if last_sync_local >= scheduled_today:
                return False
        
        return True
    
    def _update_rule_last_sync(self, rule_index: int, sync_time: datetime):
        """Обновить время последней синхронизации в конфиге"""
        rules = self.config.get_sync_rules()
        if 0 <= rule_index < len(rules):
            rules[rule_index]["last_sync"] = sync_time.isoformat()
            self.config.config["sync_rules"] = rules
    
    def _sync_rule(self, rule: Dict[str, Any], rule_index: int):
        """
        Синхронизировать папки по правилу
        
        При включённом версионировании папка загружается с датой в названии:
        folder_name_2026-01-16_14-30/
        
        Args:
            rule: Правило синхронизации
            rule_index: Индекс правила
        """
        rule_name = rule.get("name", f"rule_{rule_index}")
        bucket_name = rule.get("bucket_name")
        
        if not bucket_name:
            logger.error(f"Правило '{rule_name}': не указан бакет")
            return
        
        # Получаем конфигурацию бакета
        bucket_config = self.config.get_s3_bucket_by_name(bucket_name)
        if not bucket_config:
            logger.error(f"Правило '{rule_name}': бакет '{bucket_name}' не найден в настройках")
            return
        
        # Создаём задачу
        task_id = f"sync_{rule_name}_{int(time.time())}"
        self._create_task(task_id, rule_name)
        
        try:
            folders = rule.get("folders", [])
            pattern = rule.get("pattern", "*")
            pattern_type = rule.get("pattern_type", "wildcard")
            versioning_enabled = rule.get("versioning_enabled", False)
            
            # Получаем S3 credentials
            access_key = bucket_config.get("access_key")
            secret_key = bucket_config.get("secret_key")
            region = bucket_config.get("region", "us-east-1")
            endpoint = bucket_config.get("endpoint")
            
            # Формируем timestamp для версии
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
            
            # Собираем файлы для синхронизации
            files_to_sync = []
            for folder_path in folders:
                folder = Path(folder_path)
                if not folder.exists():
                    logger.warning(f"Папка не существует: {folder}")
                    continue
                
                for file_path in folder.rglob("*"):
                    if file_path.is_file():
                        # Проверяем паттерн
                        if self._matches_pattern(file_path.name, pattern, pattern_type):
                            # Формируем S3 ключ
                            relative_path = file_path.relative_to(folder)
                            
                            if versioning_enabled:
                                # С версионированием: folder_name_2026-01-16_14-30/path/file.txt
                                s3_key = f"{folder.name}_{timestamp}/{relative_path}".replace("\\", "/")
                            else:
                                # Без версионирования: folder_name/path/file.txt
                                s3_key = f"{folder.name}/{relative_path}".replace("\\", "/")
                            
                            files_to_sync.append((file_path, s3_key, folder.name))
            
            total_files = len(files_to_sync)
            self._update_task(task_id, total=total_files)
            
            logger.info(f"Правило '{rule_name}': найдено {total_files} файлов для синхронизации")
            
            # Синхронизируем файлы
            synced_count = 0
            total_bytes = sum(fp.stat().st_size for fp, _, _ in files_to_sync if fp.exists())
            uploaded_bytes = 0
            
            for file_path, s3_key, folder_name in files_to_sync:
                if not self.running:
                    break
                
                try:
                    file_size = file_path.stat().st_size if file_path.exists() else 0
                    current_file_name = file_path.name
                    
                    # Callback для прогресса текущего файла
                    def progress_callback(filename, uploaded, total):
                        nonlocal uploaded_bytes
                        current_uploaded = uploaded_bytes + uploaded
                        percent = int(current_uploaded / total_bytes * 100) if total_bytes > 0 else 0
                        status = f"Загрузка: {filename} ({format_size(uploaded)}/{format_size(total)})"
                        self._update_task(
                            task_id, 
                            progress=percent,
                            status=status,
                            current_file=filename,
                            current_uploaded=uploaded,
                            current_total=total
                        )
                    
                    # Обновляем статус перед загрузкой
                    self._update_task(
                        task_id,
                        status=f"Загрузка: {current_file_name} (0/{format_size(file_size)})",
                        current_file=current_file_name
                    )
                    
                    # Загружаем файл с отслеживанием прогресса
                    success, error = upload_file_to_s3(
                        str(file_path), bucket_name, s3_key,
                        access_key, secret_key, region, endpoint,
                        progress_callback=progress_callback
                    )
                    
                    if success:
                        synced_count += 1
                        uploaded_bytes += file_size
                        logger.debug(f"Загружен: {s3_key}")
                        
                        # Удаляем локальный файл если настроено
                        if rule.get("delete_after_sync"):
                            try:
                                file_path.unlink()
                                logger.debug(f"Удалён локальный файл: {file_path}")
                            except Exception as e:
                                logger.error(f"Ошибка удаления файла {file_path}: {e}")
                    else:
                        logger.error(f"Ошибка загрузки {s3_key}: {error}")
                    
                    # Обновляем общий прогресс
                    overall_percent = int(uploaded_bytes / total_bytes * 100) if total_bytes > 0 else 0
                    self._update_task(
                        task_id, 
                        processed=synced_count,
                        progress=overall_percent,
                        status=f"Загружено {synced_count} из {total_files} файлов ({format_size(uploaded_bytes)}/{format_size(total_bytes)})"
                    )
                    
                except Exception as e:
                    logger.error(f"Ошибка синхронизации файла {file_path}: {e}")
            
            # Ротация версий (удаление старых папок с датой)
            if versioning_enabled:
                # Собираем уникальные имена папок
                folder_names = set()
                for folder_path in folders:
                    folder = Path(folder_path)
                    if folder.exists():
                        folder_names.add(folder.name)
                
                for folder_name in folder_names:
                    self._rotate_folder_versions(
                        folder_name, bucket_name,
                        access_key, secret_key, region, endpoint,
                        rule
                    )
            
            logger.info(f"Правило '{rule_name}': синхронизировано {synced_count} из {total_files} файлов")
            
        except Exception as e:
            logger.error(f"Ошибка синхронизации правила '{rule_name}': {e}", exc_info=True)
        finally:
            self._complete_task(task_id)
    
    def _matches_pattern(self, filename: str, pattern: str, pattern_type: str) -> bool:
        """Проверить соответствие файла паттерну"""
        if pattern == "*":
            return True
        
        if pattern_type == "regex":
            try:
                return bool(re.match(pattern, filename))
            except re.error:
                return False
        else:  # wildcard
            return fnmatch.fnmatch(filename, pattern)
    
    def _rotate_folder_versions(
        self,
        folder_name: str,
        bucket_name: str,
        access_key: str,
        secret_key: str,
        region: str,
        endpoint: Optional[str],
        rule: Dict[str, Any]
    ):
        """
        Ротация версий папки (удаление старых папок с датой)
        
        Ищет папки с паттерном: folder_name_YYYY-MM-DD_HH-MM/
        И удаляет лишние по количеству или возрасту.
        """
        max_versions = rule.get("max_versions", 0)
        max_age_days = rule.get("max_version_age_days", 0)
        
        if max_versions == 0 and max_age_days == 0:
            return  # Ротация не настроена
        
        # Получаем все объекты в бакете
        all_objects = list_s3_objects(
            bucket_name, access_key, secret_key, region, endpoint
        )
        
        if not all_objects:
            return
        
        # Паттерн для поиска версий папки: folder_name_YYYY-MM-DD_HH-MM
        version_pattern = re.compile(
            rf"^{re.escape(folder_name)}_(\d{{4}}-\d{{2}}-\d{{2}}_\d{{2}}-\d{{2}})/"
        )
        
        # Находим все версии папки
        versions: Dict[str, List[Dict[str, Any]]] = {}  # timestamp -> list of objects
        for obj in all_objects:
            key = obj.get("key", "")
            match = version_pattern.match(key)
            if match:
                timestamp_str = match.group(1)
                if timestamp_str not in versions:
                    versions[timestamp_str] = []
                versions[timestamp_str].append(obj)
        
        if not versions:
            logger.debug(f"Нет версий папки '{folder_name}' для ротации")
            return
        
        # Сортируем версии по дате (новые первые)
        sorted_versions = sorted(versions.keys(), reverse=True)
        
        logger.info(f"Найдено {len(sorted_versions)} версий папки '{folder_name}'")
        
        now = datetime.now(timezone.utc)
        
        for i, timestamp_str in enumerate(sorted_versions):
            should_delete = False
            
            # Проверяем количество версий
            if max_versions > 0 and i >= max_versions:
                should_delete = True
                logger.debug(f"Версия {timestamp_str} превышает лимит {max_versions}")
            
            # Проверяем возраст версии
            if max_age_days > 0 and not should_delete:
                try:
                    version_date = datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M")
                    version_date = version_date.replace(tzinfo=timezone.utc)
                    age_days = (now - version_date).days
                    if age_days > max_age_days:
                        should_delete = True
                        logger.debug(f"Версия {timestamp_str} старше {max_age_days} дней (возраст: {age_days})")
                except ValueError:
                    pass
            
            if should_delete:
                # Удаляем все объекты этой версии
                objects_to_delete = versions[timestamp_str]
                deleted_count = 0
                
                for obj in objects_to_delete:
                    obj_key = obj.get("key")
                    success, error = delete_s3_object(
                        bucket_name, obj_key,
                        access_key, secret_key, region, endpoint
                    )
                    if success:
                        deleted_count += 1
                    else:
                        logger.error(f"Ошибка удаления {obj_key}: {error}")
                
                logger.info(f"Удалена версия '{folder_name}_{timestamp_str}' ({deleted_count} файлов)")
    
    # === Управление задачами ===
    
    def _create_task(self, task_id: str, name: str):
        """Создать задачу"""
        with self._tasks_lock:
            self._active_tasks[task_id] = {
                "id": task_id,
                "name": f"Синхронизация: {name}",
                "status": "Начало синхронизации...",
                "progress": 0,
                "total": 0,
                "processed": 0,
                "start_time": time.time()
            }
    
    def _update_task(self, task_id: str, **kwargs):
        """Обновить задачу"""
        with self._tasks_lock:
            if task_id in self._active_tasks:
                task = self._active_tasks[task_id]
                task.update(kwargs)
                
                # Пересчитываем прогресс только если не передан явно
                if "progress" not in kwargs and "status" not in kwargs:
                    total = task.get("total", 0)
                    processed = task.get("processed", 0)
                    if total > 0:
                        task["progress"] = int(processed / total * 100)
                        task["status"] = f"Обработано {processed} из {total} файлов"
    
    def _complete_task(self, task_id: str):
        """Завершить задачу"""
        with self._tasks_lock:
            if task_id in self._active_tasks:
                del self._active_tasks[task_id]
    
    def get_active_tasks(self) -> List[Dict[str, Any]]:
        """Получить список активных задач"""
        with self._tasks_lock:
            return list(self._active_tasks.values())
    
    def run_sync_now(self, rule_index: int):
        """Запустить синхронизацию правила немедленно"""
        rules = self.config.get_sync_rules()
        if 0 <= rule_index < len(rules):
            rule = rules[rule_index]
            threading.Thread(
                target=self._sync_rule,
                args=(rule, rule_index),
                daemon=True,
                name=f"Sync_Manual_{rule.get('name', rule_index)}"
            ).start()
            return True
        return False
