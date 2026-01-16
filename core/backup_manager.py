"""
Менеджер бэкапов - основной класс для работы с файлами бэкапов
"""
import time
import threading
import shutil
import re
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
import fnmatch


from core.config_manager import ConfigManager
from core.logger import setup_logger
from core.s3_manager import list_s3_objects, get_s3_object_metadata, upload_file_to_s3

logger = setup_logger()

try:
    import send2trash
    SEND2TRASH_AVAILABLE = True
except ImportError:
    SEND2TRASH_AVAILABLE = False
    import warnings
    warnings.warn("send2trash не установлен. Файлы будут удаляться навсегда вместо корзины. Установите: pip install send2trash")

class BackupManager:
    """Класс для управления файлами бэкапов"""
    
    def __init__(self, config: ConfigManager):
        """Инициализация менеджера бэкапов"""
        self.config = config
        self.running = False
        self._lock = threading.Lock()
        # Отслеживание активных задач удаления
        self.active_tasks: Dict[str, Dict[str, Any]] = {}
        self._task_lock = threading.Lock()
    
    def scan_and_clean(self) -> Dict[str, Any]:
        """
        Сканировать папки и удалить устаревшие файлы и папки
        Возвращает словарь с результатами: {"deleted": [], "errors": []}
        """
        logger.info("Начало сканирования и очистки")
        results = {
            "deleted": [],
            "errors": [],
            "total_scanned": 0
        }
        
        watch_folders = self.config.get_watch_folders()
        rules = [r for r in self.config.get_rules() if r.get("enabled", True)]
        
        logger.info(f"Отслеживаемых папок: {len(watch_folders)}, активных правил: {len(rules)}")
        
        for folder in watch_folders:
            if not folder.exists():
                error_msg = f"Папка не существует: {folder}"
                results["errors"].append(error_msg)
                logger.warning(error_msg)
                continue
            
            try:
                logger.info(f"Обработка папки: {folder}")
                folder_results = self._process_folder(folder, rules)
                results["deleted"].extend(folder_results["deleted"])
                results["errors"].extend(folder_results["errors"])
                results["total_scanned"] += folder_results["total_scanned"]
                
                if folder_results["deleted"]:
                    logger.info(f"Удалено из {folder}: {len(folder_results['deleted'])} объектов")
                if folder_results["errors"]:
                    logger.warning(f"Ошибок при обработке {folder}: {len(folder_results['errors'])}")
            except Exception as e:
                error_msg = f"Ошибка при обработке {folder}: {e}"
                results["errors"].append(error_msg)
                logger.error(error_msg, exc_info=True)
        
        logger.info(f"Сканирование завершено. Удалено: {len(results['deleted'])}, ошибок: {len(results['errors'])}, проверено: {results['total_scanned']}")
        
        # Удаляем все задачи после завершения сканирования
        with self._task_lock:
            self.active_tasks.clear()
        
        return results
    
    def _get_path_size(self, path: Path) -> int:
        """Получить размер файла или папки в байтах"""
        try:
            if path.is_file():
                return path.stat().st_size
            elif path.is_dir():
                total_size = 0
                for dirpath, dirnames, filenames in os.walk(path):
                    for filename in filenames:
                        filepath = os.path.join(dirpath, filename)
                        try:
                            total_size += os.path.getsize(filepath)
                        except (OSError, PermissionError):
                            pass
                return total_size
            return 0
        except (OSError, PermissionError):
            return 0
    
    def _create_task(self, task_id: str, rule_name: str, total_files: int, total_size: int) -> None:
        """Создать задачу для отслеживания"""
        with self._task_lock:
            self.active_tasks[task_id] = {
                "name": f"Удаление по правилу: {rule_name}",
                "rule_name": rule_name,
                "progress": 0,
                "status": "В процессе...",
                "total_files": total_files,
                "processed_files": 0,
                "total_size": total_size,
                "processed_size": 0,
                "start_time": time.time()
            }
    
    def _update_task_progress(self, task_id: str, files_delta: int = 1, size_delta: int = 0) -> None:
        """Обновить прогресс задачи"""
        with self._task_lock:
            if task_id in self.active_tasks:
                task = self.active_tasks[task_id]
                task["processed_files"] += files_delta
                task["processed_size"] += size_delta
                
                # Рассчитываем прогресс в процентах (используем среднее между количеством и размером)
                if task["total_files"] > 0:
                    files_progress = (task["processed_files"] / task["total_files"]) * 100
                else:
                    files_progress = 100
                
                if task["total_size"] > 0:
                    size_progress = (task["processed_size"] / task["total_size"]) * 100
                else:
                    size_progress = 100
                
                # Используем среднее значение
                task["progress"] = int((files_progress + size_progress) / 2)
                
                # Обновляем статус
                task["status"] = f"Обработано: {task['processed_files']}/{task['total_files']} файлов ({self._format_size(task['processed_size'])}/{self._format_size(task['total_size'])})"
    
    def _format_size(self, size_bytes: int) -> str:
        """Форматировать размер в читаемый вид"""
        for unit in ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} ПБ"
    
    def _complete_task(self, task_id: str) -> None:
        """Завершить задачу"""
        with self._task_lock:
            if task_id in self.active_tasks:
                task = self.active_tasks[task_id]
                task["progress"] = 100
                task["status"] = f"Завершено: удалено {task['processed_files']} файлов ({self._format_size(task['processed_size'])})"
                # Удаляем задачу через 5 секунд после завершения
                def remove_task():
                    time.sleep(5)
                    with self._task_lock:
                        if task_id in self.active_tasks:
                            del self.active_tasks[task_id]
                threading.Thread(target=remove_task, daemon=True).start()
    
    def get_active_tasks(self) -> List[Dict[str, Any]]:
        """Получить список активных задач"""
        with self._task_lock:
            return list(self.active_tasks.values())
    
    def _process_folder(self, folder: Path, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Обработать одну папку согласно правилам"""
        results = {
            "deleted": [],
            "errors": [],
            "total_scanned": 0
        }
        
        # Фильтруем правила, которые применяются к этой папке
        applicable_rules = [r for r in rules if self._rule_applies_to_folder(folder, r)]
        
        if not applicable_rules:
            return results
        
        # Собираем только элементы первого уровня (без рекурсии)
        try:
            all_paths = [p for p in folder.iterdir() if p.is_file() or p.is_dir()]
        except (PermissionError, OSError) as e:
            results["errors"].append(f"Ошибка доступа к папке {folder}: {e}")
            return results
        
        # Для каждого правила собираем подходящие объекты и обрабатываем их
        processed_paths = set()  # Отслеживаем уже обработанные пути
        
        for rule in applicable_rules:
            keep_latest = rule.get("keep_latest", 0)
            rule_name = rule.get("name", "Безымянное правило")
            
            # Собираем все объекты, которые соответствуют правилу и по возрасту должны быть удалены
            matching_objects = []
            
            for path in all_paths:
                if path in processed_paths:
                    continue  # Уже обработан другим правилом
                
                if path.is_file():
                    results["total_scanned"] += 1
                elif not path.is_dir():
                    continue
                
                if self._matches_rule(path, rule) and self._should_delete(path, rule):
                    try:
                        mtime = path.stat().st_mtime
                        matching_objects.append((path, mtime))
                    except Exception:
                        continue  # Пропускаем объекты, к которым нет доступа
            
            # Определяем объекты для удаления
            to_delete = []
            if keep_latest > 0 and matching_objects:
                # Сортируем по дате модификации (от самых свежих к самым старым)
                matching_objects.sort(key=lambda x: x[1], reverse=True)
                
                # Оставляем N самых свежих, остальные помечаем на удаление
                to_keep = matching_objects[:keep_latest]
                to_delete = matching_objects[keep_latest:]
                
                # Помечаем оставляемые объекты как обработанные
                for path, _ in to_keep:
                    processed_paths.add(path)
            
            elif not keep_latest:  # keep_latest == 0, удаляем все подходящие
                # Сортируем по дате модификации для единообразия
                matching_objects.sort(key=lambda x: x[1], reverse=False)
                to_delete = matching_objects
            
            # Создаем задачу для отслеживания прогресса удаления
            task_id = None
            if to_delete:
                # Подсчитываем общее количество файлов и размер
                total_files = 0
                total_size = 0
                for path, _ in to_delete:
                    if path.is_file():
                        total_files += 1
                        total_size += self._get_path_size(path)
                    elif path.is_dir():
                        # Для папок считаем количество файлов внутри
                        try:
                            for root, dirs, files in os.walk(path):
                                total_files += len(files)
                                for file in files:
                                    filepath = os.path.join(root, file)
                                    try:
                                        total_size += os.path.getsize(filepath)
                                    except (OSError, PermissionError):
                                        pass
                        except (OSError, PermissionError):
                            # Если не можем посчитать, просто добавляем 1 файл
                            total_files += 1
                
                # Создаем уникальный ID задачи
                task_id = f"{rule_name}_{folder}_{time.time()}"
                self._create_task(task_id, rule_name, total_files, total_size)
                results["task_id"] = task_id
            
            # Удаляем объекты
            for path, _ in to_delete:
                processed_paths.add(path)
                self._delete_path(path, results, rule, task_id)
            
            # Завершаем задачу после обработки правила
            if task_id:
                self._complete_task(task_id)
        
        return results
    
    def _delete_path(self, path: Path, results: Dict[str, Any], rule: Dict[str, Any], task_id: Optional[str] = None):
        """Удалить путь (файл или папку)"""
        permanent_delete = rule.get("permanent_delete", False)
        
        # Проверяем, что путь существует перед удалением
        if not path.exists():
            return
        
        # Получаем размер перед удалением для обновления прогресса
        path_size = 0
        files_count = 0
        if path.is_file():
            path_size = self._get_path_size(path)
            files_count = 1
        elif path.is_dir():
            # Для папок подсчитываем размер и количество файлов
            try:
                for root, dirs, files in os.walk(path):
                    files_count += len(files)
                    for file in files:
                        filepath = os.path.join(root, file)
                        try:
                            path_size += os.path.getsize(filepath)
                        except (OSError, PermissionError):
                            pass
            except (OSError, PermissionError):
                # Если не можем посчитать, используем размер папки
                path_size = self._get_path_size(path)
                files_count = 1
        
        try:
            if permanent_delete:
                # Удаление навсегда
                if path.is_file():
                    path.unlink()
                    results["deleted"].append(str(path) + " (навсегда)")
                elif path.is_dir():
                    shutil.rmtree(path)
                    results["deleted"].append(str(path) + " (папка, навсегда)")
            else:
                # Удаление в корзину (permanent_delete = False)
                if not SEND2TRASH_AVAILABLE:
                    # Если send2trash недоступен, это критическая ошибка
                    error_msg = f"ОШИБКА: send2trash не установлен! Файл НЕ удалён: {path}. Установите: pip install send2trash"
                    results["errors"].append(error_msg)
                    logger.error(error_msg)
                    # НЕ удаляем навсегда - это опасно! Файл остаётся нетронутым
                    return
                
                # Используем send2trash для удаления в корзину
                try:
                    # Конвертируем Path в строку (send2trash требует строку)
                    path_str = str(path.resolve())
                    send2trash.send2trash(path_str)
                    item_type = "папка" if path.is_dir() else "файл"
                    results["deleted"].append(f"{str(path)} ({item_type}, в корзину)")
                    logger.info(f"Удалён в корзину: {path}")
                except Exception as trash_error:
                    # Если удаление в корзину не удалось, НЕ удаляем навсегда - это опасно!
                    error_msg = f"ОШИБКА: Не удалось удалить в корзину {path}: {trash_error}. Файл НЕ удалён."
                    results["errors"].append(error_msg)
                    logger.error(error_msg, exc_info=True)
                    # Файл остаётся нетронутым
                    return
            
            # Обновляем прогресс задачи после успешного удаления
            if task_id:
                self._update_task_progress(task_id, files_delta=files_count, size_delta=path_size)
                
        except Exception as e:
            if path.is_file():
                error_msg = f"Ошибка удаления {path}: {e}"
                results["errors"].append(error_msg)
                logger.error(error_msg, exc_info=True)
            else:
                error_msg = f"Ошибка удаления папки {path}: {e}"
                results["errors"].append(error_msg)
                logger.error(error_msg, exc_info=True)
    
    def _rule_applies_to_folder(self, folder: Path, rule: Dict[str, Any]) -> bool:
        """Проверить, применяется ли правило к данной папке"""
        rule_folders = rule.get("folders", [])
        
        # Если список папок пустой, правило не применяется ни к одной папке
        if not rule_folders:
            return False
        
        # Если в списке есть "*", правило применяется ко всем папкам
        if "*" in rule_folders:
            return True
        
        folder_str = str(folder.absolute())
        
        # Проверяем, есть ли папка в списке
        for rule_folder_str in rule_folders:
            rule_folder = Path(rule_folder_str).absolute()
            # Точное совпадение или папка является подпапкой
            if folder_str == str(rule_folder) or folder_str.startswith(str(rule_folder) + "\\") or folder_str.startswith(str(rule_folder) + "/"):
                return True
        
        return False
    
    def _matches_rule(self, file_path: Path, rule: Dict[str, Any]) -> bool:
        """Проверить, соответствует ли файл паттерну правила"""
        pattern = rule.get("pattern", "*")
        pattern_type = rule.get("pattern_type", "wildcard")
        
        if pattern_type == "regex":
            try:
                # Используем fullmatch для полного совпадения имени файла
                return bool(re.fullmatch(pattern, file_path.name))
            except re.error:
                # Если регулярное выражение некорректно, возвращаем False
                return False
        else:
            # По умолчанию используем wildcard (fnmatch)
            return fnmatch.fnmatch(file_path.name, pattern)
    
    def _should_delete(self, file_path: Path, rule: Dict[str, Any]) -> bool:
        """Проверить, нужно ли удалять файл согласно правилу"""
        # Поддержка старого формата для обратной совместимости
        if "max_age_days" in rule:
            max_age_minutes = rule.get("max_age_days", 30) * 24 * 60
        else:
            max_age_minutes = rule.get("max_age_minutes", 43200)  # По умолчанию 30 дней
        
        try:
            # Получаем время модификации файла
            mtime = file_path.stat().st_mtime
            file_age = datetime.now() - datetime.fromtimestamp(mtime)
            
            # Преобразуем возраст в минуты
            file_age_minutes = file_age.total_seconds() / 60
            
            return file_age_minutes >= max_age_minutes
        except Exception:
            return False
    
    def _check_schedule(self, check_interval_minutes: float = 60) -> bool:
        """Проверить, соответствует ли текущее время расписанию
        
        Args:
            check_interval_minutes: Интервал проверки в минутах (для учета погрешности)
        """
        # Если расписание отключено, всегда разрешаем сканирование
        if not self.config.config.get("schedule_enabled", False):
            return True
        
        # Получаем список расписаний
        schedules = self.config.config.get("schedules", [])
        if not schedules:
            return True  # Если расписаний нет, разрешаем сканирование
        
        # Получаем текущее время
        now = datetime.now()
        current_day = now.weekday()  # 0=понедельник, 6=воскресенье
        
        # Проверяем каждое расписание
        for schedule in schedules:
            schedule_days = schedule.get("days", [])
            if current_day not in schedule_days:
                continue  # День не подходит, проверяем следующее расписание
            
            # Проверяем время
            schedule_time_str = schedule.get("time", "00:00")
            try:
                schedule_hour, schedule_minute = map(int, schedule_time_str.split(":"))
                schedule_datetime = datetime(now.year, now.month, now.day, schedule_hour, schedule_minute)
                
                # Вычисляем разницу во времени
                time_diff = abs((now - schedule_datetime).total_seconds() / 60)  # в минутах
                
                # Если разница меньше или равна половине интервала проверки, считаем что время совпадает
                if time_diff <= (check_interval_minutes / 2):
                    return True  # Найдено подходящее расписание
            except (ValueError, AttributeError):
                # Если не удалось распарсить время, используем точное совпадение
                current_time_str = now.strftime("%H:%M")
                if current_time_str == schedule_time_str:
                    return True
        
        # Ни одно расписание не подошло
        return False
    
    def start_monitoring(self):
        """Запустить мониторинг в фоновом режиме"""
        self.running = True
        
        logger.info("Запуск мониторинга")
        
        def monitor_loop():
            iteration = 0
            while self.running:
                iteration += 1
                
                # Читаем интервал проверки из конфигурации на каждой итерации
                # Поддержка старого формата для обратной совместимости
                if "check_interval_seconds" in self.config.config:
                    check_interval_seconds = self.config.config.get("check_interval_seconds", 3600)
                    check_interval_minutes = check_interval_seconds / 60
                else:
                    check_interval_minutes = self.config.config.get("check_interval_minutes", 60)
                    check_interval_seconds = check_interval_minutes * 60
                
                logger.info(f"Начало проверки #{iteration} (интервал: {check_interval_minutes} минут)")
                
                # Проверяем расписание перед выполнением сканирования
                if self._check_schedule(check_interval_minutes):
                    try:
                        results = self.scan_and_clean()
                        logger.info(f"Проверка #{iteration} завершена. Удалено: {len(results['deleted'])}, ошибок: {len(results['errors'])}, проверено: {results['total_scanned']}")
                    except Exception as e:
                        logger.error(f"Ошибка при выполнении проверки #{iteration}: {e}", exc_info=True)
                else:
                    # Логируем информацию о расписаниях
                    if self.config.config.get("schedule_enabled", False):
                        schedules = self.config.config.get("schedules", [])
                        schedules_info = []
                        day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
                        for sched in schedules:
                            days = sched.get("days", [])
                            time_str = sched.get("time", "00:00")
                            days_str = ", ".join([day_names[d] for d in days]) if days else "Нет дней"
                            schedules_info.append(f"{days_str} в {time_str}")
                        schedules_str = "; ".join(schedules_info)
                        logger.info(f"Проверка #{iteration} пропущена (не соответствует расписанию: {schedules_str})")
                    else:
                        logger.info(f"Проверка #{iteration} пропущена (расписание отключено)")
                
                if self.running:
                    logger.info(f"Ожидание {check_interval_minutes} минут до следующей проверки...")
                    time.sleep(check_interval_seconds)
        
        thread = threading.Thread(target=monitor_loop, daemon=True)
        thread.start()
        logger.info("Поток мониторинга запущен")
    
    def stop_monitoring(self):
        """Остановить мониторинг"""
        if self.running:
            logger.info("Остановка мониторинга...")
            self.running = False
            # Даём время потоку завершиться
            time.sleep(0.5)
            logger.info("Мониторинг остановлен")

