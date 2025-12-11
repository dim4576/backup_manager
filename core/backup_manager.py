"""
Менеджер бэкапов - основной класс для работы с файлами бэкапов
"""
import time
import threading
import shutil
import re
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import fnmatch


from core.config_manager import ConfigManager
from core.logger import setup_logger

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
        return results
    
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
            
            # Если нужно оставлять N самых свежих
            if keep_latest > 0 and matching_objects:
                # Сортируем по дате модификации (от самых свежих к самым старым)
                matching_objects.sort(key=lambda x: x[1], reverse=True)
                
                # Оставляем N самых свежих, остальные помечаем на удаление
                to_keep = matching_objects[:keep_latest]
                to_delete = matching_objects[keep_latest:]
                
                # Помечаем оставляемые объекты как обработанные
                for path, _ in to_keep:
                    processed_paths.add(path)
                
                # Удаляем остальные
                for path, _ in to_delete:
                    processed_paths.add(path)
                    self._delete_path(path, results, rule)
            
            elif not keep_latest:  # keep_latest == 0, удаляем все подходящие
                # Сортируем по дате модификации для единообразия
                matching_objects.sort(key=lambda x: x[1], reverse=False)
                
                for path, _ in matching_objects:
                    processed_paths.add(path)
                    self._delete_path(path, results, rule)
        
        return results
    
    def _delete_path(self, path: Path, results: Dict[str, Any], rule: Dict[str, Any]):
        """Удалить путь (файл или папку)"""
        permanent_delete = rule.get("permanent_delete", False)
        
        # Проверяем, что путь существует перед удалением
        if not path.exists():
            return
        
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
    
    def start_monitoring(self):
        """Запустить мониторинг в фоновом режиме"""
        self.running = True
        # Поддержка старого формата для обратной совместимости
        if "check_interval_seconds" in self.config.config:
            check_interval = self.config.config.get("check_interval_seconds", 3600)
        else:
            check_interval = self.config.config.get("check_interval_minutes", 60) * 60
        
        def monitor_loop():
            while self.running:
                self.scan_and_clean()
                time.sleep(check_interval)
        
        thread = threading.Thread(target=monitor_loop, daemon=True)
        thread.start()
    
    def stop_monitoring(self):
        """Остановить мониторинг"""
        self.running = False

