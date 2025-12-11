"""
Модуль логирования для Backup Manager
"""
import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler

LOG_DIR = Path.home() / ".backup_manager" / "logs"
LOG_FILE = LOG_DIR / "backup_manager.log"

def setup_logger(name="BackupManager"):
    """Настроить логгер для приложения"""
    logger = logging.getLogger(name)
    
    if logger.handlers:
        # Логгер уже настроен
        return logger
    
    logger.setLevel(logging.INFO)
    
    # Создаём директорию для логов если её нет
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Формат логов
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Обработчик для записи в файл (с ротацией, макс. 5MB, 5 файлов)
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5*1024*1024,  # 5MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Обработчик для консоли (только при запуске через python, не через pythonw)
    if sys.stdout and sys.stdout.isatty():
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.WARNING)  # В консоль только предупреждения и ошибки
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger

def get_log_file_path():
    """Получить путь к файлу логов"""
    return str(LOG_FILE)

