"""
Backup Manager - Режим работы на сервере без GUI
Используется для запуска на удалённых серверах без графического интерфейса
"""
import sys
import time
import signal
from core.backup_manager import BackupManager
from core.config_manager import ConfigManager
from core.logger import setup_logger

logger = setup_logger("BackupManagerServer")

# Глобальная переменная для backup_manager (для обработчика сигналов)
backup_manager_instance = None


def signal_handler(sig, frame):
    """Обработчик сигнала для корректного завершения"""
    logger.info("Получен сигнал завершения, останавливаем мониторинг...")
    global backup_manager_instance
    if backup_manager_instance:
        backup_manager_instance.stop_monitoring()
    sys.exit(0)


def main():
    """Главная функция запуска в режиме сервера"""
    global backup_manager_instance
    logger.info("Запуск Backup Manager в режиме сервера (без GUI)")
    
    # Регистрируем обработчики сигналов
    if hasattr(signal, 'SIGINT'):
        signal.signal(signal.SIGINT, signal_handler)
    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Создаём компоненты
        config = ConfigManager()
        
        # На сервере не нужно синхронизировать автозапуск (это только для Windows)
        # Проверяем, что мы на Windows перед синхронизацией автозапуска
        try:
            import platform
            if platform.system() == 'Windows':
                config.sync_autostart()
        except:
            pass  # Игнорируем ошибки автозапуска на не-Windows системах
        
        backup_manager_instance = BackupManager(config)
        
        # Запускаем мониторинг
        logger.info("Мониторинг запущен")
        backup_manager_instance.start_monitoring()
        
        # Выполняем первичную очистку
        logger.info("Выполнение первичной очистки...")
        results = backup_manager_instance.scan_and_clean()
        logger.info(f"Первичная очистка завершена. Удалено: {len(results['deleted'])}, ошибок: {len(results['errors'])}")
        
        # Бесконечный цикл для поддержания работы
        logger.info("Сервер работает. Нажмите Ctrl+C для остановки.")
        try:
            while True:
                time.sleep(60)  # Проверяем каждую минуту, не нужно ли завершить работу
        except KeyboardInterrupt:
            logger.info("Получен сигнал прерывания от пользователя")
        finally:
            if backup_manager_instance:
                backup_manager_instance.stop_monitoring()
            logger.info("Мониторинг остановлен, завершение работы")
            
    except Exception as e:
        logger.error(f"Критическая ошибка при запуске: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

