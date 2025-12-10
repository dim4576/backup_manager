"""
Backup Manager - Менеджер автоматического удаления устаревших бэкапов
"""
import sys
from PyQt5.QtWidgets import QApplication
from backup_manager import BackupManager
from tray_icon import TrayIcon
from config_manager import ConfigManager

def main():
    """Главная функция запуска приложения"""
    # Создаём QApplication (обязательно нужен для PyQt)
    app = QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(False)  # Не закрывать приложение при закрытии всех окон
    
    # Создаём компоненты
    config = ConfigManager()
    
    # Синхронизируем настройку автозапуска с реестром при старте
    config.sync_autostart()
    
    backup_manager = BackupManager(config)
    tray_icon = TrayIcon(backup_manager, config, app)
    
    # Запускаем цикл событий
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
