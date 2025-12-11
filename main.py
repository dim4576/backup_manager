"""
Backup Manager - Менеджер автоматического удаления устаревших бэкапов
"""
import sys
import os

def main():
    """Главная функция запуска приложения"""
    # Проверяем, есть ли переменная окружения для режима сервера
    if os.environ.get('BACKUP_MANAGER_SERVER_MODE') == '1' or '--server' in sys.argv:
        # Запускаем в режиме сервера без GUI
        from server_mode import main as server_main
        server_main()
        return
    
    # Проверяем наличие DISPLAY (для Linux) или возможность инициализации GUI
    try:
        # Пытаемся настроить пути к плагинам Qt перед импортом
        try:
            import PyQt5
            from pathlib import Path
            
            # Ищем плагины Qt
            pyqt5_path = Path(PyQt5.__file__).parent
            possible_plugin_paths = [
                pyqt5_path / "Qt5" / "plugins",
                pyqt5_path / "plugins",
                Path(os.environ.get("QT_PLUGIN_PATH", "")),
            ]
            
            # Устанавливаем путь к плагинам, если найден
            for plugin_path in possible_plugin_paths:
                if plugin_path and Path(plugin_path).exists():
                    os.environ.setdefault("QT_PLUGIN_PATH", str(plugin_path))
                    break
        except:
            pass  # Игнорируем ошибки при поиске плагинов
        
        from PyQt5.QtWidgets import QApplication
        from core.backup_manager import BackupManager
        from gui.tray_icon import TrayIcon
        from core.config_manager import ConfigManager
        
        # Пытаемся создать QApplication
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
        
    except Exception as e:
        error_msg = str(e)
        # Если не удалось инициализировать GUI, предлагаем решения
        print(f"\n{'='*80}")
        print("ОШИБКА ИНИЦИАЛИЗАЦИИ GUI")
        print(f"{'='*80}")
        print(f"Ошибка: {error_msg}")
        
        # Определяем тип ошибки
        if "platform plugin" in error_msg.lower() or "qt.qpa.plugin" in error_msg.lower():
            print("\nЭто ошибка с плагинами Qt. Решения:")
            print("\n1. Запустите диагностику:")
            print("   python fix_qt_plugins.py")
            print("\n2. Переустановите PyQt5:")
            print("   pip uninstall PyQt5 PyQt5-Qt5 PyQt5-sip -y")
            print("   pip install PyQt5==5.15.10")
            print("\n3. Или используйте режим сервера (без GUI):")
            print("   python server_mode.py")
        else:
            print("\nВозможные решения:")
            print("1. Для запуска на сервере без GUI используйте:")
            print("   python server_mode.py")
            print("   или")
            print("   BACKUP_MANAGER_SERVER_MODE=1 python main.py")
            print("   или")
            print("   python main.py --server")
            print("\n2. Для диагностики проблемы с Qt:")
            print("   python fix_qt_plugins.py")
            print("\n3. Для установки PyQt5:")
            print("   pip install PyQt5==5.15.10")
            print("\n4. Проверьте наличие дисплея (для Linux):")
            print("   echo $DISPLAY")
        
        print(f"{'='*80}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
