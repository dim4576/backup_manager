"""
Скрипт для диагностики и исправления проблем с Qt плагинами
"""
import sys
import os
from pathlib import Path


def find_qt_plugins():
    """Найти путь к плагинам Qt"""
    try:
        import PyQt5
        pyqt5_path = Path(PyQt5.__file__).parent
        
        possible_paths = [
            pyqt5_path / "Qt5" / "plugins",
            pyqt5_path / "plugins",
            pyqt5_path.parent / "PyQt5" / "Qt5" / "plugins",
        ]
        
        for path in possible_paths:
            if path.exists():
                platforms_dir = path / "platforms"
                if platforms_dir.exists():
                    dll_files = list(platforms_dir.glob("*.dll")) + list(platforms_dir.glob("*.so"))
                    if dll_files:
                        return str(path)
        
        return None
    except ImportError:
        return None


def check_qt_installation():
    """Проверить установку PyQt5"""
    print("=" * 80)
    print("Диагностика установки PyQt5")
    print("=" * 80)
    
    # Проверка импорта
    try:
        import PyQt5
        print(f"✓ PyQt5 установлен: {PyQt5.__file__}")
    except ImportError:
        print("✗ PyQt5 не установлен")
        print("\nРешение: pip install PyQt5==5.15.10")
        return False
    
    # Поиск плагинов
    plugin_path = find_qt_plugins()
    if plugin_path:
        print(f"✓ Плагины Qt найдены: {plugin_path}")
        
        platforms_dir = Path(plugin_path) / "platforms"
        if platforms_dir.exists():
            dll_files = list(platforms_dir.glob("*.dll")) + list(platforms_dir.glob("*.so"))
            print(f"✓ Найдено плагинов платформы: {len(dll_files)}")
            for dll in dll_files:
                print(f"  - {dll.name}")
            
            # Проверяем наличие windows плагина
            windows_plugin = platforms_dir / "qwindows.dll" if sys.platform == 'win32' else platforms_dir / "qwindows.so"
            if not windows_plugin.exists():
                windows_plugin = platforms_dir / "qwindows" + (".dll" if sys.platform == 'win32' else ".so")
            
            if windows_plugin.exists():
                print(f"✓ Плагин 'windows' найден: {windows_plugin}")
            else:
                print(f"✗ Плагин 'windows' не найден в {platforms_dir}")
                print("\nРешение: Переустановите PyQt5:")
                print("  pip uninstall PyQt5 PyQt5-Qt5 PyQt5-sip -y")
                print("  pip install PyQt5==5.15.10")
        else:
            print(f"✗ Директория platforms не найдена: {platforms_dir}")
    else:
        print("✗ Плагины Qt не найдены")
        print("\nРешение: Переустановите PyQt5:")
        print("  pip uninstall PyQt5 PyQt5-Qt5 PyQt5-sip -y")
        print("  pip install PyQt5==5.15.10")
        return False
    
    # Проверка переменных окружения
    print("\nПеременные окружения:")
    qt_plugin_path = os.environ.get("QT_PLUGIN_PATH")
    if qt_plugin_path:
        print(f"  QT_PLUGIN_PATH: {qt_plugin_path}")
        if Path(qt_plugin_path).exists():
            print("  ✓ Путь существует")
        else:
            print("  ✗ Путь не существует")
    else:
        print("  QT_PLUGIN_PATH: не установлена")
        if plugin_path:
            print(f"\n  Рекомендуется установить:")
            if sys.platform == 'win32':
                print(f"  set QT_PLUGIN_PATH={plugin_path}")
            else:
                print(f"  export QT_PLUGIN_PATH={plugin_path}")
    
    # Попытка инициализации Qt
    print("\nПопытка инициализации Qt:")
    try:
        from PyQt5.QtWidgets import QApplication
        import sys as sys_module
        
        # Устанавливаем путь к плагинам если найден
        if plugin_path and not qt_plugin_path:
            os.environ["QT_PLUGIN_PATH"] = plugin_path
        
        app = QApplication(sys_module.argv)
        print("  ✓ QApplication успешно создан")
        print(f"  Пути к библиотекам: {app.libraryPaths()}")
        app.quit()
        return True
    except Exception as e:
        print(f"  ✗ Ошибка: {e}")
        return False


def fix_qt_plugins():
    """Попытаться исправить проблему с плагинами"""
    plugin_path = find_qt_plugins()
    if plugin_path:
        os.environ["QT_PLUGIN_PATH"] = plugin_path
        print(f"\n✓ Установлена переменная QT_PLUGIN_PATH={plugin_path}")
        print("Попробуйте запустить приложение снова.")
        return True
    return False


if __name__ == "__main__":
    success = check_qt_installation()
    
    if not success:
        print("\n" + "=" * 80)
        print("Попытка автоматического исправления...")
        print("=" * 80)
        if fix_qt_plugins():
            print("\nПроверка после исправления:")
            check_qt_installation()
        else:
            print("\nАвтоматическое исправление не удалось.")
            print("Выполните переустановку PyQt5 вручную.")
    
    sys.exit(0 if success else 1)

