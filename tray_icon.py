"""
Системная иконка в трее (System Tray Icon) - PyQt5 версия
"""
from PyQt5.QtWidgets import QSystemTrayIcon, QMenu, QApplication, QWidget
from PyQt5.QtGui import QIcon, QPainter, QColor, QBrush, QPen, QFont, QPixmap
from PyQt5.QtCore import QSize, Qt, pyqtSignal, QObject
from gui import SettingsWindow
from backup_manager import BackupManager
from config_manager import ConfigManager

class TrayIcon(QObject):
    """Класс для работы с иконкой в системном трее"""
    
    def __init__(self, backup_manager: BackupManager, config: ConfigManager, parent=None):
        """Инициализация иконки в трее"""
        super().__init__(parent)
        self.backup_manager = backup_manager
        self.config = config
        self.settings_window = None
        self.app = parent  # QApplication
        
        # Создаём системную иконку
        self.tray_icon = QSystemTrayIcon(self)
        
        # Создаём иконку
        icon = self._create_icon()
        self.tray_icon.setIcon(icon)
        self.tray_icon.setToolTip("Backup Manager")
        
        # Создаём меню
        menu = self._create_menu()
        self.tray_icon.setContextMenu(menu)
        
        # Обработчик двойного клика
        self.tray_icon.activated.connect(self._on_tray_activated)
        
        # Показываем иконку
        self.tray_icon.show()
        
        # Запускаем мониторинг
        self.backup_manager.start_monitoring()
    
    def _create_icon(self):
        """Создать изображение иконки"""
        # Создаём pixmap 64x64 с прозрачным фоном
        pixmap = QPixmap(64, 64)
        pixmap.fill(Qt.transparent)
        
        painter = QPainter(pixmap)
        if painter.isActive():
            painter.setRenderHint(QPainter.Antialiasing)
            
            # Рисуем синий круг
            brush = QBrush(QColor(0, 100, 200))
            pen = QPen(QColor(0, 50, 150), 2)
            painter.setBrush(brush)
            painter.setPen(pen)
            painter.drawEllipse(10, 10, 44, 44)
            
            # Рисуем букву "B"
            painter.setPen(QPen(QColor(255, 255, 255)))
            font = QFont("Arial", 24, QFont.Bold)
            painter.setFont(font)
            painter.drawText(20, 45, "B")
            
            painter.end()
        else:
            # Если painter не активен, создаём простую цветную иконку
            pixmap.fill(QColor(0, 100, 200))
        
        # Создаём иконку из pixmap
        icon = QIcon(pixmap)
        return icon
    
    def _on_tray_activated(self, reason):
        """Обработчик активации иконки в трее"""
        if reason == QSystemTrayIcon.DoubleClick:
            self._show_settings()
    
    def _on_cleanup_clicked(self):
        """Обработчик клика на 'Очистить сейчас'"""
        results = self.backup_manager.scan_and_clean()
        deleted_count = len(results["deleted"])
        error_count = len(results["errors"])
        
        message = f"Очистка завершена.\nУдалено файлов: {deleted_count}"
        if error_count > 0:
            message += f"\nОшибок: {error_count}"
        
        self.tray_icon.showMessage(
            "Backup Manager",
            message,
            QSystemTrayIcon.Information,
            3000
        )
    
    def _on_settings_clicked(self):
        """Обработчик клика на 'Настройки'"""
        self._show_settings()
    
    def _show_settings(self):
        """Показать окно настроек"""
        if self.settings_window is None or not self.settings_window.isVisible():
            # QDialog принимает QWidget или None, а не QApplication
            self.settings_window = SettingsWindow(None, self.config, self.backup_manager)
        
        self.settings_window.show()
        self.settings_window.raise_()
        self.settings_window.activateWindow()
    
    def _on_exit_clicked(self):
        """Обработчик клика на 'Выход'"""
        self.backup_manager.stop_monitoring()
        self.tray_icon.hide()
        if self.app:
            self.app.quit()
    
    def _create_menu(self):
        """Создать меню для иконки в трее"""
        menu = QMenu()
        
        action_cleanup = menu.addAction("Очистить сейчас")
        action_cleanup.triggered.connect(self._on_cleanup_clicked)
        
        action_settings = menu.addAction("Настройки")
        action_settings.triggered.connect(self._on_settings_clicked)
        
        menu.addSeparator()
        
        action_exit = menu.addAction("Выход")
        action_exit.triggered.connect(self._on_exit_clicked)
        
        return menu
    
    def show_message(self, title, message, duration=3000):
        """Показать сообщение из трея"""
        self.tray_icon.showMessage(title, message, QSystemTrayIcon.Information, duration)
