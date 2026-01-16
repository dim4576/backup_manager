"""
Системная иконка в трее (System Tray Icon) - PyQt5 версия
"""
from PyQt5.QtWidgets import QSystemTrayIcon, QMenu, QApplication, QWidget
from PyQt5.QtGui import QIcon, QPainter, QColor, QBrush, QPen, QFont, QPixmap
from PyQt5.QtCore import QSize, Qt, pyqtSignal, QObject, QRect
from gui.settings_window import SettingsWindow
from core.backup_manager import BackupManager
from core.config_manager import ConfigManager
from core.s3_manager import shutdown_s3_connections


class TrayIcon(QObject):
    """Класс для работы с иконкой в системном трее"""
    
    def __init__(self, backup_manager: BackupManager, config: ConfigManager, parent=None, sync_manager=None):
        """Инициализация иконки в трее"""
        super().__init__(parent)
        self.backup_manager = backup_manager
        self.config = config
        self.sync_manager = sync_manager
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
        # Создаём pixmap 256x256 для чёткого отображения
        size = 256
        pixmap = QPixmap(size, size)
        pixmap.fill(Qt.transparent)
        
        painter = QPainter(pixmap)
        if painter.isActive():
            painter.setRenderHint(QPainter.Antialiasing)
            painter.setRenderHint(QPainter.TextAntialiasing)
            painter.setRenderHint(QPainter.SmoothPixmapTransform)
            
            # Рисуем синий круг на весь размер с небольшим отступом
            margin = 8
            brush = QBrush(QColor(0, 120, 215))  # Windows 10 синий
            pen = QPen(QColor(0, 80, 180), 6)
            painter.setBrush(brush)
            painter.setPen(pen)
            painter.drawEllipse(margin, margin, size - margin * 2, size - margin * 2)
            
            # Рисуем букву "B" по центру
            painter.setPen(QPen(QColor(255, 255, 255)))
            font = QFont("Arial", 140, QFont.Bold)
            painter.setFont(font)
            
            # Центрируем текст
            text_rect = QRect(0, 0, size, size)
            painter.drawText(text_rect, Qt.AlignCenter, "B")
            
            painter.end()
        else:
            # Если painter не активен, создаём простую цветную иконку
            pixmap.fill(QColor(0, 120, 215))
        
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
            self.settings_window = SettingsWindow(None, self.config, self.backup_manager, self.sync_manager)
        
        self.settings_window.show()
        self.settings_window.raise_()
        self.settings_window.activateWindow()
    
    def _on_exit_clicked(self):
        """Обработчик клика на 'Выход'"""
        # Останавливаем мониторинг
        self.backup_manager.stop_monitoring()
        
        # Останавливаем синхронизацию
        if self.sync_manager:
            self.sync_manager.stop()
        
        # Закрываем все S3 соединения
        try:
            shutdown_s3_connections()
        except Exception:
            pass
        
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

