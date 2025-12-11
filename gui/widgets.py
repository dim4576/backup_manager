"""
Кастомные виджеты для GUI
"""
from PyQt5.QtWidgets import QTreeWidget
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QKeyEvent


class FoldersTreeWidget(QTreeWidget):
    """Виджет списка папок с обработкой Delete"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.delete_callback = None
    
    def set_delete_callback(self, callback):
        """Установить обработчик удаления"""
        self.delete_callback = callback
    
    def keyPressEvent(self, event: QKeyEvent):
        """Обработка нажатий клавиш"""
        if event.key() == Qt.Key_Delete and self.delete_callback:
            self.delete_callback()
        else:
            super().keyPressEvent(event)


class RulesTreeWidget(QTreeWidget):
    """Виджет списка правил с обработкой Delete"""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.delete_callback = None
    
    def set_delete_callback(self, callback):
        """Установить обработчик удаления"""
        self.delete_callback = callback
    
    def keyPressEvent(self, event: QKeyEvent):
        """Обработка нажатий клавиш"""
        if event.key() == Qt.Key_Delete and self.delete_callback:
            self.delete_callback()
        else:
            super().keyPressEvent(event)

