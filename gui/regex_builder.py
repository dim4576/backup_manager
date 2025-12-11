"""
Визуальный конструктор регулярных выражений
"""
import re
from PyQt5.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QGroupBox,
                             QLabel, QLineEdit, QPushButton, QDialogButtonBox,
                             QGridLayout, QMessageBox)
from PyQt5.QtCore import Qt


class RegexBuilderDialog(QDialog):
    """Визуальный конструктор регулярных выражений"""
    
    def __init__(self, parent, initial_pattern=""):
        """Инициализация диалога конструктора"""
        super().__init__(parent)
        self.setWindowTitle("Конструктор регулярных выражений")
        self.setMinimumSize(700, 600)
        
        layout = QVBoxLayout(self)
        
        # Результирующее регулярное выражение
        result_group = QGroupBox("Результат")
        result_layout = QVBoxLayout()
        self.result_edit = QLineEdit(initial_pattern)
        self.result_edit.textChanged.connect(self._update_preview)
        result_layout.addWidget(self.result_edit)
        result_group.setLayout(result_layout)
        layout.addWidget(result_group)
        
        # Предварительный просмотр и тестирование
        preview_group = QGroupBox("Тестирование")
        preview_layout = QVBoxLayout()
        
        test_label = QLabel("Тестовый текст:")
        preview_layout.addWidget(test_label)
        self.test_edit = QLineEdit()
        self.test_edit.textChanged.connect(self._update_preview)
        preview_layout.addWidget(self.test_edit)
        
        preview_label = QLabel("Результат:")
        preview_layout.addWidget(preview_label)
        self.preview_label = QLabel("")
        self.preview_label.setStyleSheet("background-color: #f0f0f0; padding: 5px; border: 1px solid #ccc;")
        self.preview_label.setWordWrap(True)
        preview_layout.addWidget(self.preview_label)
        
        preview_group.setLayout(preview_layout)
        layout.addWidget(preview_group)
        
        # Элементы конструктора
        builder_group = QGroupBox("Элементы")
        builder_layout = QVBoxLayout()
        
        # Начало и конец строки
        anchors_layout = QHBoxLayout()
        anchors_label = QLabel("Привязки:")
        anchors_layout.addWidget(anchors_label)
        btn_start = QPushButton("^ (начало)")
        btn_start.clicked.connect(lambda: self._insert_text("^"))
        anchors_layout.addWidget(btn_start)
        btn_end = QPushButton("$ (конец)")
        btn_end.clicked.connect(lambda: self._insert_text("$"))
        anchors_layout.addWidget(btn_end)
        anchors_layout.addStretch()
        builder_layout.addLayout(anchors_layout)
        
        # Базовые элементы
        basic_layout = QGridLayout()
        basic_label = QLabel("Базовые элементы:")
        basic_layout.addWidget(basic_label, 0, 0, 1, 3)
        
        basic_buttons = [
            (". (любой символ)", "."),
            ("\\d (цифра)", "\\d"),
            ("\\D (не цифра)", "\\D"),
            ("\\w (буква/цифра/_)", "\\w"),
            ("\\W (не буква/цифра/_)", "\\W"),
            ("\\s (пробел)", "\\s"),
            ("\\S (не пробел)", "\\S"),
            ("\\n (новая строка)", "\\n"),
            ("\\t (табуляция)", "\\t"),
        ]
        
        row, col = 1, 0
        for label, pattern in basic_buttons:
            btn = QPushButton(label)
            btn.clicked.connect(lambda checked, p=pattern: self._insert_text(p))
            basic_layout.addWidget(btn, row, col)
            col += 1
            if col >= 3:
                col = 0
                row += 1
        
        builder_layout.addLayout(basic_layout)
        
        # Квантификаторы
        quant_layout = QHBoxLayout()
        quant_label = QLabel("Квантификаторы:")
        quant_layout.addWidget(quant_label)
        quant_buttons = [
            ("* (0 или больше)", "*"),
            ("+ (1 или больше)", "+"),
            ("? (0 или 1)", "?"),
            ("{n,m} (от n до m)", "{n,m}"),
        ]
        for label, pattern in quant_buttons:
            btn = QPushButton(label)
            btn.clicked.connect(lambda checked, p=pattern: self._insert_text(p))
            quant_layout.addWidget(btn)
        quant_layout.addStretch()
        builder_layout.addLayout(quant_layout)
        
        # Группы символов
        groups_layout = QHBoxLayout()
        groups_label = QLabel("Группы:")
        groups_layout.addWidget(groups_label)
        group_buttons = [
            ("[abc] (один из)", "[abc]"),
            ("[a-z] (диапазон)", "[a-z]"),
            ("[^abc] (кроме)", "[^abc]"),
            ("(группа)", "()"),
            ("| (или)", "|"),
        ]
        for label, pattern in group_buttons:
            btn = QPushButton(label)
            btn.clicked.connect(lambda checked, p=pattern: self._insert_text(p))
            groups_layout.addWidget(btn)
        groups_layout.addStretch()
        builder_layout.addLayout(groups_layout)
        
        # Специальные символы
        special_layout = QHBoxLayout()
        special_label = QLabel("Экранирование:")
        special_layout.addWidget(special_label)
        special_buttons = [
            ("\\. (точка)", "\\."),
            ("\\* (звёздочка)", "\\*"),
            ("\\+ (плюс)", "\\+"),
            ("\\? (вопрос)", "\\?"),
            ("\\| (вертикальная черта)", "\\|"),
            ("\\( (скобка)", "\\("),
            ("\\) (скобка)", "\\)"),
        ]
        for label, pattern in special_buttons:
            btn = QPushButton(label)
            btn.clicked.connect(lambda checked, p=pattern: self._insert_text(p))
            special_layout.addWidget(btn)
        special_layout.addStretch()
        builder_layout.addLayout(special_layout)
        
        # Быстрые шаблоны
        templates_layout = QHBoxLayout()
        templates_label = QLabel("Шаблоны:")
        templates_layout.addWidget(templates_label)
        template_buttons = [
            ("Имя файла", "^[\\w\\-\\.]+$"),
            ("Расширение", "\\.[a-zA-Z]+$"),
            ("Дата YYYY-MM-DD", "\\d{4}-\\d{2}-\\d{2}"),
            ("Число", "\\d+"),
            ("Email", "[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]+"),
        ]
        for label, pattern in template_buttons:
            btn = QPushButton(label)
            btn.clicked.connect(lambda checked, p=pattern: self._set_pattern(p))
            templates_layout.addWidget(btn)
        templates_layout.addStretch()
        builder_layout.addLayout(templates_layout)
        
        builder_group.setLayout(builder_layout)
        layout.addWidget(builder_group)
        
        layout.addStretch()
        
        # Кнопки
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self._validate_and_accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        
        # Обновляем предварительный просмотр
        self._update_preview()
    
    def _insert_text(self, text):
        """Вставить текст в поле результата"""
        cursor = self.result_edit.cursorPosition()
        current_text = self.result_edit.text()
        new_text = current_text[:cursor] + text + current_text[cursor:]
        self.result_edit.setText(new_text)
        self.result_edit.setCursorPosition(cursor + len(text))
        self.result_edit.setFocus()
    
    def _set_pattern(self, pattern):
        """Установить полный паттерн"""
        self.result_edit.setText(pattern)
        self.result_edit.setFocus()
    
    def _update_preview(self):
        """Обновить предварительный просмотр"""
        pattern = self.result_edit.text()
        test_text = self.test_edit.text()
        
        if not pattern:
            self.preview_label.setText("Введите регулярное выражение. Используйте кнопки ниже для вставки элементов.")
            self.preview_label.setStyleSheet("background-color: #f0f0f0; padding: 5px; border: 1px solid #ccc;")
            return
        
        try:
            regex = re.compile(pattern)
            if test_text:
                match = regex.fullmatch(test_text)
                if match:
                    groups_info = f"Группы: {match.groups()}" if match.groups() else "Группы: нет"
                    self.preview_label.setText(f"✓ Совпадение найдено!\n{groups_info}")
                    self.preview_label.setStyleSheet("background-color: #d4edda; padding: 5px; border: 1px solid #28a745;")
                else:
                    self.preview_label.setText("✗ Совпадение не найдено\nПопробуйте изменить регулярное выражение или тестовый текст.")
                    self.preview_label.setStyleSheet("background-color: #f8d7da; padding: 5px; border: 1px solid #dc3545;")
            else:
                self.preview_label.setText("✓ Регулярное выражение корректно.\nВведите текст для тестирования выше.")
                self.preview_label.setStyleSheet("background-color: #fff3cd; padding: 5px; border: 1px solid #ffc107;")
        except re.error as e:
            error_msg = str(e).replace("at position", "на позиции")
            self.preview_label.setText(f"✗ Ошибка регулярного выражения:\n{error_msg}\n\nИспользуйте кнопки ниже для построения корректного выражения.")
            self.preview_label.setStyleSheet("background-color: #f8d7da; padding: 5px; border: 1px solid #dc3545;")
    
    def _validate_and_accept(self):
        """Проверить и принять"""
        pattern = self.result_edit.text().strip()
        if not pattern:
            QMessageBox.warning(self, "Предупреждение", "Регулярное выражение не может быть пустым")
            return
        
        try:
            re.compile(pattern)
            self.accept()
        except re.error as e:
            QMessageBox.critical(self, "Ошибка", f"Некорректное регулярное выражение:\n{e}")
    
    def get_pattern(self):
        """Получить построенное регулярное выражение"""
        return self.result_edit.text().strip()

