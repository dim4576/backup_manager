"""
Графический интерфейс для настройки приложения - PyQt5 версия
"""
import re
from PyQt5.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QTabWidget, 
                             QTreeWidget, QTreeWidgetItem, QPushButton, 
                             QLabel, QSpinBox, QCheckBox, QFileDialog, 
                             QMessageBox, QDialogButtonBox, QFormLayout,
                             QLineEdit, QGroupBox, QWidget, QScrollArea,
                             QListWidget, QListWidgetItem, QComboBox, QTextEdit, QGridLayout,
                             QMenu)
from PyQt5.QtCore import Qt, pyqtSignal, QPoint
from PyQt5.QtGui import QKeyEvent
from pathlib import Path
from typing import Optional
from config_manager import ConfigManager
from backup_manager import BackupManager
from logger import get_log_file_path


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


class SettingsWindow(QDialog):
    """Окно настроек приложения"""
    
    def __init__(self, parent, config: ConfigManager, backup_manager: BackupManager):
        """Инициализация окна настроек"""
        super().__init__(parent)
        self.config = config
        self.backup_manager = backup_manager
        self.setWindowTitle("Настройки Backup Manager")
        self.setMinimumSize(800, 600)
        
        self._create_ui()
    
    def _create_ui(self):
        """Создать интерфейс"""
        layout = QVBoxLayout(self)
        
        # Создаём вкладки
        tabs = QTabWidget()
        
        # Вкладка папок
        folders_tab = self._create_folders_tab()
        tabs.addTab(folders_tab, "Папки для мониторинга")
        
        # Вкладка правил
        rules_tab = self._create_rules_tab()
        tabs.addTab(rules_tab, "Правила удаления")
        
        # Вкладка общих настроек
        general_tab = self._create_general_tab()
        tabs.addTab(general_tab, "Общие настройки")
        
        layout.addWidget(tabs)
        
        # Кнопки
        buttons = QDialogButtonBox(QDialogButtonBox.Close)
        buttons.rejected.connect(self.accept)
        layout.addWidget(buttons)
    
    def _create_folders_tab(self):
        """Создать вкладку с папками"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        label = QLabel("Отслеживаемые папки:")
        layout.addWidget(label)
        
        # Список папок
        self.folders_tree = FoldersTreeWidget()
        self.folders_tree.setHeaderLabel("Папка")
        self.folders_tree.setRootIsDecorated(False)
        self.folders_tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.folders_tree.customContextMenuRequested.connect(self._folders_context_menu)
        self.folders_tree.set_delete_callback(self._remove_folder)
        layout.addWidget(self.folders_tree)
        
        # Кнопки управления
        btn_layout = QHBoxLayout()
        
        btn_add = QPushButton("Добавить папку")
        btn_add.clicked.connect(self._add_folder)
        btn_layout.addWidget(btn_add)
        
        btn_remove = QPushButton("Удалить папку")
        btn_remove.clicked.connect(self._remove_folder)
        btn_layout.addWidget(btn_remove)
        
        btn_refresh = QPushButton("Обновить список")
        btn_refresh.clicked.connect(self._refresh_folders)
        btn_layout.addWidget(btn_refresh)
        
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
        
        self._refresh_folders()
        return widget
    
    def _create_rules_tab(self):
        """Создать вкладку с правилами"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        label = QLabel("Правила удаления:")
        layout.addWidget(label)
        
        # Список правил
        self.rules_tree = RulesTreeWidget()
        self.rules_tree.setHeaderLabels(["Название", "Паттерн", "Дней", "Оставить", "Папки", "Вкл"])
        self.rules_tree.setRootIsDecorated(False)
        self.rules_tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.rules_tree.customContextMenuRequested.connect(self._rules_context_menu)
        self.rules_tree.itemDoubleClicked.connect(self._edit_rule)
        self.rules_tree.set_delete_callback(self._remove_rule)
        layout.addWidget(self.rules_tree)
        
        # Кнопки управления
        btn_layout = QHBoxLayout()
        
        btn_add = QPushButton("Добавить правило")
        btn_add.clicked.connect(self._add_rule)
        btn_layout.addWidget(btn_add)
        
        btn_edit = QPushButton("Редактировать")
        btn_edit.clicked.connect(self._edit_rule)
        btn_layout.addWidget(btn_edit)
        
        btn_remove = QPushButton("Удалить правило")
        btn_remove.clicked.connect(self._remove_rule)
        btn_layout.addWidget(btn_remove)
        
        btn_refresh = QPushButton("Обновить список")
        btn_refresh.clicked.connect(self._refresh_rules)
        btn_layout.addWidget(btn_refresh)
        
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
        
        self._refresh_rules()
        return widget
    
    def _create_general_tab(self):
        """Создать вкладку с общими настройками"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        group = QGroupBox("Настройки мониторинга")
        form_layout = QFormLayout()
        
        # Интервал проверки
        self.interval_spin = QSpinBox()
        self.interval_spin.setRange(1, 24)
        self.interval_spin.setSuffix(" часов")
        self.interval_spin.setValue(self.config.config.get("check_interval_seconds", 3600) // 3600)
        form_layout.addRow("Интервал проверки:", self.interval_spin)
        
        # Автозапуск
        self.auto_start_check = QCheckBox()
        self.auto_start_check.setChecked(self.config.config.get("auto_start", False))
        form_layout.addRow("Запускать автоматически при старте Windows:", self.auto_start_check)
        
        group.setLayout(form_layout)
        layout.addWidget(group)
        
        layout.addStretch()
        
        # Кнопка сохранения
        btn_save = QPushButton("Сохранить настройки")
        btn_save.clicked.connect(self._save_general_settings)
        layout.addWidget(btn_save)
        
        # Информация о логах
        logs_label = QLabel(f"Логи сохраняются в:\n{get_log_file_path()}")
        logs_label.setStyleSheet("color: gray; font-size: 8pt;")
        logs_label.setWordWrap(True)
        layout.addWidget(logs_label)
        
        return widget
    
    def _add_folder(self):
        """Добавить папку для мониторинга"""
        folder = QFileDialog.getExistingDirectory(self, "Выберите папку для мониторинга")
        if folder:
            try:
                self.config.add_watch_folder(Path(folder))
                self._refresh_folders()
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Не удалось добавить папку: {e}")
    
    def _remove_folder(self):
        """Удалить выбранную папку"""
        item = self.folders_tree.currentItem()
        if not item:
            QMessageBox.warning(self, "Предупреждение", "Выберите папку для удаления")
            return
        
        folder_path = Path(item.text(0))
        try:
            self.config.remove_watch_folder(folder_path)
            self._refresh_folders()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось удалить папку: {e}")
    
    def _refresh_folders(self):
        """Обновить список папок"""
        self.folders_tree.clear()
        for folder in self.config.get_watch_folders():
            folder_path = Path(folder)
            status = "✓" if folder_path.exists() else "✗"
            item = QTreeWidgetItem([str(folder_path), status])
            self.folders_tree.addTopLevelItem(item)
    
    def _folders_context_menu(self, position: QPoint):
        """Контекстное меню для папок"""
        item = self.folders_tree.itemAt(position)
        if item is None:
            return
        
        menu = QMenu(self)
        action_refresh = menu.addAction("Обновить список")
        action_refresh.triggered.connect(self._refresh_folders)
        menu.addSeparator()
        action_remove = menu.addAction("Удалить папку")
        action_remove.triggered.connect(self._remove_folder)
        
        menu.exec_(self.folders_tree.viewport().mapToGlobal(position))
    
    def _add_rule(self):
        """Добавить новое правило"""
        dialog = RuleDialog(self, self.config, None)
        if dialog.exec_() == QDialog.Accepted:
            self._refresh_rules()
    
    def _edit_rule(self):
        """Редактировать выбранное правило"""
        item = self.rules_tree.currentItem()
        if not item:
            QMessageBox.warning(self, "Предупреждение", "Выберите правило для редактирования")
            return
        
        rule_index = self.rules_tree.indexOfTopLevelItem(item)
        rules = self.config.get_rules()
        
        if 0 <= rule_index < len(rules):
            dialog = RuleDialog(self, self.config, rule_index)
            if dialog.exec_() == QDialog.Accepted:
                self._refresh_rules()
    
    def _remove_rule(self):
        """Удалить выбранное правило"""
        item = self.rules_tree.currentItem()
        if not item:
            QMessageBox.warning(self, "Предупреждение", "Выберите правило для удаления")
            return
        
        rule_index = self.rules_tree.indexOfTopLevelItem(item)
        try:
            self.config.remove_rule(rule_index)
            self._refresh_rules()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось удалить правило: {e}")
    
    def _refresh_rules(self):
        """Обновить список правил"""
        self.rules_tree.clear()
        for rule in self.config.get_rules():
            folders = rule.get("folders", [])
            if not folders:
                folders_str = "Не выбрано"
            elif "*" in folders:
                folders_str = "Все папки"
            elif len(folders) == 1:
                folders_str = Path(folders[0]).name
            else:
                folders_str = f"{len(folders)} папок"
            
            keep_latest = rule.get("keep_latest", 0)
            keep_str = "Все" if keep_latest == 0 else str(keep_latest)
            
            item = QTreeWidgetItem([
                rule.get("name", "Без названия"),
                rule.get("pattern", "*"),
                str(rule.get("max_age_days", 30)),
                keep_str,
                folders_str,
                "Да" if rule.get("enabled", True) else "Нет"
            ])
            self.rules_tree.addTopLevelItem(item)
    
    def _rules_context_menu(self, position: QPoint):
        """Контекстное меню для правил"""
        item = self.rules_tree.itemAt(position)
        
        menu = QMenu(self)
        
        if item is not None:
            action_add = menu.addAction("Добавить правило")
            action_add.triggered.connect(self._add_rule)
            menu.addSeparator()
            action_edit = menu.addAction("Редактировать")
            action_edit.triggered.connect(self._edit_rule)
            action_remove = menu.addAction("Удалить правило")
            action_remove.triggered.connect(self._remove_rule)
            menu.addSeparator()
        else:
            action_add = menu.addAction("Добавить правило")
            action_add.triggered.connect(self._add_rule)
            menu.addSeparator()
        
        action_refresh = menu.addAction("Обновить список")
        action_refresh.triggered.connect(self._refresh_rules)
        
        menu.exec_(self.rules_tree.viewport().mapToGlobal(position))
    
    
    def _save_general_settings(self):
        """Сохранить общие настройки"""
        hours = self.interval_spin.value()
        self.config.config["check_interval_seconds"] = hours * 3600
        self.config.config["auto_start"] = self.auto_start_check.isChecked()
        
        try:
            self.config.save_config()
            message = "Настройки сохранены"
            if self.config.config["auto_start"]:
                message += "\nАвтозапуск включен. Приложение будет запускаться при старте Windows."
            else:
                message += "\nАвтозапуск отключен."
            QMessageBox.information(self, "Успех", message)
        except Exception as e:
            QMessageBox.warning(self, "Предупреждение", 
                              f"Настройки сохранены, но произошла ошибка при установке автозапуска:\n{e}")


class RuleDialog(QDialog):
    """Диалог для добавления/редактирования правила"""
    
    def __init__(self, parent, config: ConfigManager, rule_index: Optional[int] = None):
        """Инициализация диалога"""
        super().__init__(parent)
        self.config = config
        self.rule_index = rule_index
        
        title = "Добавить правило" if rule_index is None else "Редактировать правило"
        self.setWindowTitle(title)
        self.setMinimumWidth(500)
        self.setMinimumHeight(500)
        
        # Загружаем правило если редактируем
        rule = {}
        if rule_index is not None:
            rules = self.config.get_rules()
            if 0 <= rule_index < len(rules):
                rule = rules[rule_index]
        
        main_layout = QVBoxLayout(self)
        
        # Форма с основными полями
        form_layout = QFormLayout()
        
        # Название
        self.name_edit = QLineEdit(rule.get("name", ""))
        form_layout.addRow("Название:", self.name_edit)
        
        # Тип паттерна
        pattern_type = rule.get("pattern_type", "wildcard")
        self.pattern_type_combo = QComboBox()
        self.pattern_type_combo.addItems(["Шаблон (wildcard)", "Регулярное выражение (regex)"])
        self.pattern_type_combo.setCurrentIndex(1 if pattern_type == "regex" else 0)
        self.pattern_type_combo.currentIndexChanged.connect(self._on_pattern_type_changed)
        form_layout.addRow("Тип паттерна:", self.pattern_type_combo)
        
        # Паттерн
        pattern_layout = QHBoxLayout()
        self.pattern_edit = QLineEdit(rule.get("pattern", "*"))
        pattern_layout.addWidget(self.pattern_edit)
        
        # Кнопка конструктора regex (только для regex типа)
        self.regex_builder_btn = QPushButton("Конструктор...")
        self.regex_builder_btn.setEnabled(pattern_type == "regex")
        self.regex_builder_btn.clicked.connect(self._open_regex_builder)
        pattern_layout.addWidget(self.regex_builder_btn)
        
        form_layout.addRow("Паттерн:", pattern_layout)
        self.pattern_hint = QLabel("(например: *.bak, backup_*, ?_*.sql)")
        self.pattern_hint.setStyleSheet("color: gray; font-size: 9pt;")
        form_layout.addRow("", self.pattern_hint)
        
        # Обновляем подсказку при загрузке
        self._on_pattern_type_changed()
        
        # Максимальный возраст
        self.days_spin = QSpinBox()
        self.days_spin.setRange(1, 365)
        self.days_spin.setSuffix(" дней")
        self.days_spin.setValue(rule.get("max_age_days", 30))
        form_layout.addRow("Удалять файлы старше:", self.days_spin)
        
        # Количество самых свежих объектов для сохранения
        self.keep_latest_spin = QSpinBox()
        self.keep_latest_spin.setRange(0, 1000)
        self.keep_latest_spin.setValue(rule.get("keep_latest", 0))
        form_layout.addRow("Оставить самых свежих:", self.keep_latest_spin)
        keep_hint = QLabel("(0 = удалить все подходящие объекты)")
        keep_hint.setStyleSheet("color: gray; font-size: 9pt;")
        form_layout.addRow("", keep_hint)
        
        # Тип удаления
        self.permanent_delete_check = QCheckBox("Удалять навсегда (без корзины)")
        permanent_delete = rule.get("permanent_delete", False)
        self.permanent_delete_check.setChecked(permanent_delete)
        form_layout.addRow("Тип удаления:", self.permanent_delete_check)
        delete_hint = QLabel("(если не отмечено, файлы будут удалены в корзину)")
        delete_hint.setStyleSheet("color: gray; font-size: 9pt;")
        form_layout.addRow("", delete_hint)
        
        # Включено
        self.enabled_check = QCheckBox()
        self.enabled_check.setChecked(rule.get("enabled", True))
        form_layout.addRow("Правило активно:", self.enabled_check)
        
        main_layout.addLayout(form_layout)
        
        # Группа выбора папок
        folders_group = QGroupBox("Применять к папкам:")
        folders_layout = QVBoxLayout()
        
        # Чекбокс "Все папки"
        self.all_folders_check = QCheckBox("Все отслеживаемые папки")
        rule_folders = rule.get("folders", [])
        # "*" означает "все папки"
        self.all_folders_check.setChecked("*" in rule_folders)
        self.all_folders_check.toggled.connect(self._on_all_folders_toggled)
        folders_layout.addWidget(self.all_folders_check)
        
        # Список папок с чекбоксами
        folders_label = QLabel("Выбрать конкретные папки:")
        folders_layout.addWidget(folders_label)
        
        # Список с прокруткой
        self.folders_list = QListWidget()
        # Отключаем список если выбраны "все папки"
        self.folders_list.setEnabled(not self.all_folders_check.isChecked())
        
        # Заполняем список папок
        watch_folders = config.get_watch_folders()
        # Исключаем "*" из списка для проверки
        rule_folders_filtered = [f for f in rule_folders if f != "*"]
        rule_folders_set = set(str(Path(f).absolute()) for f in rule_folders_filtered)
        
        for folder in watch_folders:
            folder_str = str(folder.absolute())
            item = QListWidgetItem(folder_str)
            item.setCheckState(Qt.Checked if folder_str in rule_folders_set else Qt.Unchecked)
            self.folders_list.addItem(item)
        
        if not watch_folders:
            no_folders_label = QLabel("(Нет отслеживаемых папок. Добавьте папки на вкладке 'Папки для мониторинга')")
            no_folders_label.setStyleSheet("color: gray; font-style: italic;")
            folders_layout.addWidget(no_folders_label)
        
        folders_layout.addWidget(self.folders_list)
        folders_group.setLayout(folders_layout)
        main_layout.addWidget(folders_group)
        
        main_layout.addStretch()
        
        # Кнопки
        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self._save)
        buttons.rejected.connect(self.reject)
        main_layout.addWidget(buttons)
    
    def _on_pattern_type_changed(self):
        """Обработчик изменения типа паттерна"""
        is_regex = self.pattern_type_combo.currentIndex() == 1
        if is_regex:
            self.pattern_hint.setText("(регулярное выражение, например: ^backup_.*\\.bak$, .*_[0-9]{4}\\.sql$)")
            self.regex_builder_btn.setEnabled(True)
        else:
            self.pattern_hint.setText("(шаблон, например: *.bak, backup_*, ?_*.sql)")
            self.regex_builder_btn.setEnabled(False)
    
    def _open_regex_builder(self):
        """Открыть визуальный конструктор регулярных выражений"""
        current_pattern = self.pattern_edit.text()
        
        # Проверяем, является ли текущий паттерн валидным regex
        # Если нет, предлагаем начать с чистого поля
        try:
            if current_pattern:
                re.compile(current_pattern)
        except re.error:
            # Если паттерн невалиден для regex, очищаем его
            # (вероятно, это был wildcard паттерн)
            reply = QMessageBox.question(
                self,
                "Внимание",
                f"Текущий паттерн '{current_pattern}' не является валидным регулярным выражением.\n"
                "Начать с пустого поля?",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.Yes
            )
            if reply == QMessageBox.Yes:
                current_pattern = ""
        
        dialog = RegexBuilderDialog(self, current_pattern)
        if dialog.exec_() == QDialog.Accepted:
            self.pattern_edit.setText(dialog.get_pattern())
    
    def _on_all_folders_toggled(self, checked):
        """Обработчик переключения чекбокса 'Все папки'"""
        self.folders_list.setEnabled(not checked)
        if checked:
            # Снимаем все галочки при включении "Все папки"
            for i in range(self.folders_list.count()):
                self.folders_list.item(i).setCheckState(Qt.Unchecked)
    
    def _save(self):
        """Сохранить правило"""
        if not self.name_edit.text().strip():
            QMessageBox.warning(self, "Предупреждение", "Введите название правила")
            return
        
        # Получаем выбранные папки
        folders = []
        if self.all_folders_check.isChecked():
            # Если выбрано "Все папки", сохраняем специальное значение
            folders = ["*"]
        else:
            # Иначе собираем выбранные папки
            for i in range(self.folders_list.count()):
                item = self.folders_list.item(i)
                if item.checkState() == Qt.Checked:
                    folders.append(item.text())
        
        # Определяем тип паттерна
        pattern_type = "regex" if self.pattern_type_combo.currentIndex() == 1 else "wildcard"
        
        # Проверяем корректность регулярного выражения, если выбран regex
        if pattern_type == "regex":
            try:
                re.compile(self.pattern_edit.text().strip())
            except re.error as e:
                QMessageBox.warning(self, "Ошибка", f"Некорректное регулярное выражение: {e}")
                return
        
        rule = {
            "name": self.name_edit.text().strip(),
            "pattern": self.pattern_edit.text().strip(),
            "pattern_type": pattern_type,
            "max_age_days": self.days_spin.value(),
            "enabled": self.enabled_check.isChecked(),
            "folders": folders,
            "keep_latest": self.keep_latest_spin.value(),
            "permanent_delete": self.permanent_delete_check.isChecked()
        }
        
        if self.rule_index is None:
            self.config.add_rule(rule)
        else:
            self.config.update_rule(self.rule_index, rule)
        
        self.accept()


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
