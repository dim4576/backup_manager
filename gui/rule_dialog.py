"""
Диалог для добавления/редактирования правила
"""
import re
from PyQt5.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QFormLayout,
                             QLineEdit, QComboBox, QSpinBox, QCheckBox,
                             QGroupBox, QLabel, QPushButton, QListWidget,
                             QListWidgetItem, QDialogButtonBox, QMessageBox,
                             QTabWidget, QWidget)
from PyQt5.QtCore import Qt
from pathlib import Path
from typing import Optional
from core.config_manager import ConfigManager
from gui.regex_builder import RegexBuilderDialog


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
        
        # Создаём вкладки
        tabs = QTabWidget()
        
        # Первая вкладка - Основные настройки
        main_tab = QWidget()
        main_tab_layout = QVBoxLayout(main_tab)
        
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
        
        # Максимальный возраст (годы, месяцы, дни, часы, минуты)
        age_layout = QHBoxLayout()
        
        # Годы
        self.years_spin = QSpinBox()
        self.years_spin.setRange(0, 100)
        self.years_spin.setSuffix(" г.")
        age_layout.addWidget(self.years_spin)
        
        # Месяцы
        self.months_spin = QSpinBox()
        self.months_spin.setRange(0, 11)
        self.months_spin.setSuffix(" мес.")
        age_layout.addWidget(self.months_spin)
        
        # Дни
        self.days_spin = QSpinBox()
        self.days_spin.setRange(0, 30)
        self.days_spin.setSuffix(" дн.")
        age_layout.addWidget(self.days_spin)
        
        # Часы
        self.hours_spin = QSpinBox()
        self.hours_spin.setRange(0, 23)
        self.hours_spin.setSuffix(" ч.")
        age_layout.addWidget(self.hours_spin)
        
        # Минуты
        self.minutes_spin = QSpinBox()
        self.minutes_spin.setRange(0, 59)
        self.minutes_spin.setSuffix(" мин.")
        age_layout.addWidget(self.minutes_spin)
        
        age_layout.addStretch()
        
        # Загружаем значение из правила
        total_minutes = 0
        if "max_age_days" in rule:
            total_minutes = rule.get("max_age_days", 30) * 24 * 60
        else:
            total_minutes = rule.get("max_age_minutes", 43200)  # По умолчанию 30 дней
        
        # Конвертируем минуты в годы, месяцы, дни, часы, минуты
        # 1 год = 365 дней, 1 месяц = 30 дней
        years = total_minutes // (365 * 24 * 60)
        remaining_after_years = total_minutes % (365 * 24 * 60)
        months = remaining_after_years // (30 * 24 * 60)
        remaining_after_months = remaining_after_years % (30 * 24 * 60)
        days = remaining_after_months // (24 * 60)
        remaining_minutes = remaining_after_months % (24 * 60)
        hours = remaining_minutes // 60
        minutes = remaining_minutes % 60
        
        self.years_spin.setValue(years)
        self.months_spin.setValue(months)
        self.days_spin.setValue(days)
        self.hours_spin.setValue(hours)
        self.minutes_spin.setValue(minutes)
        
        form_layout.addRow("Удалять файлы старше:", age_layout)
        
        # Подсказка
        age_hint = QLabel("(укажите годы, месяцы, дни, часы и/или минуты. Например: 1 г. 2 мес. 7 дн. = 1 год 2 месяца 7 дней)")
        age_hint.setStyleSheet("color: gray; font-size: 9pt;")
        form_layout.addRow("", age_hint)
        
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
        
        main_tab_layout.addLayout(form_layout)
        
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
        main_tab_layout.addWidget(folders_group)
        
        main_tab_layout.addStretch()
        tabs.addTab(main_tab, "Основные настройки")
        
        main_layout.addWidget(tabs)
        
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
        
        # Конвертируем годы, месяцы, дни, часы, минуты в общее количество минут
        # 1 год = 365 дней, 1 месяц = 30 дней
        total_minutes = (self.years_spin.value() * 365 * 24 * 60 +
                        self.months_spin.value() * 30 * 24 * 60 +
                        self.days_spin.value() * 24 * 60 + 
                        self.hours_spin.value() * 60 + 
                        self.minutes_spin.value())
        
        # Проверяем, что хотя бы одно значение задано
        if total_minutes == 0:
            QMessageBox.warning(self, "Предупреждение", "Укажите возраст файлов для удаления (годы, месяцы, дни, часы или минуты)")
            return
        
        rule = {
            "name": self.name_edit.text().strip(),
            "pattern": self.pattern_edit.text().strip(),
            "pattern_type": pattern_type,
            "max_age_minutes": total_minutes,
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

