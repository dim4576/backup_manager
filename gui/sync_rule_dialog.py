"""
Диалог для редактирования правил синхронизации с S3
"""
from PyQt5.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QFormLayout,
                             QLineEdit, QSpinBox, QCheckBox, QComboBox,
                             QListWidget, QListWidgetItem, QPushButton,
                             QLabel, QGroupBox, QMessageBox, QDialogButtonBox,
                             QAbstractItemView, QWidget, QRadioButton, QButtonGroup,
                             QTimeEdit, QFrame, QScrollArea)
from PyQt5.QtCore import Qt, QTime
from pathlib import Path
from core.config_manager import ConfigManager


class SyncRuleDialog(QDialog):
    """Диалог редактирования правила синхронизации"""
    
    def __init__(self, parent, config: ConfigManager, rule_index: int = None):
        """
        Инициализация диалога
        
        Args:
            parent: Родительский виджет
            config: Менеджер конфигурации
            rule_index: Индекс правила для редактирования (None = новое правило)
        """
        super().__init__(parent)
        self.config = config
        self.rule_index = rule_index
        self.is_new = rule_index is None
        
        self.setWindowTitle("Новое правило синхронизации" if self.is_new else "Редактирование правила синхронизации")
        self.setMinimumWidth(650)
        self.setMinimumHeight(750)
        self.resize(700, 850)
        
        self._create_ui()
        
        if not self.is_new:
            self._load_rule()
    
    def _create_ui(self):
        """Создать интерфейс"""
        main_layout = QVBoxLayout(self)
        
        # Создаем прокручиваемую область
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        
        # Контейнер для содержимого
        content_widget = QWidget()
        layout = QVBoxLayout(content_widget)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(10)
        
        # === Основные настройки ===
        main_group = QGroupBox("Основные настройки")
        main_form_layout = QFormLayout()
        
        # Название правила
        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("Например: Синхронизация бэкапов БД")
        main_form_layout.addRow("Название:", self.name_edit)
        
        # Выбор S3 бакета
        self.bucket_combo = QComboBox()
        self._populate_buckets()
        main_form_layout.addRow("S3 бакет:", self.bucket_combo)
        
        # Включено
        self.enabled_check = QCheckBox()
        self.enabled_check.setChecked(True)
        main_form_layout.addRow("Включено:", self.enabled_check)
        
        main_group.setLayout(main_form_layout)
        layout.addWidget(main_group)
        
        # === Выбор папок ===
        folders_group = QGroupBox("Папки для синхронизации")
        folders_layout = QVBoxLayout()
        
        folders_hint = QLabel("Выберите папки из списка отслеживаемых папок:")
        folders_hint.setStyleSheet("color: gray; font-size: 9pt;")
        folders_layout.addWidget(folders_hint)
        
        self.folders_list = QListWidget()
        self.folders_list.setSelectionMode(QAbstractItemView.MultiSelection)
        self._populate_folders()
        folders_layout.addWidget(self.folders_list)
        
        # Кнопки выбора
        select_btns = QHBoxLayout()
        btn_select_all = QPushButton("Выбрать все")
        btn_select_all.clicked.connect(self._select_all_folders)
        btn_deselect_all = QPushButton("Снять выделение")
        btn_deselect_all.clicked.connect(self._deselect_all_folders)
        select_btns.addWidget(btn_select_all)
        select_btns.addWidget(btn_deselect_all)
        select_btns.addStretch()
        folders_layout.addLayout(select_btns)
        
        folders_group.setLayout(folders_layout)
        layout.addWidget(folders_group)
        
        # === Расписание синхронизации ===
        schedule_group = QGroupBox("Расписание синхронизации")
        schedule_main_layout = QVBoxLayout()
        
        # Выбор типа расписания
        self.schedule_type_group = QButtonGroup()
        
        # Вариант 1: По интервалу
        self.interval_radio = QRadioButton("По интервалу")
        self.schedule_type_group.addButton(self.interval_radio, 0)
        schedule_main_layout.addWidget(self.interval_radio)
        
        # Контейнер для настроек интервала
        self.interval_container = QWidget()
        interval_layout = QVBoxLayout(self.interval_container)
        interval_layout.setContentsMargins(25, 5, 5, 5)
        interval_layout.setSpacing(5)
        
        # Режим интервала
        self.interval_mode_group = QButtonGroup()
        
        # Минуты
        minutes_layout = QHBoxLayout()
        self.minutes_radio = QRadioButton()
        self.interval_minutes_spin = QSpinBox()
        self.interval_minutes_spin.setRange(1, 10080)
        self.interval_minutes_spin.setValue(60)
        self.interval_minutes_spin.setSuffix(" минут")
        minutes_layout.addWidget(self.minutes_radio)
        minutes_layout.addWidget(QLabel("Каждые"))
        minutes_layout.addWidget(self.interval_minutes_spin)
        minutes_layout.addStretch()
        self.interval_mode_group.addButton(self.minutes_radio, 0)
        interval_layout.addLayout(minutes_layout)
        
        # Часы
        hours_layout = QHBoxLayout()
        self.hours_radio = QRadioButton()
        self.interval_hours_spin = QSpinBox()
        self.interval_hours_spin.setRange(1, 168)
        self.interval_hours_spin.setValue(1)
        self.interval_hours_spin.setSuffix(" часов")
        hours_layout.addWidget(self.hours_radio)
        hours_layout.addWidget(QLabel("Каждые"))
        hours_layout.addWidget(self.interval_hours_spin)
        hours_layout.addStretch()
        self.interval_mode_group.addButton(self.hours_radio, 1)
        interval_layout.addLayout(hours_layout)
        
        # Дни
        days_layout = QHBoxLayout()
        self.days_radio = QRadioButton()
        self.interval_days_spin = QSpinBox()
        self.interval_days_spin.setRange(1, 365)
        self.interval_days_spin.setValue(1)
        self.interval_days_spin.setSuffix(" дней")
        days_layout.addWidget(self.days_radio)
        days_layout.addWidget(QLabel("Каждые"))
        days_layout.addWidget(self.interval_days_spin)
        days_layout.addStretch()
        self.interval_mode_group.addButton(self.days_radio, 2)
        interval_layout.addLayout(days_layout)
        
        self.hours_radio.setChecked(True)
        schedule_main_layout.addWidget(self.interval_container)
        
        # Разделитель
        separator = QFrame()
        separator.setFrameShape(QFrame.HLine)
        separator.setFrameShadow(QFrame.Sunken)
        schedule_main_layout.addWidget(separator)
        
        # Вариант 2: По расписанию
        self.schedule_radio = QRadioButton("По расписанию")
        self.schedule_type_group.addButton(self.schedule_radio, 1)
        schedule_main_layout.addWidget(self.schedule_radio)
        
        # Контейнер для настроек расписания
        self.schedule_container = QWidget()
        sched_layout = QVBoxLayout(self.schedule_container)
        sched_layout.setContentsMargins(25, 5, 5, 5)
        sched_layout.setSpacing(5)
        
        # Дни недели
        days_label = QLabel("Дни недели:")
        sched_layout.addWidget(days_label)
        
        days_widget = QWidget()
        days_h_layout = QHBoxLayout(days_widget)
        days_h_layout.setContentsMargins(0, 0, 0, 0)
        days_h_layout.setSpacing(10)
        
        self.day_checks = {}
        day_names = [
            ("mon", "Пн"), ("tue", "Вт"), ("wed", "Ср"), ("thu", "Чт"),
            ("fri", "Пт"), ("sat", "Сб"), ("sun", "Вс")
        ]
        for day_key, day_name in day_names:
            check = QCheckBox(day_name)
            self.day_checks[day_key] = check
            days_h_layout.addWidget(check)
        days_h_layout.addStretch()
        sched_layout.addWidget(days_widget)
        
        # Кнопки быстрого выбора дней
        quick_days_layout = QHBoxLayout()
        btn_weekdays = QPushButton("Будни")
        btn_weekdays.setMaximumWidth(80)
        btn_weekdays.clicked.connect(self._select_weekdays)
        btn_weekend = QPushButton("Выходные")
        btn_weekend.setMaximumWidth(80)
        btn_weekend.clicked.connect(self._select_weekend)
        btn_all_days = QPushButton("Все дни")
        btn_all_days.setMaximumWidth(80)
        btn_all_days.clicked.connect(self._select_all_days)
        quick_days_layout.addWidget(btn_weekdays)
        quick_days_layout.addWidget(btn_weekend)
        quick_days_layout.addWidget(btn_all_days)
        quick_days_layout.addStretch()
        sched_layout.addLayout(quick_days_layout)
        
        # Время запуска
        time_layout = QHBoxLayout()
        time_layout.addWidget(QLabel("Время запуска:"))
        self.schedule_time = QTimeEdit()
        self.schedule_time.setDisplayFormat("HH:mm")
        self.schedule_time.setTime(QTime(3, 0))  # По умолчанию 03:00
        time_layout.addWidget(self.schedule_time)
        time_layout.addStretch()
        sched_layout.addLayout(time_layout)
        
        schedule_main_layout.addWidget(self.schedule_container)
        
        # По умолчанию выбираем интервал
        self.interval_radio.setChecked(True)
        
        # Связываем переключатели
        self.interval_radio.toggled.connect(self._on_schedule_type_changed)
        self.schedule_radio.toggled.connect(self._on_schedule_type_changed)
        self.minutes_radio.toggled.connect(self._on_interval_mode_changed)
        self.hours_radio.toggled.connect(self._on_interval_mode_changed)
        self.days_radio.toggled.connect(self._on_interval_mode_changed)
        self._on_schedule_type_changed()
        self._on_interval_mode_changed()
        
        schedule_group.setLayout(schedule_main_layout)
        layout.addWidget(schedule_group)
        
        # === Версионирование и ротация ===
        versioning_group = QGroupBox("Версионирование и ротация")
        versioning_layout = QFormLayout()
        
        # Включить версионирование
        self.versioning_check = QCheckBox("Сохранять версии папок с датой в названии")
        self.versioning_check.setChecked(False)
        self.versioning_check.toggled.connect(self._on_versioning_toggled)
        versioning_layout.addRow(self.versioning_check)
        
        # Подсказка о формате версий
        version_hint = QLabel("Формат: folder_name_2026-01-16_14-30/")
        version_hint.setStyleSheet("color: gray; font-size: 9pt; margin-left: 20px;")
        versioning_layout.addRow("", version_hint)
        
        # Максимальное количество версий
        versions_widget = QWidget()
        versions_layout = QHBoxLayout(versions_widget)
        versions_layout.setContentsMargins(0, 0, 0, 0)
        self.max_versions_spin = QSpinBox()
        self.max_versions_spin.setRange(0, 100)
        self.max_versions_spin.setValue(5)
        self.max_versions_spin.setEnabled(False)
        versions_layout.addWidget(self.max_versions_spin)
        versions_layout.addWidget(QLabel("(0 = без ограничений)"))
        versions_layout.addStretch()
        versioning_layout.addRow("Макс. версий:", versions_widget)
        
        # Максимальный возраст версий
        age_widget = QWidget()
        age_layout = QHBoxLayout(age_widget)
        age_layout.setContentsMargins(0, 0, 0, 0)
        self.max_version_age_spin = QSpinBox()
        self.max_version_age_spin.setRange(0, 365)
        self.max_version_age_spin.setValue(30)
        self.max_version_age_spin.setSuffix(" дней")
        self.max_version_age_spin.setEnabled(False)
        age_layout.addWidget(self.max_version_age_spin)
        age_layout.addWidget(QLabel("(0 = без ограничений)"))
        age_layout.addStretch()
        versioning_layout.addRow("Макс. возраст версий:", age_widget)
        
        versioning_group.setLayout(versioning_layout)
        layout.addWidget(versioning_group)
        
        # === Дополнительные настройки ===
        extra_group = QGroupBox("Дополнительные настройки")
        extra_layout = QFormLayout()
        
        # Удалять локальные файлы после загрузки
        self.delete_after_sync_check = QCheckBox("Удалять локальные файлы после успешной загрузки")
        self.delete_after_sync_check.setChecked(False)
        extra_layout.addRow(self.delete_after_sync_check)
        
        # Синхронизировать удаления
        self.sync_deletions_check = QCheckBox("Удалять в S3 файлы, удалённые локально")
        self.sync_deletions_check.setChecked(False)
        extra_layout.addRow(self.sync_deletions_check)
        
        # Паттерн фильтрации
        pattern_widget = QWidget()
        pattern_layout = QHBoxLayout(pattern_widget)
        pattern_layout.setContentsMargins(0, 0, 0, 0)
        self.pattern_edit = QLineEdit()
        self.pattern_edit.setPlaceholderText("*")
        self.pattern_edit.setText("*")
        pattern_layout.addWidget(self.pattern_edit)
        self.pattern_type_combo = QComboBox()
        self.pattern_type_combo.addItems(["wildcard", "regex"])
        pattern_layout.addWidget(self.pattern_type_combo)
        extra_layout.addRow("Фильтр файлов:", pattern_widget)
        
        extra_group.setLayout(extra_layout)
        layout.addWidget(extra_group)
        
        # Добавляем растягивающий элемент в конце
        layout.addStretch()
        
        # Устанавливаем контейнер в прокручиваемую область
        scroll_area.setWidget(content_widget)
        main_layout.addWidget(scroll_area)
        
        # === Кнопки ===
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self._save_rule)
        buttons.rejected.connect(self.reject)
        main_layout.addWidget(buttons)
    
    def _populate_buckets(self):
        """Заполнить список бакетов"""
        self.bucket_combo.clear()
        self.bucket_combo.addItem("(Выберите бакет)", None)
        
        buckets = self.config.get_s3_buckets()
        for bucket in buckets:
            bucket_name = bucket.get("name", "")
            self.bucket_combo.addItem(bucket_name, bucket_name)
    
    def _populate_folders(self):
        """Заполнить список папок"""
        self.folders_list.clear()
        folders = self.config.get_watch_folders()
        
        for folder in folders:
            item = QListWidgetItem(str(folder))
            item.setData(Qt.UserRole, str(folder))
            self.folders_list.addItem(item)
    
    def _select_all_folders(self):
        """Выбрать все папки"""
        for i in range(self.folders_list.count()):
            self.folders_list.item(i).setSelected(True)
    
    def _deselect_all_folders(self):
        """Снять выделение со всех папок"""
        for i in range(self.folders_list.count()):
            self.folders_list.item(i).setSelected(False)
    
    def _on_schedule_type_changed(self):
        """Обработчик изменения типа расписания (интервал/расписание)"""
        is_interval = self.interval_radio.isChecked()
        self.interval_container.setEnabled(is_interval)
        self.schedule_container.setEnabled(not is_interval)
        
        # Визуально затемняем неактивный контейнер
        self.interval_container.setStyleSheet("" if is_interval else "color: gray;")
        self.schedule_container.setStyleSheet("color: gray;" if is_interval else "")
    
    def _on_interval_mode_changed(self):
        """Обработчик изменения режима интервала"""
        self.interval_minutes_spin.setEnabled(self.minutes_radio.isChecked())
        self.interval_hours_spin.setEnabled(self.hours_radio.isChecked())
        self.interval_days_spin.setEnabled(self.days_radio.isChecked())
    
    def _select_weekdays(self):
        """Выбрать будние дни"""
        for day_key, check in self.day_checks.items():
            check.setChecked(day_key in ["mon", "tue", "wed", "thu", "fri"])
    
    def _select_weekend(self):
        """Выбрать выходные"""
        for day_key, check in self.day_checks.items():
            check.setChecked(day_key in ["sat", "sun"])
    
    def _select_all_days(self):
        """Выбрать все дни"""
        for check in self.day_checks.values():
            check.setChecked(True)
    
    def _on_versioning_toggled(self, checked: bool):
        """Обработчик переключения версионирования"""
        self.max_versions_spin.setEnabled(checked)
        self.max_version_age_spin.setEnabled(checked)
    
    def _get_interval_minutes(self) -> int:
        """Получить интервал в минутах"""
        if self.minutes_radio.isChecked():
            return self.interval_minutes_spin.value()
        elif self.hours_radio.isChecked():
            return self.interval_hours_spin.value() * 60
        else:  # days
            return self.interval_days_spin.value() * 24 * 60
    
    def _set_interval_from_minutes(self, minutes: int):
        """Установить интервал из минут"""
        if minutes % (24 * 60) == 0 and minutes >= 24 * 60:
            # Целые дни
            self.days_radio.setChecked(True)
            self.interval_days_spin.setValue(minutes // (24 * 60))
        elif minutes % 60 == 0 and minutes >= 60:
            # Целые часы
            self.hours_radio.setChecked(True)
            self.interval_hours_spin.setValue(minutes // 60)
        else:
            # Минуты
            self.minutes_radio.setChecked(True)
            self.interval_minutes_spin.setValue(minutes)
    
    def _load_rule(self):
        """Загрузить существующее правило"""
        rules = self.config.get_sync_rules()
        if self.rule_index is None or self.rule_index >= len(rules):
            return
        
        rule = rules[self.rule_index]
        
        # Основные настройки
        self.name_edit.setText(rule.get("name", ""))
        self.enabled_check.setChecked(rule.get("enabled", True))
        
        # Бакет
        bucket_name = rule.get("bucket_name", "")
        index = self.bucket_combo.findData(bucket_name)
        if index >= 0:
            self.bucket_combo.setCurrentIndex(index)
        
        # Папки
        selected_folders = rule.get("folders", [])
        for i in range(self.folders_list.count()):
            item = self.folders_list.item(i)
            folder_path = item.data(Qt.UserRole)
            if folder_path in selected_folders or "*" in selected_folders:
                item.setSelected(True)
        
        # Тип расписания
        schedule_type = rule.get("schedule_type", "interval")
        if schedule_type == "schedule":
            self.schedule_radio.setChecked(True)
        else:
            self.interval_radio.setChecked(True)
        
        # Интервал
        interval_minutes = rule.get("interval_minutes", 60)
        self._set_interval_from_minutes(interval_minutes)
        
        # Расписание: дни недели
        schedule_days = rule.get("schedule_days", [])
        for day_key, check in self.day_checks.items():
            check.setChecked(day_key in schedule_days)
        
        # Расписание: время
        schedule_time = rule.get("schedule_time", "03:00")
        try:
            time_parts = schedule_time.split(":")
            self.schedule_time.setTime(QTime(int(time_parts[0]), int(time_parts[1])))
        except:
            self.schedule_time.setTime(QTime(3, 0))
        
        self._on_schedule_type_changed()
        
        # Версионирование
        versioning = rule.get("versioning_enabled", False)
        self.versioning_check.setChecked(versioning)
        self.max_versions_spin.setValue(rule.get("max_versions", 5))
        self.max_version_age_spin.setValue(rule.get("max_version_age_days", 30))
        
        # Дополнительные настройки
        self.delete_after_sync_check.setChecked(rule.get("delete_after_sync", False))
        self.sync_deletions_check.setChecked(rule.get("sync_deletions", False))
        self.pattern_edit.setText(rule.get("pattern", "*"))
        
        pattern_type = rule.get("pattern_type", "wildcard")
        index = self.pattern_type_combo.findText(pattern_type)
        if index >= 0:
            self.pattern_type_combo.setCurrentIndex(index)
    
    def _save_rule(self):
        """Сохранить правило"""
        # Валидация
        name = self.name_edit.text().strip()
        if not name:
            QMessageBox.warning(self, "Ошибка", "Укажите название правила")
            return
        
        bucket_name = self.bucket_combo.currentData()
        if not bucket_name:
            QMessageBox.warning(self, "Ошибка", "Выберите S3 бакет")
            return
        
        # Получаем выбранные папки
        selected_folders = []
        for i in range(self.folders_list.count()):
            item = self.folders_list.item(i)
            if item.isSelected():
                selected_folders.append(item.data(Qt.UserRole))
        
        if not selected_folders:
            QMessageBox.warning(self, "Ошибка", "Выберите хотя бы одну папку для синхронизации")
            return
        
        # Определяем тип расписания
        schedule_type = "schedule" if self.schedule_radio.isChecked() else "interval"
        
        # Собираем выбранные дни недели
        schedule_days = [day_key for day_key, check in self.day_checks.items() if check.isChecked()]
        
        # Проверяем, что выбран хотя бы один день для расписания
        if schedule_type == "schedule" and not schedule_days:
            QMessageBox.warning(self, "Ошибка", "Выберите хотя бы один день недели для расписания")
            return
        
        # Определяем last_sync для нового правила
        # Для режима "по расписанию" устанавливаем текущее время,
        # чтобы синхронизация не запустилась сразу при создании
        if self.is_new:
            if schedule_type == "schedule":
                from datetime import datetime, timezone
                last_sync = datetime.now(timezone.utc).isoformat()
            else:
                last_sync = None
        else:
            # При редактировании сохраняем предыдущее значение
            rules = self.config.get_sync_rules()
            if self.rule_index < len(rules):
                last_sync = rules[self.rule_index].get("last_sync")
            else:
                last_sync = None
        
        # Формируем правило
        rule = {
            "name": name,
            "bucket_name": bucket_name,
            "enabled": self.enabled_check.isChecked(),
            "folders": selected_folders,
            "schedule_type": schedule_type,
            "interval_minutes": self._get_interval_minutes(),
            "schedule_days": schedule_days,
            "schedule_time": self.schedule_time.time().toString("HH:mm"),
            "versioning_enabled": self.versioning_check.isChecked(),
            "max_versions": self.max_versions_spin.value(),
            "max_version_age_days": self.max_version_age_spin.value(),
            "delete_after_sync": self.delete_after_sync_check.isChecked(),
            "sync_deletions": self.sync_deletions_check.isChecked(),
            "pattern": self.pattern_edit.text().strip() or "*",
            "pattern_type": self.pattern_type_combo.currentText(),
            "last_sync": last_sync
        }
        
        # Сохраняем
        if self.is_new:
            self.config.add_sync_rule(rule)
        else:
            self.config.update_sync_rule(self.rule_index, rule)
        
        self.accept()
