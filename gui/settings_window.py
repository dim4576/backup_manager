"""
Окно настроек приложения
"""
from PyQt5.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QTabWidget,
                             QPushButton, QLabel, QSpinBox, QCheckBox,
                             QFileDialog, QMessageBox, QDialogButtonBox,
                             QFormLayout, QGroupBox, QWidget, QMenu,
                             QTreeWidgetItem, QTimeEdit, QRadioButton,
                             QButtonGroup, QListWidget, QListWidgetItem,
                             QSplitter, QTableWidget, QTableWidgetItem,
                             QHeaderView, QProgressBar)
from PyQt5.QtCore import Qt, QPoint, QTime, QThread, pyqtSignal, QTimer
from pathlib import Path
from core.config_manager import ConfigManager
from core.backup_manager import BackupManager
from core.logger import get_log_file_path
from core.s3_manager import check_bucket_availability
from gui.widgets import FoldersTreeWidget, RulesTreeWidget
from gui.rule_dialog import RuleDialog
from gui.s3_bucket_dialog import S3BucketDialog
from gui.sync_rule_dialog import SyncRuleDialog


class S3TestWorker(QThread):
    """Рабочий поток для проверки доступности S3 бакета"""
    finished = pyqtSignal(str, str)  # result, details
    
    def __init__(self, bucket):
        super().__init__()
        self.bucket = bucket
    
    def run(self):
        """Выполнить проверку доступности в отдельном потоке"""
        bucket_name = self.bucket.get("name")
        # Проверяем, что bucket_name не None и является строкой
        if not bucket_name or not isinstance(bucket_name, str) or not bucket_name.strip():
            result = "Ошибка конфигурации"
            details = "Имя бакета не указано или имеет неверный формат."
            self.finished.emit(result, details)
            return
        
        bucket_name = bucket_name.strip()
        
        try:
            # Получаем параметры подключения
            access_key = self.bucket.get("access_key")
            secret_key = self.bucket.get("secret_key")
            region = self.bucket.get("region")
            endpoint = self.bucket.get("endpoint")
            
            # Endpoint передаём как есть - check_bucket_availability сам его нормализует
            if endpoint and isinstance(endpoint, str):
                endpoint = endpoint.strip() if endpoint.strip() else None
            else:
                endpoint = None
            
            # Устанавливаем регион по умолчанию если не указан
            if region and isinstance(region, str) and region.strip():
                region_name = region.strip()
            else:
                region_name = 'us-east-1'
            
            # Используем функцию из модуля s3_manager
            success, result, details = check_bucket_availability(
                bucket_name,
                access_key,
                secret_key,
                region_name,
                endpoint,
                timeout=15
            )
            
            # Отправляем результат через сигнал
            self.finished.emit(result, details)
            
        except ImportError:
            result = "Ошибка"
            details = "Библиотека boto3 не установлена. Установите её командой:\npip install boto3"
            self.finished.emit(result, details)
        except Exception as e:
            result = "Ошибка"
            error_msg = str(e)
            error_type = type(e).__name__
            import traceback
            tb_str = traceback.format_exc()
            details = f"Произошла непредвиденная ошибка:\n\nТип ошибки: {error_type}\nСообщение: {error_msg}\n\nДетали:\n{tb_str}"
            self.finished.emit(result, details)


class SettingsWindow(QDialog):
    """Окно настроек приложения"""
    
    def __init__(self, parent, config: ConfigManager, backup_manager: BackupManager, sync_manager=None):
        """Инициализация окна настроек"""
        super().__init__(parent)
        self.config = config
        self.backup_manager = backup_manager
        self.sync_manager = sync_manager
        self.setWindowTitle("Настройки Backup Manager")
        self.setMinimumSize(800, 600)
        
        # Таймер для автоматического обновления списка задач
        self.tasks_timer = QTimer(self)
        self.tasks_timer.timeout.connect(self._refresh_tasks)
        self.tasks_timer.setInterval(1000)  # Обновление каждую секунду
        
        self._create_ui()
    
    def _create_ui(self):
        """Создать интерфейс"""
        layout = QVBoxLayout(self)
        
        # Создаём вкладки
        tabs = QTabWidget()
        
        # Вкладка папок
        folders_tab = self._create_folders_tab()
        tabs.addTab(folders_tab, "Папки для мониторинга")
        
        # Вкладка правил удаления
        rules_tab = self._create_rules_tab()
        tabs.addTab(rules_tab, "Правила удаления")
        
        # Вкладка синхронизации с S3
        sync_tab = self._create_sync_tab()
        tabs.addTab(sync_tab, "Синхронизация S3")
        
        # Вкладка общих настроек
        general_tab = self._create_general_tab()
        tabs.addTab(general_tab, "Общие настройки")
        
        # Вкладка настройки S3
        s3_tab = self._create_s3_tab()
        tabs.addTab(s3_tab, "Настройка S3")
        
        # Вкладка задач
        tasks_tab = self._create_tasks_tab()
        tabs.addTab(tasks_tab, "Задачи")
        
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
        self.rules_tree.setHeaderLabels(["Название", "Паттерн", "Возраст", "Оставить", "Папки", "Вкл"])
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
    
    def _create_sync_tab(self):
        """Создать вкладку с правилами синхронизации"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # Описание
        desc_label = QLabel(
            "Правила синхронизации позволяют автоматически загружать файлы из выбранных папок в S3.\n"
            "Каждое правило работает независимо от правил удаления."
        )
        desc_label.setStyleSheet("color: gray; font-size: 9pt; margin-bottom: 10px;")
        desc_label.setWordWrap(True)
        layout.addWidget(desc_label)
        
        label = QLabel("Правила синхронизации:")
        layout.addWidget(label)
        
        # Таблица правил синхронизации
        self.sync_rules_table = QTableWidget()
        self.sync_rules_table.setColumnCount(6)
        self.sync_rules_table.setHorizontalHeaderLabels([
            "Название", "Бакет", "Папки", "Интервал", "Версионирование", "Вкл"
        ])
        self.sync_rules_table.horizontalHeader().setStretchLastSection(True)
        self.sync_rules_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.sync_rules_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.sync_rules_table.doubleClicked.connect(self._edit_sync_rule)
        layout.addWidget(self.sync_rules_table)
        
        # Кнопки управления
        btn_layout = QHBoxLayout()
        
        btn_add = QPushButton("Добавить правило")
        btn_add.clicked.connect(self._add_sync_rule)
        btn_layout.addWidget(btn_add)
        
        btn_edit = QPushButton("Редактировать")
        btn_edit.clicked.connect(self._edit_sync_rule)
        btn_layout.addWidget(btn_edit)
        
        btn_remove = QPushButton("Удалить правило")
        btn_remove.clicked.connect(self._remove_sync_rule)
        btn_layout.addWidget(btn_remove)
        
        btn_run_now = QPushButton("Запустить сейчас")
        btn_run_now.clicked.connect(self._run_sync_now)
        btn_layout.addWidget(btn_run_now)
        
        btn_refresh = QPushButton("Обновить список")
        btn_refresh.clicked.connect(self._refresh_sync_rules)
        btn_layout.addWidget(btn_refresh)
        
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
        
        self._refresh_sync_rules()
        return widget
    
    def _refresh_sync_rules(self):
        """Обновить список правил синхронизации"""
        self.sync_rules_table.setRowCount(0)
        rules = self.config.get_sync_rules()
        
        for rule in rules:
            row = self.sync_rules_table.rowCount()
            self.sync_rules_table.insertRow(row)
            
            # Название
            name_item = QTableWidgetItem(rule.get("name", "Без названия"))
            self.sync_rules_table.setItem(row, 0, name_item)
            
            # Бакет
            bucket_item = QTableWidgetItem(rule.get("bucket_name", ""))
            self.sync_rules_table.setItem(row, 1, bucket_item)
            
            # Папки
            folders = rule.get("folders", [])
            if not folders:
                folders_str = "Не выбрано"
            elif len(folders) == 1:
                folders_str = Path(folders[0]).name
            else:
                folders_str = f"{len(folders)} папок"
            folders_item = QTableWidgetItem(folders_str)
            self.sync_rules_table.setItem(row, 2, folders_item)
            
            # Интервал
            interval_minutes = rule.get("interval_minutes", 60)
            if interval_minutes >= 24 * 60:
                interval_str = f"{interval_minutes // (24 * 60)} дн."
            elif interval_minutes >= 60:
                interval_str = f"{interval_minutes // 60} ч."
            else:
                interval_str = f"{interval_minutes} мин."
            interval_item = QTableWidgetItem(interval_str)
            self.sync_rules_table.setItem(row, 3, interval_item)
            
            # Версионирование
            versioning = "Да" if rule.get("versioning_enabled") else "Нет"
            versioning_item = QTableWidgetItem(versioning)
            self.sync_rules_table.setItem(row, 4, versioning_item)
            
            # Включено
            enabled = "Да" if rule.get("enabled", True) else "Нет"
            enabled_item = QTableWidgetItem(enabled)
            self.sync_rules_table.setItem(row, 5, enabled_item)
        
        self.sync_rules_table.resizeColumnsToContents()
    
    def _add_sync_rule(self):
        """Добавить новое правило синхронизации"""
        # Проверяем, есть ли бакеты
        if not self.config.get_s3_buckets():
            QMessageBox.warning(
                self, "Предупреждение",
                "Сначала добавьте хотя бы один S3 бакет в разделе 'Настройка S3'"
            )
            return
        
        # Проверяем, есть ли папки
        if not self.config.get_watch_folders():
            QMessageBox.warning(
                self, "Предупреждение",
                "Сначала добавьте хотя бы одну папку для мониторинга в разделе 'Папки для мониторинга'"
            )
            return
        
        dialog = SyncRuleDialog(self, self.config, None)
        if dialog.exec_() == QDialog.Accepted:
            self._refresh_sync_rules()
    
    def _edit_sync_rule(self):
        """Редактировать выбранное правило синхронизации"""
        current_row = self.sync_rules_table.currentRow()
        if current_row < 0:
            QMessageBox.warning(self, "Предупреждение", "Выберите правило для редактирования")
            return
        
        dialog = SyncRuleDialog(self, self.config, current_row)
        if dialog.exec_() == QDialog.Accepted:
            self._refresh_sync_rules()
    
    def _remove_sync_rule(self):
        """Удалить выбранное правило синхронизации"""
        current_row = self.sync_rules_table.currentRow()
        if current_row < 0:
            QMessageBox.warning(self, "Предупреждение", "Выберите правило для удаления")
            return
        
        rule_name = self.sync_rules_table.item(current_row, 0).text()
        reply = QMessageBox.question(
            self, "Подтверждение",
            f"Удалить правило синхронизации '{rule_name}'?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            try:
                self.config.remove_sync_rule(current_row)
                self._refresh_sync_rules()
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Не удалось удалить правило: {e}")
    
    def _run_sync_now(self):
        """Запустить синхронизацию немедленно"""
        current_row = self.sync_rules_table.currentRow()
        if current_row < 0:
            QMessageBox.warning(self, "Предупреждение", "Выберите правило для запуска")
            return
        
        if self.sync_manager is None:
            QMessageBox.warning(self, "Ошибка", "Менеджер синхронизации не инициализирован")
            return
        
        rule_name = self.sync_rules_table.item(current_row, 0).text()
        
        if self.sync_manager.run_sync_now(current_row):
            QMessageBox.information(
                self, "Синхронизация запущена",
                f"Синхронизация для правила '{rule_name}' запущена.\n"
                "Прогресс можно отслеживать во вкладке 'Задачи'."
            )
        else:
            QMessageBox.warning(self, "Ошибка", "Не удалось запустить синхронизацию")
    
    def _create_general_tab(self):
        """Создать вкладку с общими настройками"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        group = QGroupBox("Настройки мониторинга")
        form_layout = QFormLayout()
        
        # Режим работы: интервал или расписание
        mode_layout = QHBoxLayout()
        mode_label = QLabel("Режим работы:")
        mode_layout.addWidget(mode_label)
        
        self.mode_button_group = QButtonGroup()
        self.interval_radio = QRadioButton("По интервалу")
        self.schedule_radio = QRadioButton("По расписанию")
        self.mode_button_group.addButton(self.interval_radio, 0)
        self.mode_button_group.addButton(self.schedule_radio, 1)
        
        # Поддержка старого формата для обратной совместимости
        old_schedule = self.config.config.get("schedule", {})
        if old_schedule and "enabled" in old_schedule:
            # Миграция старого формата
            if "schedules" not in self.config.config or not self.config.config.get("schedules"):
                self.config.config["schedule_enabled"] = old_schedule.get("enabled", False)
                if old_schedule.get("enabled", False):
                    self.config.config["schedules"] = [{
                        "days": old_schedule.get("days", [0, 1, 2, 3, 4, 5, 6]),
                        "time": old_schedule.get("time", "00:00")
                    }]
                else:
                    self.config.config["schedules"] = [{
                        "days": [0, 1, 2, 3, 4, 5, 6],
                        "time": "00:00"
                    }]
        
        schedule_enabled = self.config.config.get("schedule_enabled", False)
        
        if schedule_enabled:
            self.schedule_radio.setChecked(True)
        else:
            self.interval_radio.setChecked(True)
        
        self.interval_radio.toggled.connect(self._on_mode_changed)
        self.schedule_radio.toggled.connect(self._on_mode_changed)
        
        mode_layout.addWidget(self.interval_radio)
        mode_layout.addWidget(self.schedule_radio)
        mode_layout.addStretch()
        form_layout.addRow("", mode_layout)
        
        # Интервал проверки
        self.interval_spin = QSpinBox()
        self.interval_spin.setRange(1, 10080)  # От 1 минуты до 7 дней (10080 минут)
        self.interval_spin.setSuffix(" минут")
        # Поддержка старого формата для обратной совместимости
        if "check_interval_seconds" in self.config.config:
            self.interval_spin.setValue(self.config.config.get("check_interval_seconds", 3600) // 60)
        else:
            self.interval_spin.setValue(self.config.config.get("check_interval_minutes", 60))
        
        # Создаем виджет-обертку для метки и подсказки
        interval_label_widget = QWidget()
        interval_label_layout = QVBoxLayout(interval_label_widget)
        interval_label_layout.setContentsMargins(0, 0, 0, 0)
        interval_label = QLabel("Интервал проверки:")
        interval_hint = QLabel("(в режиме расписания - частота проверки расписания)")
        interval_hint.setStyleSheet("color: gray; font-size: 8pt;")
        interval_label_layout.addWidget(interval_label)
        interval_label_layout.addWidget(interval_hint)
        
        form_layout.addRow(interval_label_widget, self.interval_spin)
        
        # Автозапуск
        self.auto_start_check = QCheckBox()
        self.auto_start_check.setChecked(self.config.config.get("auto_start", False))
        form_layout.addRow("Запускать автоматически при старте Windows:", self.auto_start_check)
        
        group.setLayout(form_layout)
        layout.addWidget(group)
        
        # Группа настроек расписания
        schedule_group = QGroupBox("Расписание сканирования")
        schedule_layout = QVBoxLayout()
        
        # Список расписаний
        schedules_list_layout = QHBoxLayout()
        
        # Левая часть: список расписаний
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)
        
        self.schedules_list = QListWidget()
        self.schedules_list.currentRowChanged.connect(self._on_schedule_selected)
        left_layout.addWidget(QLabel("Расписания:"))
        left_layout.addWidget(self.schedules_list)
        
        # Кнопки управления списком
        list_buttons_layout = QHBoxLayout()
        self.btn_add_schedule = QPushButton("Добавить расписание")
        self.btn_add_schedule.clicked.connect(self._add_schedule)
        self.btn_remove_schedule = QPushButton("Удалить расписание")
        self.btn_remove_schedule.clicked.connect(self._remove_schedule)
        list_buttons_layout.addWidget(self.btn_add_schedule)
        list_buttons_layout.addWidget(self.btn_remove_schedule)
        left_layout.addLayout(list_buttons_layout)
        
        schedules_list_layout.addWidget(left_widget)
        
        # Правая часть: форма редактирования расписания
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(0, 0, 0, 0)
        
        edit_label = QLabel("Настройки расписания:")
        right_layout.addWidget(edit_label)
        
        # Дни недели
        days_label = QLabel("Дни недели:")
        right_layout.addWidget(days_label)
        
        days_layout = QHBoxLayout()
        self.day_checks = []
        day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        
        for i, day_name in enumerate(day_names):
            check = QCheckBox(day_name)
            check.stateChanged.connect(self._on_schedule_changed)
            self.day_checks.append(check)
            days_layout.addWidget(check)
        
        days_layout.addStretch()
        right_layout.addLayout(days_layout)
        
        # Время
        time_layout = QHBoxLayout()
        time_label = QLabel("Время:")
        time_layout.addWidget(time_label)
        
        self.time_edit = QTimeEdit()
        self.time_edit.setDisplayFormat("HH:mm")
        self.time_edit.timeChanged.connect(self._on_schedule_changed)
        time_layout.addWidget(self.time_edit)
        time_layout.addStretch()
        right_layout.addLayout(time_layout)
        
        right_layout.addStretch()
        schedules_list_layout.addWidget(right_widget, 2)  # Правая часть занимает больше места
        
        schedule_layout.addLayout(schedules_list_layout)
        
        schedule_group.setLayout(schedule_layout)
        layout.addWidget(schedule_group)
        
        # Инициализируем список расписаний
        self._refresh_schedules_list()
        
        # Обновляем состояние элементов в зависимости от выбранного режима
        self._on_mode_changed()
        
        # Выбираем первое расписание, если есть
        if self.schedules_list.count() > 0:
            self.schedules_list.setCurrentRow(0)
        
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
                self._refresh_all_lists()
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
            self._refresh_all_lists()
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
    
    def _refresh_all_lists(self):
        """Обновить все списки в окне настроек"""
        if hasattr(self, 'folders_tree'):
            self._refresh_folders()
        if hasattr(self, 'rules_tree'):
            self._refresh_rules()
        if hasattr(self, 'sync_rules_table'):
            self._refresh_sync_rules()
        if hasattr(self, 's3_table'):
            self._refresh_s3_buckets()
    
    def _add_rule(self):
        """Добавить новое правило"""
        dialog = RuleDialog(self, self.config, None)
        if dialog.exec_() == QDialog.Accepted:
            self._refresh_all_lists()
    
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
                self._refresh_all_lists()
    
    def _remove_rule(self):
        """Удалить выбранное правило"""
        item = self.rules_tree.currentItem()
        if not item:
            QMessageBox.warning(self, "Предупреждение", "Выберите правило для удаления")
            return
        
        rule_index = self.rules_tree.indexOfTopLevelItem(item)
        try:
            self.config.remove_rule(rule_index)
            self._refresh_all_lists()
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
            
            # Поддержка старого формата для обратной совместимости
            if "max_age_days" in rule:
                age_minutes = rule.get("max_age_days", 30) * 24 * 60
            else:
                age_minutes = rule.get("max_age_minutes", 43200)
            
            # Форматируем возраст в удобочитаемый вид (годы, месяцы, дни, часы, минуты)
            # 1 год = 365 дней, 1 месяц = 30 дней
            years = age_minutes // (365 * 24 * 60)
            remaining_after_years = age_minutes % (365 * 24 * 60)
            months = remaining_after_years // (30 * 24 * 60)
            remaining_after_months = remaining_after_years % (30 * 24 * 60)
            days = remaining_after_months // (24 * 60)
            remaining_minutes = remaining_after_months % (24 * 60)
            hours = remaining_minutes // 60
            minutes = remaining_minutes % 60
            
            age_str_parts = []
            if years > 0:
                age_str_parts.append(f"{years} г.")
            if months > 0:
                age_str_parts.append(f"{months} мес.")
            if days > 0:
                age_str_parts.append(f"{days} дн.")
            if hours > 0:
                age_str_parts.append(f"{hours} ч.")
            if minutes > 0 or len(age_str_parts) == 0:
                age_str_parts.append(f"{minutes} мин.")
            age_str = " ".join(age_str_parts)
            
            item = QTreeWidgetItem([
                rule.get("name", "Без названия"),
                rule.get("pattern", "*"),
                age_str,
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
    
    def _on_mode_changed(self):
        """Обработчик изменения режима работы (интервал/расписание)"""
        schedule_mode = self.schedule_radio.isChecked()
        
        # Интервал всегда активен (используется и для проверки расписания)
        # Но в режиме "По интервалу" он определяет частоту сканирования,
        # а в режиме "По расписанию" - частоту проверки расписания
        
        # Активируем/деактивируем элементы расписания
        self.schedules_list.setEnabled(schedule_mode)
        self.btn_add_schedule.setEnabled(schedule_mode)
        self.btn_remove_schedule.setEnabled(schedule_mode and self.schedules_list.count() > 1)
        for check in self.day_checks:
            check.setEnabled(schedule_mode)
        self.time_edit.setEnabled(schedule_mode)
    
    def _refresh_schedules_list(self):
        """Обновить список расписаний"""
        self.schedules_list.clear()
        schedules = self.config.config.get("schedules", [])
        day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        
        for i, schedule in enumerate(schedules):
            days = schedule.get("days", [])
            time_str = schedule.get("time", "00:00")
            days_str = ", ".join([day_names[d] for d in days]) if days else "Нет дней"
            item_text = f"{days_str} в {time_str}"
            self.schedules_list.addItem(item_text)
        
        # Обновляем состояние кнопки удаления
        self.btn_remove_schedule.setEnabled(self.schedules_list.count() > 1)
    
    def _on_schedule_selected(self, row: int):
        """Обработчик выбора расписания из списка"""
        if row < 0:
            return
        
        schedules = self.config.config.get("schedules", [])
        if row >= len(schedules):
            return
        
        schedule = schedules[row]
        days = schedule.get("days", [])
        time_str = schedule.get("time", "00:00")
        
        # Обновляем чекбоксы дней
        for i, check in enumerate(self.day_checks):
            check.blockSignals(True)
            check.setChecked(i in days)
            check.blockSignals(False)
        
        # Обновляем время
        try:
            hour, minute = map(int, time_str.split(":"))
            self.time_edit.blockSignals(True)
            self.time_edit.setTime(QTime(hour, minute))
            self.time_edit.blockSignals(False)
        except:
            pass
    
    def _on_schedule_changed(self):
        """Обработчик изменения настроек расписания"""
        current_row = self.schedules_list.currentRow()
        if current_row < 0:
            return
        
        schedules = self.config.config.get("schedules", [])
        if current_row >= len(schedules):
            return
        
        # Обновляем расписание
        schedule = schedules[current_row]
        schedule["days"] = [i for i, check in enumerate(self.day_checks) if check.isChecked()]
        time = self.time_edit.time()
        schedule["time"] = time.toString("HH:mm")
        
        # Обновляем отображение в списке
        day_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        days = schedule["days"]
        days_str = ", ".join([day_names[d] for d in days]) if days else "Нет дней"
        item_text = f"{days_str} в {schedule['time']}"
        self.schedules_list.item(current_row).setText(item_text)
    
    def _add_schedule(self):
        """Добавить новое расписание"""
        schedules = self.config.config.get("schedules", [])
        new_schedule = {
            "days": [0, 1, 2, 3, 4, 5, 6],
            "time": "00:00"
        }
        schedules.append(new_schedule)
        self.config.config["schedules"] = schedules
        self._refresh_schedules_list()
        # Выбираем новое расписание
        self.schedules_list.setCurrentRow(len(schedules) - 1)
        # Обновляем состояние кнопки удаления
        self._on_mode_changed()
    
    def _remove_schedule(self):
        """Удалить выбранное расписание"""
        current_row = self.schedules_list.currentRow()
        if current_row < 0:
            return
        
        schedules = self.config.config.get("schedules", [])
        if len(schedules) <= 1:
            QMessageBox.warning(self, "Предупреждение", "Нельзя удалить последнее расписание")
            return
        
        schedules.pop(current_row)
        self.config.config["schedules"] = schedules
        self._refresh_schedules_list()
        
        # Выбираем предыдущее или следующее расписание
        if self.schedules_list.count() > 0:
            new_row = min(current_row, self.schedules_list.count() - 1)
            self.schedules_list.setCurrentRow(new_row)
        
        # Обновляем состояние кнопки удаления
        self._on_mode_changed()
    
    def _save_general_settings(self):
        """Сохранить общие настройки"""
        minutes = self.interval_spin.value()
        old_minutes = self.config.config.get("check_interval_minutes", 60)
        
        self.config.config["check_interval_minutes"] = minutes
        # Удаляем старый формат если есть
        if "check_interval_seconds" in self.config.config:
            del self.config.config["check_interval_seconds"]
        self.config.config["auto_start"] = self.auto_start_check.isChecked()
        
        # Сохраняем настройки расписания
        schedule_enabled = self.schedule_radio.isChecked()
        self.config.config["schedule_enabled"] = schedule_enabled
        
        # Сохраняем текущее редактируемое расписание
        current_row = self.schedules_list.currentRow()
        if current_row >= 0:
            schedules = self.config.config.get("schedules", [])
            if current_row < len(schedules):
                schedule = schedules[current_row]
                schedule["days"] = [i for i, check in enumerate(self.day_checks) if check.isChecked()]
                time = self.time_edit.time()
                schedule["time"] = time.toString("HH:mm")
                self.config.config["schedules"] = schedules
        
        try:
            self.config.save_config()
            
            # Если интервал изменился, перезапускаем мониторинг
            if old_minutes != minutes:
                self.backup_manager.stop_monitoring()
                # Перезапускаем мониторинг с новым интервалом
                self.backup_manager.start_monitoring()
            
            if schedule_enabled:
                schedules = self.config.config.get("schedules", [])
                schedules_count = len(schedules)
                message = f"Настройки сохранены\nРежим: По расписанию\nРасписаний: {schedules_count}\nИнтервал проверки: {minutes} минут (для проверки расписания)"
            else:
                message = f"Настройки сохранены\nРежим: По интервалу\nИнтервал проверки: {minutes} минут"
            
            if self.config.config["auto_start"]:
                message += "\n\nАвтозапуск включен. Приложение будет запускаться при старте Windows."
            else:
                message += "\n\nАвтозапуск отключен."
            
            QMessageBox.information(self, "Успех", message)
        except Exception as e:
            QMessageBox.warning(self, "Предупреждение", 
                              f"Настройки сохранены, но произошла ошибка при установке автозапуска:\n{e}")
    
    def _create_s3_tab(self):
        """Создать вкладку с настройками S3"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        label = QLabel("S3 бакеты:")
        layout.addWidget(label)
        
        # Таблица бакетов
        self.s3_table = QTableWidget()
        self.s3_table.setColumnCount(6)
        self.s3_table.setHorizontalHeaderLabels(["Имя бакета", "Endpoint", "Access Key", "Регион", "Занят правилом", "Действия"])
        self.s3_table.horizontalHeader().setStretchLastSection(True)
        self.s3_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.s3_table.setEditTriggers(QTableWidget.NoEditTriggers)
        layout.addWidget(self.s3_table)
        
        # Кнопки управления
        btn_layout = QHBoxLayout()
        
        btn_add = QPushButton("Добавить бакет")
        btn_add.clicked.connect(self._add_s3_bucket)
        btn_layout.addWidget(btn_add)
        
        btn_edit = QPushButton("Редактировать")
        btn_edit.clicked.connect(self._edit_s3_bucket)
        btn_layout.addWidget(btn_edit)
        
        btn_remove = QPushButton("Удалить бакет")
        btn_remove.clicked.connect(self._remove_s3_bucket)
        btn_layout.addWidget(btn_remove)
        
        btn_refresh = QPushButton("Обновить список")
        btn_refresh.clicked.connect(self._refresh_s3_buckets)
        btn_layout.addWidget(btn_refresh)
        
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
        
        self._refresh_s3_buckets()
        return widget
    
    def _get_bucket_usage_map(self):
        """Получить словарь {имя_бакета: имя_правила} для занятых бакетов"""
        bucket_to_rule = {}
        rules = self.config.get_rules()
        for rule in rules:
            if rule.get("copy_enabled") and rule.get("copy_s3_bucket_name"):
                bucket_name = rule.get("copy_s3_bucket_name")
                rule_name = rule.get("name", "Без названия")
                bucket_to_rule[bucket_name] = rule_name
        return bucket_to_rule
    
    def _refresh_s3_buckets(self):
        """Обновить список S3 бакетов"""
        self.s3_table.setRowCount(0)
        buckets = self.config.get_s3_buckets()
        
        # Получаем информацию о занятости бакетов
        bucket_to_rule = self._get_bucket_usage_map()
        
        for i, bucket in enumerate(buckets):
            row = self.s3_table.rowCount()
            self.s3_table.insertRow(row)
            
            bucket_name = bucket.get("name", "")
            
            # Имя бакета
            name_item = QTableWidgetItem(bucket_name)
            self.s3_table.setItem(row, 0, name_item)
            
            # Endpoint
            endpoint = bucket.get("endpoint", "") or "По умолчанию (AWS)"
            endpoint_item = QTableWidgetItem(endpoint)
            self.s3_table.setItem(row, 1, endpoint_item)
            
            # Access Key (показываем только первые 8 символов)
            access_key = bucket.get("access_key", "")
            access_key_display = access_key[:8] + "..." if len(access_key) > 8 else access_key
            access_key_item = QTableWidgetItem(access_key_display)
            self.s3_table.setItem(row, 2, access_key_item)
            
            # Регион
            region = bucket.get("region", "") or "Не указан"
            region_item = QTableWidgetItem(region)
            self.s3_table.setItem(row, 3, region_item)
            
            # Занят правилом
            rule_name = bucket_to_rule.get(bucket_name)
            rule_item = QTableWidgetItem(rule_name if rule_name else "(Не занят)")
            self.s3_table.setItem(row, 4, rule_item)
            
            # Кнопка проверки доступности
            btn_test = QPushButton("Проверить доступность")
            btn_test.clicked.connect(lambda checked, idx=i: self._test_s3_bucket(idx))
            self.s3_table.setCellWidget(row, 5, btn_test)
        
        # Настраиваем ширину колонок
        self.s3_table.resizeColumnsToContents()
        self.s3_table.setColumnWidth(5, 180)  # Фиксированная ширина для кнопки
    
    def _add_s3_bucket(self):
        """Добавить новый S3 бакет"""
        dialog = S3BucketDialog(self, self.config, None)
        if dialog.exec_() == QDialog.Accepted:
            self._refresh_all_lists()
    
    def _edit_s3_bucket(self):
        """Редактировать выбранный S3 бакет"""
        current_row = self.s3_table.currentRow()
        if current_row < 0:
            QMessageBox.warning(self, "Предупреждение", "Выберите бакет для редактирования")
            return
        
        dialog = S3BucketDialog(self, self.config, current_row)
        if dialog.exec_() == QDialog.Accepted:
            self._refresh_all_lists()
    
    def _remove_s3_bucket(self):
        """Удалить выбранный S3 бакет"""
        current_row = self.s3_table.currentRow()
        if current_row < 0:
            QMessageBox.warning(self, "Предупреждение", "Выберите бакет для удаления")
            return
        
        bucket_name = self.s3_table.item(current_row, 0).text()
        reply = QMessageBox.question(
            self, "Подтверждение",
            f"Удалить бакет '{bucket_name}'?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            try:
                self.config.remove_s3_bucket(current_row)
                self._refresh_all_lists()
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Не удалось удалить бакет: {e}")
    
    def _create_tasks_tab(self):
        """Создать вкладку с задачами"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        label = QLabel("Текущие задачи:")
        layout.addWidget(label)
        
        # Список задач
        self.tasks_list = QListWidget()
        layout.addWidget(self.tasks_list)
        
        # Кнопка обновления
        btn_layout = QHBoxLayout()
        btn_refresh = QPushButton("Обновить список")
        btn_refresh.clicked.connect(self._refresh_tasks)
        btn_layout.addWidget(btn_refresh)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
        
        self._refresh_tasks()
        return widget
    
    def showEvent(self, event):
        """Обработчик события показа окна"""
        super().showEvent(event)
        # Запускаем таймер обновления задач, если окно видимо
        if hasattr(self, 'tasks_timer'):
            self.tasks_timer.start()
            self._refresh_tasks()  # Обновляем сразу при показе
    
    def hideEvent(self, event):
        """Обработчик события скрытия окна"""
        super().hideEvent(event)
        # Останавливаем таймер обновления задач, если окно скрыто
        if hasattr(self, 'tasks_timer'):
            self.tasks_timer.stop()
    
    def _refresh_tasks(self):
        """Обновить список задач"""
        if not hasattr(self, 'tasks_list'):
            return
        
        self.tasks_list.clear()
        
        # Получаем активные задачи из BackupManager
        active_tasks = []
        if hasattr(self.backup_manager, 'get_active_tasks'):
            active_tasks.extend(self.backup_manager.get_active_tasks())
        
        # Получаем активные задачи из SyncManager
        if self.sync_manager and hasattr(self.sync_manager, 'get_active_tasks'):
            active_tasks.extend(self.sync_manager.get_active_tasks())
        
        if not active_tasks:
            no_tasks_label = QLabel("(Нет активных задач)")
            no_tasks_label.setStyleSheet("color: gray; font-style: italic; padding: 10px;")
            no_tasks_item = QListWidgetItem()
            no_tasks_item.setSizeHint(no_tasks_label.sizeHint())
            self.tasks_list.addItem(no_tasks_item)
            self.tasks_list.setItemWidget(no_tasks_item, no_tasks_label)
        else:
            # Отображаем список активных задач с прогресс-барами
            for task in active_tasks:
                task_widget = self._create_task_widget(task)
                task_item = QListWidgetItem()
                task_item.setSizeHint(task_widget.sizeHint())
                self.tasks_list.addItem(task_item)
                self.tasks_list.setItemWidget(task_item, task_widget)
    
    def _create_task_widget(self, task):
        """Создать виджет для отображения задачи с прогресс-баром"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setContentsMargins(5, 5, 5, 5)
        
        # Название задачи
        task_label = QLabel(task.get("name", "Неизвестная задача"))
        task_label.setStyleSheet("font-weight: bold;")
        layout.addWidget(task_label)
        
        # Прогресс-бар
        progress = QProgressBar()
        progress.setMinimum(0)
        progress.setMaximum(100)
        progress_value = task.get("progress", 0)
        progress.setValue(progress_value)
        progress.setFormat("%p%")
        layout.addWidget(progress)
        
        # Статус с информацией о количестве и размере
        status_label = QLabel(task.get("status", "В процессе..."))
        status_label.setStyleSheet("color: gray; font-size: 9pt;")
        layout.addWidget(status_label)
        
        return widget
    
    def _test_s3_bucket(self, bucket_index: int):
        """Проверить доступность S3 бакета"""
        buckets = self.config.get_s3_buckets()
        if bucket_index < 0 or bucket_index >= len(buckets):
            QMessageBox.warning(self, "Ошибка", "Неверный индекс бакета")
            return
        
        bucket = buckets[bucket_index]
        bucket_name = bucket.get("name", "")
        
        # Показываем диалог с прогрессом
        progress_dialog = QMessageBox(self)
        progress_dialog.setWindowTitle("Проверка доступности")
        progress_dialog.setText(f"Проверка доступности бакета '{bucket_name}'...")
        progress_dialog.setStandardButtons(QMessageBox.NoButton)
        progress_dialog.setModal(True)
        progress_dialog.show()
        
        # Создаём и запускаем рабочий поток
        self.test_worker = S3TestWorker(bucket)
        
        def on_test_finished(result, details):
            """Обработчик завершения проверки"""
            progress_dialog.close()
            progress_dialog.deleteLater()
            
            # Показываем результат
            result_dialog = QMessageBox(self)
            result_dialog.setWindowTitle("Результат проверки доступности")
            
            if result == "Успешно":
                result_dialog.setIcon(QMessageBox.Information)
                result_dialog.setText(f"✓ {result}")
            else:
                result_dialog.setIcon(QMessageBox.Warning)
                result_dialog.setText(f"✗ {result}")
            
            result_dialog.setDetailedText(details)
            result_dialog.setStandardButtons(QMessageBox.Ok)
            result_dialog.exec_()
            
            # Очищаем ссылку на поток
            self.test_worker = None
        
        self.test_worker.finished.connect(on_test_finished)
        self.test_worker.start()

