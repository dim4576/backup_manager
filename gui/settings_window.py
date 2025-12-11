"""
Окно настроек приложения
"""
from PyQt5.QtWidgets import (QDialog, QVBoxLayout, QHBoxLayout, QTabWidget,
                             QPushButton, QLabel, QSpinBox, QCheckBox,
                             QFileDialog, QMessageBox, QDialogButtonBox,
                             QFormLayout, QGroupBox, QWidget, QMenu,
                             QTreeWidgetItem)
from PyQt5.QtCore import Qt, QPoint
from pathlib import Path
from core.config_manager import ConfigManager
from core.backup_manager import BackupManager
from core.logger import get_log_file_path
from gui.widgets import FoldersTreeWidget, RulesTreeWidget
from gui.rule_dialog import RuleDialog


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
    
    def _create_general_tab(self):
        """Создать вкладку с общими настройками"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        group = QGroupBox("Настройки мониторинга")
        form_layout = QFormLayout()
        
        # Интервал проверки
        self.interval_spin = QSpinBox()
        self.interval_spin.setRange(1, 10080)  # От 1 минуты до 7 дней (10080 минут)
        self.interval_spin.setSuffix(" минут")
        # Поддержка старого формата для обратной совместимости
        if "check_interval_seconds" in self.config.config:
            self.interval_spin.setValue(self.config.config.get("check_interval_seconds", 3600) // 60)
        else:
            self.interval_spin.setValue(self.config.config.get("check_interval_minutes", 60))
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
    
    def _save_general_settings(self):
        """Сохранить общие настройки"""
        minutes = self.interval_spin.value()
        old_minutes = self.config.config.get("check_interval_minutes", 60)
        
        self.config.config["check_interval_minutes"] = minutes
        # Удаляем старый формат если есть
        if "check_interval_seconds" in self.config.config:
            del self.config.config["check_interval_seconds"]
        self.config.config["auto_start"] = self.auto_start_check.isChecked()
        
        try:
            self.config.save_config()
            
            # Если интервал изменился, перезапускаем мониторинг
            if old_minutes != minutes:
                self.backup_manager.stop_monitoring()
                # Перезапускаем мониторинг с новым интервалом
                self.backup_manager.start_monitoring()
            
            message = f"Настройки сохранены\nИнтервал проверки: {minutes} минут"
            if self.config.config["auto_start"]:
                message += "\nАвтозапуск включен. Приложение будет запускаться при старте Windows."
            else:
                message += "\nАвтозапуск отключен."
            QMessageBox.information(self, "Успех", message)
        except Exception as e:
            QMessageBox.warning(self, "Предупреждение", 
                              f"Настройки сохранены, но произошла ошибка при установке автозапуска:\n{e}")

