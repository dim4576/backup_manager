"""
Диалог для добавления/редактирования S3 бакета
"""
from PyQt5.QtWidgets import (QDialog, QVBoxLayout, QFormLayout,
                             QLineEdit, QPushButton, QDialogButtonBox,
                             QMessageBox, QCheckBox, QLabel)
from PyQt5.QtCore import Qt
from typing import Optional
from core.config_manager import ConfigManager
from core.s3_manager import normalize_endpoint


class S3BucketDialog(QDialog):
    """Диалог для добавления/редактирования S3 бакета"""
    
    def __init__(self, parent, config: ConfigManager, bucket_index: Optional[int] = None):
        """Инициализация диалога"""
        super().__init__(parent)
        self.config = config
        self.bucket_index = bucket_index
        
        title = "Добавить S3 бакет" if bucket_index is None else "Редактировать S3 бакет"
        self.setWindowTitle(title)
        self.setMinimumWidth(500)
        
        # Загружаем бакет если редактируем
        bucket = {}
        if bucket_index is not None:
            buckets = self.config.get_s3_buckets()
            if 0 <= bucket_index < len(buckets):
                bucket = buckets[bucket_index]
        
        main_layout = QVBoxLayout(self)
        
        # Форма с основными полями
        form_layout = QFormLayout()
        
        # Имя бакета
        self.name_edit = QLineEdit(bucket.get("name", ""))
        form_layout.addRow("Имя бакета:", self.name_edit)
        
        # Endpoint URL
        endpoint = bucket.get("endpoint") or ""
        self.endpoint_edit = QLineEdit(endpoint)
        self.endpoint_edit.setPlaceholderText("https://s3.amazonaws.com или https://s3.yandexcloud.net")
        form_layout.addRow("Endpoint URL:", self.endpoint_edit)
        endpoint_hint = QLabel("(оставьте пустым для AWS S3 по умолчанию)")
        endpoint_hint.setStyleSheet("color: gray; font-size: 9pt;")
        form_layout.addRow("", endpoint_hint)
        
        # Access Key ID
        self.access_key_edit = QLineEdit(bucket.get("access_key", ""))
        form_layout.addRow("Access Key ID:", self.access_key_edit)
        
        # Secret Access Key
        self.secret_key_edit = QLineEdit(bucket.get("secret_key", ""))
        self.secret_key_edit.setEchoMode(QLineEdit.Password)
        form_layout.addRow("Secret Access Key:", self.secret_key_edit)
        
        # Кнопка показать/скрыть пароль
        show_password_layout = QFormLayout()
        self.show_password_check = QCheckBox("Показать пароль")
        self.show_password_check.toggled.connect(self._on_show_password_toggled)
        show_password_layout.addRow("", self.show_password_check)
        form_layout.addRow("", show_password_layout)
        
        # Регион (опционально)
        region = bucket.get("region") or ""
        self.region_edit = QLineEdit(region)
        self.region_edit.setPlaceholderText("us-east-1")
        form_layout.addRow("Регион (опционально):", self.region_edit)
        
        main_layout.addLayout(form_layout)
        main_layout.addStretch()
        
        # Кнопки
        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self._save)
        buttons.rejected.connect(self.reject)
        main_layout.addWidget(buttons)
    
    def _normalize_endpoint(self, endpoint: str) -> str:
        """Нормализовать endpoint URL - добавить протокол если его нет"""
        return normalize_endpoint(endpoint)
    
    def _on_show_password_toggled(self, checked):
        """Обработчик переключения показа пароля"""
        if checked:
            self.secret_key_edit.setEchoMode(QLineEdit.Normal)
        else:
            self.secret_key_edit.setEchoMode(QLineEdit.Password)
    
    def _save(self):
        """Сохранить бакет"""
        name = self.name_edit.text().strip()
        if not name:
            QMessageBox.warning(self, "Предупреждение", "Введите имя бакета")
            return
        
        access_key = self.access_key_edit.text().strip()
        if not access_key:
            QMessageBox.warning(self, "Предупреждение", "Введите Access Key ID")
            return
        
        secret_key = self.secret_key_edit.text().strip()
        if not secret_key:
            QMessageBox.warning(self, "Предупреждение", "Введите Secret Access Key")
            return
        
        endpoint = self.endpoint_edit.text().strip()
        region = self.region_edit.text().strip()
        
        # Нормализуем endpoint - добавляем протокол если его нет
        if endpoint:
            endpoint = self._normalize_endpoint(endpoint)
        
        bucket = {
            "name": name,
            "access_key": access_key,
            "secret_key": secret_key
        }
        
        # Добавляем endpoint и region только если они указаны
        if endpoint:
            bucket["endpoint"] = endpoint
        if region:
            bucket["region"] = region
        
        if self.bucket_index is None:
            self.config.add_s3_bucket(bucket)
        else:
            self.config.update_s3_bucket(self.bucket_index, bucket)
        
        self.accept()
