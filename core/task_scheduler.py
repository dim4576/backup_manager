"""
Модуль для работы с планировщиком задач Windows.
Создаёт задачу для автоматического перезапуска программы.
"""
import os
import sys
import subprocess
from pathlib import Path
from typing import Tuple, Optional

from core.logger import setup_logger

logger = setup_logger("TaskScheduler")

# Имя задачи в планировщике
TASK_NAME = "BackupManagerWatchdog"


def get_pythonw_path() -> str:
    """Получить путь к pythonw.exe"""
    python_dir = Path(sys.executable).parent
    pythonw = python_dir / "pythonw.exe"
    if pythonw.exists():
        return str(pythonw)
    # Fallback на python.exe
    return sys.executable


def get_launcher_path() -> str:
    """Получить путь к launcher.pyw"""
    # Определяем директорию проекта
    if getattr(sys, 'frozen', False):
        # Если запущено как exe
        app_dir = Path(sys.executable).parent
    else:
        # Если запущено как скрипт
        app_dir = Path(__file__).parent.parent
    
    return str(app_dir / "launcher.pyw")


def task_exists() -> bool:
    """Проверить, существует ли задача в планировщике"""
    try:
        result = subprocess.run(
            ["schtasks", "/Query", "/TN", TASK_NAME],
            capture_output=True,
            text=True,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        )
        return result.returncode == 0
    except Exception as e:
        logger.error(f"Ошибка проверки задачи в планировщике: {e}")
        return False


def create_watchdog_script() -> str:
    """Создать скрипт-чекер для проверки и запуска программы"""
    if getattr(sys, 'frozen', False):
        app_dir = Path(sys.executable).parent
    else:
        app_dir = Path(__file__).parent.parent
    
    script_path = app_dir / "watchdog_check.pyw"
    launcher_path = get_launcher_path()
    
    script_content = f'''"""
Скрипт для проверки и автоматического запуска Backup Manager.
Запускается планировщиком задач Windows каждый час.
"""
import subprocess
import sys

def is_process_running(process_name: str) -> bool:
    """Проверить, запущен ли процесс"""
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"IMAGENAME eq {{process_name}}", "/FO", "CSV"],
            capture_output=True,
            text=True,
            creationflags=0x08000000  # CREATE_NO_WINDOW
        )
        return process_name.lower() in result.stdout.lower()
    except:
        return False

def is_backup_manager_running() -> bool:
    """Проверить, запущен ли Backup Manager"""
    # Проверяем pythonw.exe с launcher.pyw или main.py в командной строке
    try:
        result = subprocess.run(
            ["wmic", "process", "where", "name='pythonw.exe'", "get", "commandline"],
            capture_output=True,
            text=True,
            creationflags=0x08000000  # CREATE_NO_WINDOW
        )
        output = result.stdout.lower()
        return "launcher.pyw" in output or "backup_manager" in output
    except:
        pass
    
    # Альтернативная проверка через tasklist
    return is_process_running("pythonw.exe")

def main():
    if not is_backup_manager_running():
        # Запускаем программу
        launcher = r"{launcher_path}"
        pythonw = r"{get_pythonw_path()}"
        
        try:
            subprocess.Popen(
                [pythonw, launcher],
                creationflags=0x08000000  # CREATE_NO_WINDOW
            )
        except Exception as e:
            # Логируем ошибку в файл
            from pathlib import Path
            log_file = Path(launcher).parent / "watchdog_error.log"
            with open(log_file, "a", encoding="utf-8") as f:
                import datetime
                f.write(f"{{datetime.datetime.now()}}: {{e}}\\n")

if __name__ == "__main__":
    main()
'''
    
    with open(script_path, "w", encoding="utf-8") as f:
        f.write(script_content)
    
    logger.info(f"Создан скрипт watchdog: {script_path}")
    return str(script_path)


def create_task() -> Tuple[bool, Optional[str]]:
    """Создать задачу в планировщике Windows"""
    try:
        # Создаём скрипт watchdog
        watchdog_script = create_watchdog_script()
        pythonw = get_pythonw_path()
        
        # XML для задачи планировщика
        xml_content = f'''<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Проверяет запущен ли Backup Manager и запускает его при необходимости</Description>
  </RegistrationInfo>
  <Triggers>
    <CalendarTrigger>
      <Repetition>
        <Interval>PT1H</Interval>
        <StopAtDurationEnd>false</StopAtDurationEnd>
      </Repetition>
      <StartBoundary>2024-01-01T00:00:00</StartBoundary>
      <Enabled>true</Enabled>
      <ScheduleByDay>
        <DaysInterval>1</DaysInterval>
      </ScheduleByDay>
    </CalendarTrigger>
    <LogonTrigger>
      <Enabled>true</Enabled>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <IdleSettings>
      <StopOnIdleEnd>false</StopOnIdleEnd>
      <RestartOnIdle>false</RestartOnIdle>
    </IdleSettings>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT5M</ExecutionTimeLimit>
    <Priority>7</Priority>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>"{pythonw}"</Command>
      <Arguments>"{watchdog_script}"</Arguments>
    </Exec>
  </Actions>
</Task>'''
        
        # Сохраняем XML во временный файл
        if getattr(sys, 'frozen', False):
            app_dir = Path(sys.executable).parent
        else:
            app_dir = Path(__file__).parent.parent
        
        xml_path = app_dir / "task_scheduler.xml"
        with open(xml_path, "w", encoding="utf-16") as f:
            f.write(xml_content)
        
        # Создаём задачу через schtasks
        result = subprocess.run(
            ["schtasks", "/Create", "/TN", TASK_NAME, "/XML", str(xml_path), "/F"],
            capture_output=True,
            text=True,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        )
        
        # Удаляем временный XML
        try:
            xml_path.unlink()
        except:
            pass
        
        if result.returncode == 0:
            logger.info(f"Задача '{TASK_NAME}' успешно создана в планировщике")
            return True, None
        else:
            error = result.stderr or result.stdout
            logger.error(f"Ошибка создания задачи: {error}")
            return False, error
            
    except Exception as e:
        logger.error(f"Ошибка при создании задачи в планировщике: {e}")
        return False, str(e)


def delete_task() -> Tuple[bool, Optional[str]]:
    """Удалить задачу из планировщика"""
    try:
        result = subprocess.run(
            ["schtasks", "/Delete", "/TN", TASK_NAME, "/F"],
            capture_output=True,
            text=True,
            creationflags=subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        )
        
        if result.returncode == 0:
            logger.info(f"Задача '{TASK_NAME}' удалена из планировщика")
            return True, None
        else:
            error = result.stderr or result.stdout
            return False, error
            
    except Exception as e:
        logger.error(f"Ошибка при удалении задачи: {e}")
        return False, str(e)


def ensure_task_exists() -> bool:
    """
    Убедиться, что задача существует в планировщике.
    Если нет — создать её.
    
    Returns:
        bool: True если задача существует или была создана успешно
    """
    if sys.platform != "win32":
        logger.debug("Планировщик задач доступен только в Windows")
        return False
    
    if task_exists():
        logger.debug(f"Задача '{TASK_NAME}' уже существует в планировщике")
        return True
    
    logger.info(f"Задача '{TASK_NAME}' не найдена, создаю...")
    success, error = create_task()
    
    if not success:
        logger.warning(f"Не удалось создать задачу в планировщике: {error}")
    
    return success
