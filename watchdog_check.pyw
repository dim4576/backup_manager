"""
Скрипт для проверки и автоматического запуска Backup Manager.
Запускается планировщиком задач Windows каждый час.
"""
import subprocess
import sys

def is_process_running(process_name: str) -> bool:
    """Проверить, запущен ли процесс"""
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"IMAGENAME eq {process_name}", "/FO", "CSV"],
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
        launcher = r"C:\Users\USER_103\prjects\backup_manager\launcher.pyw"
        pythonw = r"C:\Users\USER_103\AppData\Local\Programs\Python\Python314\pythonw.exe"
        
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
                f.write(f"{datetime.datetime.now()}: {e}\n")

if __name__ == "__main__":
    main()
