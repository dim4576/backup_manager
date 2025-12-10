"""
Launcher для запуска Backup Manager без консоли (только для Windows)
"""
import sys
import os
from pathlib import Path

# Добавляем директорию скрипта в путь
script_dir = Path(__file__).parent
sys.path.insert(0, str(script_dir))

# Импортируем и запускаем main
from main import main

if __name__ == "__main__":
    main()

