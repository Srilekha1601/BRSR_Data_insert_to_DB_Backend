import logging
import os
from datetime import datetime

def setup_logger(excel_filename):
    print("excel_filename",excel_filename)
    logs_base_path = os.path.join(os.getcwd(), "logs")
    os.makedirs(logs_base_path, exist_ok=True)

    # Timestamp for unique log file per Excel run
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f"{os.path.splitext(excel_filename)[0]}_{timestamp}.log"
    log_file_path = os.path.join(logs_base_path, log_filename)

    # Use excel filename as part of logger name to avoid duplicate loggers
    logger_name = f"logger_{os.path.splitext(excel_filename)[0]}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate handlers
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file_path)
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
