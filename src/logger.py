from src.config_parser import CONFIG
import logging
import os
import logging.handlers


log_path = os.path.join(CONFIG['logging']['log_folder'], 'app.log')

file_rotate_handler = logging.handlers.TimedRotatingFileHandler(log_path,
                                                                when="midnight",
                                                                backupCount=CONFIG['logging']['keep_logs_for_days'])
file_rotate_handler.setFormatter(logging.Formatter(fmt=CONFIG['logging']['format']))

logging.getLogger('').setLevel(logging.getLevelName(CONFIG['logging']['level']))
logging.getLogger('').addHandler(file_rotate_handler)
if CONFIG['logging']['stderr_handler']:
    logging.getLogger('').addHandler(logging.StreamHandler())

class Logger:
    def __init__(self):
        self.loggers = []

    def get_logger(self, name):
        logger = logging.getLogger(name)
        self.loggers.append({
            "name": name,
            "logger": logger
        })
        return logger

