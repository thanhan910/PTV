import logging
import os
import sys


def create_logger(log_filepath, level=logging.INFO, name = "root"):

    os.makedirs(os.path.dirname(log_filepath), exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create a handler for logging to file
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    file_handler = logging.FileHandler(log_filepath)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Create a handler for logging to console
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger

