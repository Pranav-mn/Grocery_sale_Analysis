import logging

def log_info(message):
    logging.basicConfig(filename='logs/loginfo.log',
                        level=logging.INFO,
                        format='%(asctime)s , %(levelname)s ,%(message)s ')
    return logging.info(message)

def log_error(message):
    logging.basicConfig(filename='logs/loginfo.log',
                        level=logging.ERROR,
                        format='%(asctime)s , %(levelname)s ,%(message)s ')
    return logging.info(message)

def log_warning(message):
    logging.basicConfig(filename='logs/loginfo.log',
                        level=logging.WARN,
                        format='%(asctime)s , %(levelname)s ,%(message)s ')
    return logging.warning(message)

def log_debug(message):
    logging.basicConfig(filename='logs/loginfo.log',
                        level=logging.DEBUG,
                        format='%(asctime)s , %(levelname)s ,%(message)s ')
    return logging.debug(message)

def log_critical(message):
    logging.basicConfig(filename='logs/loginfo.log',
                        level=logging.CRITICAL,
                        format='%(asctime)s , %(levelname)s ,%(message)s ')
    return logging.critical(message)