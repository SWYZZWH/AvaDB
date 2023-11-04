import logging
import constant

# Create a custom logger
logger = logging.getLogger(constant.CONST_APP_NAME)
logger.setLevel(logging.DEBUG)

f_handler = logging.FileHandler('file.log')
f_handler.setLevel(logging.DEBUG)

f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
f_handler.setFormatter(f_format)
logger.addHandler(f_handler)


def get_logger():
    return logger
