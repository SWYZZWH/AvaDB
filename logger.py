import logging
import constant

logger_map = {}


# wrapper of logging.Logger
# for type checking
class Logger:

    def __init__(self, logger):
        self.logger = logger

    def info(self, *args, **kwargs):
        return self.logger.info(*args, **kwargs)

    def warn(self, *args, **kwargs):
        return self.logger.warn(*args, **kwargs)

    def error(self, *args, **kwargs):
        return self.logger.error(*args, **kwargs)


def get_logger(name: str) -> Logger:
    if name not in logger_map:
        # create a new logger
        component_name = constant.APP_NAME + " " + name
        logger = logging.getLogger(component_name)
        logger.setLevel(logging.DEBUG)

        f_handler = logging.FileHandler('{}.log'.format(component_name))
        f_handler.setLevel(logging.DEBUG)

        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        f_handler.setFormatter(f_format)
        logger.addHandler(f_handler)

        logger_map[name] = Logger(logger)
    return logger_map[name]
