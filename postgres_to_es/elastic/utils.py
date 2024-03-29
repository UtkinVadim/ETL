import logging
import time
from functools import wraps

from dotenv import find_dotenv, load_dotenv


def load_env():
    load_dotenv(find_dotenv(raise_error_if_not_found=False))


def get_module_logger(mod_name):
    """
    Для использования используйте logger = get_module_logger(__name__)
    """
    logger = logging.getLogger(mod_name)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


def backoff(
    start_sleep_time: float = 0.1, factor: int = 2, border_sleep_time: int = 10
):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            sleep_time = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as err:
                    get_module_logger(__name__).warning(err)
                    time.sleep(sleep_time)
                    sleep_time = (
                        sleep_time * factor
                        if (sleep_time * factor) < border_sleep_time
                        else border_sleep_time
                    )

        return inner

    return func_wrapper
