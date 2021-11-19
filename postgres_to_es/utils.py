import logging
import time
from dotenv import find_dotenv, load_dotenv
from functools import wraps


def load_env():
    load_dotenv(find_dotenv(raise_error_if_not_found=False))


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def generate_state_name(table_name: str) -> str:
    """
    Конструирует и возвращает название переменной для сохранения состояния

    :param table_name:
    :return:
    """
    return '_'.join([table_name, 'updated_at'])


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
                    get_logger(__name__).warning(err)
                    time.sleep(sleep_time)
                    sleep_time = (
                        sleep_time * factor
                        if (sleep_time * factor) < border_sleep_time
                        else border_sleep_time
                    )

        return inner

    return func_wrapper
