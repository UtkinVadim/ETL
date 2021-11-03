import logging
import time
from functools import wraps

import elasticsearch.exceptions as es_exception
from psycopg2 import OperationalError

logger = logging.getLogger(__name__)


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            sleep_time = start_sleep_time
            while True:
                try:
                    return func(*args, **kwargs)
                except OperationalError as err:
                    logger.warning(f"Postgres Connection Error: {err}")
                    time.sleep(sleep_time)
                    sleep_time = (
                        sleep_time * factor
                        if (sleep_time * factor) < border_sleep_time
                        else border_sleep_time
                    )
                except es_exception.ConnectionError as err:
                    logger.warning(f"Elasticsearch Connection Error: {err}")
                    time.sleep(sleep_time)
                    sleep_time = (
                        sleep_time * factor
                        if (sleep_time * factor) < border_sleep_time
                        else border_sleep_time
                    )

        return inner

    return func_wrapper
