from datetime import datetime, timedelta
from functools import wraps
from typing import Callable


class throttle(object):
    """
    Decorator that prevents a function from being called more than once every
    time period.
    To create a function that cannot be called more than once a minute:
        @throttle(minutes=1)
        def my_fun():
            pass
    """

    def __init__(self, seconds: int = 0, minutes: int = 0, hours: int = 0) -> None:
        self.throttle_period = timedelta(seconds=seconds, minutes=minutes, hours=hours)
        self.time_of_last_call = datetime.min

    def __call__(self, fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            now = datetime.now()
            time_since_last_call = now - self.time_of_last_call

            if time_since_last_call > self.throttle_period:
                self.time_of_last_call = now
                return fn(*args, **kwargs)

        return wrapper


class param_throttle(object):
    """
    Decorator that prevents a function from being called more than once every
    time period. Expects a function parameter as first argument via *args. This will be checked and compared.
    Example:
    @param_throttle(seconds=30)
    def send_project_update(project_id, message, is_global=False):
        --> same project_id call only once every x - new project_id has its own time comparison

    """

    def __init__(self, seconds: int = 0, minutes: int = 0, hours: int = 0) -> None:
        self.throttle_period = timedelta(seconds=seconds, minutes=minutes, hours=hours)
        self.time_of_last_call = {None: datetime.min}

    def __call__(self, fn: Callable) -> Callable:
        @wraps(fn)
        def wrapper(*args, **kwargs):
            now = datetime.now()
            first_param = args[0]
            if first_param in self.time_of_last_call:
                time_since_last_call = now - self.time_of_last_call[first_param]
                call = True if time_since_last_call > self.throttle_period else False
            else:
                call = True
            if call:
                self.time_of_last_call[first_param] = now
                return fn(*args, **kwargs)

        return wrapper
