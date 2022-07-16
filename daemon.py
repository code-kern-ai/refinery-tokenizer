import threading
from typing import Callable


def run(target: Callable, *args, **kwargs) -> None:
    threading.Thread(
        target=target,
        args=args,
        kwargs=kwargs,
        daemon=True,
    ).start()
