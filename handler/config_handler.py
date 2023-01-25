from typing import Dict, Any, Optional, Union
import requests
import json
import time

from misc import daemon


__config = None

REQUEST_URL = "http://refinery-config:80/full_config"


def __get_config() -> Dict[str, Any]:
    global __config
    if __config:
        return __config
    refresh_config()
    return __config


def refresh_config():
    response = requests.get(REQUEST_URL)
    if response.status_code == 200:
        global __config
        __config = json.loads(json.loads(response.text))
        daemon.run(invalidate_after, 3600)  # one hour
    else:
        raise Exception(
            f"Config service cant be reached -- response.code{response.status_code}"
        )


def get_config_value(
    key: str, subkey: Optional[str] = None
) -> Union[str, Dict[str, str]]:
    config = __get_config()
    if key not in config:
        raise Exception(f"Key {key} coudn't be found in config")
    value = config[key]

    if not subkey:
        return value

    if isinstance(value, dict) and subkey in value:
        return value[subkey]
    else:
        raise Exception(f"Subkey {subkey} coudn't be found in config[{key}]")


def invalidate_after(sec: int) -> None:
    time.sleep(sec)
    global __config
    __config = None
