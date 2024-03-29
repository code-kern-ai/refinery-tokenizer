import os
import pickle
import spacy
from spacy.language import Language
from handler.config_handler import get_config_value
from submodules.model.business_objects import (
    project,
)
import traceback
import subprocess

__downloaded_language_models = ["en_core_web_sm", "de_core_news_sm"]
__tokenizer_by_config_str = {}


def get_tokenizer_by_project(project_id: str) -> Language:
    project_item = project.get(project_id)
    tokenizer_config_str = project_item.tokenizer
    tokenizer = get_tokenizer(tokenizer_config_str)

    return tokenizer


def init_tokenizer(config_string: str) -> None:
    if config_string not in __downloaded_language_models:
        __download_tokenizer(config_string)
    try:
        __tokenizer_by_config_str[config_string] = spacy.load(config_string)
    except Exception:
        print(traceback.format_exc(), flush=True)


def __download_tokenizer(config_string: str) -> None:
    print("trying to download package", config_string, flush=True)
    bashCommand = f"python -m spacy download {config_string}"
    result = subprocess.run(
        bashCommand.split(), stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT
    )
    if result.returncode == 0:
        print("download package", config_string, flush=True)
        __downloaded_language_models.append(config_string)
    else:
        print("error on download of package", config_string, flush=True)


def get_tokenizer(config_string: str) -> Language:
    allowed_configs = get_config_value("spacy_downloads")
    if config_string not in allowed_configs:
        raise ValueError(
            f"Tried to get tokenizer ({config_string}) outside of configured ({allowed_configs})"
        )
    if config_string not in __tokenizer_by_config_str:
        print(f"config string {config_string} not yet loaded", flush=True)
        init_tokenizer(config_string)
        save_tokenizer_as_pickle(config_string)

    return __tokenizer_by_config_str[config_string]


def save_tokenizer_as_pickle(config_string: str, overwrite: bool = False) -> None:
    # this is only relevant if the save_tokenizer endpoint is called
    # when invoked from get_tokenizer, the tokenizer is always loaded
    if config_string not in __tokenizer_by_config_str:
        init_tokenizer(config_string)

    pickle_path = os.path.join(
        "/inference/tokenizers", f"tokenizer-{config_string}.pkl"
    )
    if not os.path.exists(pickle_path) or overwrite:
        os.makedirs(os.path.dirname(pickle_path), exist_ok=True)
        with open(pickle_path, "wb") as f:
            pickle.dump(__tokenizer_by_config_str[config_string], f)
