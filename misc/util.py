from spacy.tokens import DocBin, Doc
from spacy.vocab import Vocab
from typing import Dict, List
from .notification import (
    send_project_update,
    send_project_update_throttle,
)

from submodules.model.enums import AttributeState
from submodules.model.business_objects import (
    attribute,
    tokenization,
    user,
    organization,
)
from submodules.s3 import controller as s3


def get_attribute_names_string(attribute_names: List[str]) -> str:
    attribute_names = [f'"{name}"' for name in attribute_names]
    attribute_names = ",".join(attribute_names)
    return "{" + attribute_names + "}"


def put_data_in_minio_bucket(project_id: str, missing_columns: List[str]) -> None:
    missing_columns_str = ",\n".join(
        ["'" + k + "',r.data->'" + k + "'" for k in missing_columns]
    )
    org_id = organization.get_id_by_project_id(project_id)
    data = tokenization.get_doc_bin_table_to_json(project_id, missing_columns_str)
    s3.upload_tokenizer_data(org_id, project_id, data)


def get_docs_from_db(project_id: str, record_id: str, vocab: Vocab) -> Dict[str, Doc]:
    tbl_entry = tokenization.get_record_tokenized_entry(project_id, record_id)
    if not tbl_entry:
        raise ValueError(f"Can't find docbin for record {record_id}")

    doc_bin_loaded = DocBin().from_bytes(tbl_entry.bytes)
    docs = list(doc_bin_loaded.get_docs(vocab))
    doc_dict = {}
    for (col, doc) in zip(tbl_entry.columns, docs):
        doc_dict[col] = doc
    return doc_dict


def reupload_docbins(project_id: str) -> int:
    missing_columns = attribute.get_non_text_attributes(
        project_id,
        state_filter=[
            AttributeState.UPLOADED.value,
            AttributeState.USABLE.value,
            AttributeState.AUTOMATICALLY_CREATED.value,
            AttributeState.RUNNING.value,
        ],
    ).keys()
    put_data_in_minio_bucket(project_id, missing_columns)
    return 200


def get_migration_user() -> str:
    return user.get_migration_user()


def send_websocket_update(
    project_id: str, throttle: bool, arguments: List[str]
) -> None:
    if throttle:
        send_project_update_throttle(project_id, f"tokenization:{':'.join(arguments)}")
    else:
        send_project_update(project_id, f"tokenization:{':'.join(arguments)}")


def upload_to_minio_after_every_10th_chunk(
    chunk: int, project_id: str, non_text_attributes: List[str]
) -> None:
    if chunk % 10 == 0:
        put_data_in_minio_bucket(project_id, non_text_attributes)
