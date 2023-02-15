from typing import Any, Dict, List
from spacy.tokens import DocBin
from submodules.model.business_objects import record
from submodules.model.models import Record, RecordTokenized


def get_doc_bin_in_bytes(
    project_id: str,
    tokenizer: str,
    record_item: Record,
    text_attributes: List[str],
    update_statistic: bool = False,
) -> Dict[str, Any]:
    doc_bin = DocBin()
    attribute_names_ordered = []
    for key in record_item.data:
        if key in text_attributes:
            to_be_tokenized = record_item.data[key]
            if not to_be_tokenized:
                # None / null types can't be tokenized by spacy so dummy string is used
                to_be_tokenized = ""
            doc = tokenizer(to_be_tokenized)
            doc_bin.add(doc)
            attribute_names_ordered.append(key)
            if update_statistic:
                record.create_or_update_token_statistic(
                    project_id,
                    record_item.id,
                    text_attributes[key],
                    len(doc),
                    with_commit=True,
                )

    return {
        "bytes": doc_bin.to_bytes(),
        "attribute_names_ordered": attribute_names_ordered,
    }


def add_attribute_to_docbin(
    tokenizer: str,
    tokenized_record: Any,  # from get_attribute_data_with_doc_bins_of_records
) -> Dict[str, Any]:
    doc_bin = DocBin()
    doc_bin_bytes = tokenized_record.bytes
    doc_bin.from_bytes(doc_bin_bytes)
    to_be_tokenized = tokenized_record.attribute_data
    if not to_be_tokenized:
        # None / null types can't be tokenized by spacy so dummy string is used
        to_be_tokenized = ""
    doc = tokenizer(to_be_tokenized)
    doc_bin.add(doc)
    return {
        "_id": tokenized_record.id,
        "bytes": doc_bin.to_bytes(),
    }


def tokenize_record(
    project_id: str,
    tokenizer: str,
    record: Record,
    text_attributes: List[str],
    update_statistic: bool = False,
) -> RecordTokenized:
    tokenization_result = get_doc_bin_in_bytes(
        project_id, tokenizer, record, text_attributes, update_statistic
    )
    return RecordTokenized(
        project_id=project_id,
        record_id=record.id,
        bytes=tokenization_result.get("bytes"),
        columns=tokenization_result.get("attribute_names_ordered"),
    )
