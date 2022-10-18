import time
from spacy.language import Language
from spacy.tokens import DocBin, Doc
from spacy.vocab import Vocab
import daemon
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from submodules.model import enums
from notification import (
    send_project_update,
    send_project_update_throttle,
    send_notification_created,
)
from submodules.model import (
    RecordTokenized,
    RecordAttributeTokenStatistics,
)
from submodules.model.enums import AttributeState, DataTypes
from submodules.model.business_objects import (
    project,
    attribute,
    record,
    tokenization,
    general,
    user,
    notification,
    organization,
)
from submodules.s3 import controller as s3
from tokenizer_handler import get_tokenizer_by_project


__prioritized_records = {}


def start_tokenization_task(project_id: str, user_id: str) -> int:
    # as thread so the prioritization of single records works
    initial_count = record.count_missing_tokenized_records(project_id)
    if initial_count != 0:
        notification.create(
            project_id,
            user_id,
            "Started tokenization.",
            "INFO",
            enums.NotificationType.TOKEN_CREATION_STARTED.value,
        )
        general.commit()
        send_notification_created(project_id, user_id, False)
        task = tokenization.create_tokenization_task(
            project_id, user_id, with_commit=True
        )
        daemon.run(tokenize_project, project_id, user_id, str(task.id), initial_count)
    else:
        start_rats_task(project_id, user_id)
    return 200


def tokenize_project(
    project_id: str,
    user_id: str,
    task_id: str,
    initial_count: int,
) -> None:
    try:

        session_token = general.get_ctx_token()
        tokenization_task = tokenization.get(project_id, task_id)
        tokenizer = get_tokenizer_by_project(project_id)
        tokenization_task.workload = initial_count
        tokenization_task.state = enums.TokenizerTask.STATE_IN_PROGRESS.value
        send_websocket_update(
            project_id, False, ["docbin", "state", str(tokenization_task.state)]
        )
        general.commit()
        record_set = record.get_missing_tokenized_records(project_id, 100)
        chunk = 0
        missing_columns = []
        while record_set:
            entries = []
            entries_to_add = []
            for record_item in record_set:
                if project_id in __prioritized_records:
                    if record_item.id in __prioritized_records[project_id]:
                        del __prioritized_records[project_id][record_item.id]
                        continue
                bytes, columns, missing_columns = __get_docbin_and_columns(
                    project_id, tokenizer, record_item
                )
                # missing_columns = tmp
                entries.append(
                    RecordTokenized(
                        project_id=project_id,
                        record_id=record_item.id,
                        bytes=bytes,
                        columns=columns,
                    )
                )
            for entry in entries:
                # recheck to ensure during the calculation none were prioritized
                if project_id in __prioritized_records:
                    if entry.record_id in __prioritized_records[project_id]:
                        del __prioritized_records[project_id][record_item.id]
                        continue
                entries_to_add.append(entry)
            project_item = project.get(project_id)
            if not project_item:
                # docbings cant be added (e.g. project deleted)
                break
            else:
                general.add_all(entries_to_add)
                general.commit()
            if chunk % 10 == 0:
                # we dont have chunking for the bucket yet so this will dump everything every 10 rotations
                # takes longer but lfs can work with a reduced set
                # first 100 are added once they are there
                __put_data_in_minio_bucket(project_id, missing_columns)
                # ensure session isn't used up to refresh ocasionally
                session_token = general.remove_and_refresh_session(session_token, True)
                tokenization_task = tokenization.get(project_id, task_id)
            current_count = record.count_missing_tokenized_records(project_id)
            tokenization_task.progress = round(1 - current_count / initial_count, 4)
            send_websocket_update(
                project_id,
                True,
                ["docbin", "progress", str(tokenization_task.progress)],
            )
            general.commit()
            record_set = record.get_missing_tokenized_records(project_id, 100)
            chunk += 1
        # after everything is inserted ensure we only have the values once
        tokenization.delete_dublicated_tokenization(project_id)
        __put_data_in_minio_bucket(project_id, missing_columns)
        tokenization_task.progress = 1
        send_websocket_update(
            project_id, False, ["docbin", "progress", str(tokenization_task.progress)]
        )
        tokenization_task.state = enums.TokenizerTask.STATE_FINISHED.value
        tokenization_task.finished_at = datetime.now()
        general.commit()
        send_websocket_update(
            project_id, False, ["docbin", "state", str(tokenization_task.state)]
        )
        start_rats_task(project_id, user_id)
    except Exception:
        general.rollback()
        if project.get(project_id):
            print(traceback.format_exc(), flush=True)
            tokenization_task.state = enums.TokenizerTask.STATE_FAILED.value
            notification.create(
                project_id,
                user_id,
                "The tokenization failed. Please contact the support.",
                "ERROR",
                enums.NotificationType.TOKEN_CREATION_DONE.value,
            )
            general.commit()
            send_notification_created(project_id, user_id, False)
            send_websocket_update(
                project_id, False, ["docbin", "state", str(tokenization_task.state)]
            )
        else:
            print("Stopping since no project existst to complete the task", flush=True)

    general.remove_and_refresh_session(session_token, False)


def __put_data_in_minio_bucket(project_id: str, missing_columns: List[str]) -> None:
    missing_columns_str = ",\n".join(
        ["'" + k + "',r.data->'" + k + "'" for k in missing_columns]
    )
    org_id = organization.get_id_by_project_id(project_id)
    data = tokenization.get_doc_bin_table_to_json(project_id, missing_columns_str)
    s3.upload_tokenizer_data(org_id, project_id, data)


# rats = record_attribute_token_statistics
def start_rats_task(
    project_id: str, user_id: str, attribute_id: Optional[str] = None
) -> int:
    if tokenization.is_doc_bin_creation_running(project_id):
        # at the end of doc bin creation rats will be calculated
        return

    initial_count = record.count_missing_rats_records(project_id, attribute_id)

    if initial_count != 0:
        task = tokenization.create_tokenization_task(
            project_id,
            user_id,
            enums.TokenizerTask.TYPE_TOKEN_STATISTICS.value,
            with_commit=True,
        )
        daemon.run(
            create_rats_entries,
            project_id,
            user_id,
            attribute_id,
            str(task.id),
            initial_count,
        )
    else:
        notification.create(
            project_id,
            user_id,
            "Completed tokenization.",
            "SUCCESS",
            enums.NotificationType.TOKEN_CREATION_DONE.value,
        )
        general.commit()
    return 200


# rats = record_attribute_token_statistics
def create_rats_entries(
    project_id: str,
    user_id: str,
    attribute_id: str,
    task_id: str,
    initial_count: int,
) -> None:
    session_token = general.get_ctx_token()
    if not project.get(project_id):
        # project was deleted in the meantime
        return
    try:
        tokenization_task = tokenization.get(project_id, task_id)
        tokenization_task.workload = initial_count
        tokenization_task.state = enums.TokenizerTask.STATE_IN_PROGRESS.value
        send_websocket_update(
            project_id, False, ["rats", "state", str(tokenization_task.state)]
        )
        general.commit()
        i = 0
        while initial_count > record.count_tokenized_records(project_id):
            if i > 9:
                print("Docbins missing", flush=True)
                raise Exception("Docbins missing")
            time.sleep(1)
            i += 1
        if attribute_id:
            text_attribute = attribute.get(project_id, attribute_id)
            text_attributes = {text_attribute.name: text_attribute.id}
        else:
            text_attributes = attribute.get_text_attributes(
                project_id,
                state_filter=[
                    AttributeState.UPLOADED.value,
                    AttributeState.USABLE.value,
                    AttributeState.RUNNING.value,
                ],
            )
        vocab = get_tokenizer_by_project(project_id).vocab
        record_set = record.get_missing_rats_records(project_id, 100, attribute_id)
        chunk = 0
        while record_set:
            entries = []
            for record_item in record_set:
                docs = __get_docs_from_db(project_id, str(record_item.record_id), vocab)
                attribute_ids = [str(id) for id in record_item.attribute_ids]
                for col in text_attributes:
                    if text_attributes[col] in attribute_ids:
                        entries.append(
                            RecordAttributeTokenStatistics(
                                project_id=project_id,
                                record_id=record_item.record_id,
                                attribute_id=text_attributes[col],
                                num_token=len(docs[col]),
                            )
                        )
            if not project.get(project_id):
                # rats cant be added (e.g. project deleted)
                break
            else:
                general.add_all(entries)
                general.commit()
            if chunk % 20 == 0:
                # ensure session isn't used up to refresh ocasionally
                session_token = general.remove_and_refresh_session(session_token, True)
                tokenization_task = tokenization.get(project_id, task_id)
            current_count = record.count_missing_rats_records(project_id, attribute_id)
            tokenization_task.progress = round(1 - current_count / initial_count, 4)
            send_websocket_update(
                project_id, True, ["rats", "progress", str(tokenization_task.progress)]
            )
            general.commit()
            record_set = record.get_missing_rats_records(project_id, 100, attribute_id)
            chunk += 1
        # after everything is inserted ensure we only have the values once
        record.delete_dublicated_rats()
        tokenization_task.progress = 1
        send_websocket_update(
            project_id, False, ["rats", "progress", str(tokenization_task.progress)]
        )
        tokenization_task.state = enums.TokenizerTask.STATE_FINISHED.value
        send_websocket_update(
            project_id, False, ["rats", "state", str(tokenization_task.state)]
        )
        tokenization_task.finished_at = datetime.now()
        notification.create(
            project_id,
            user_id,
            "Completed tokenization.",
            "SUCCESS",
            enums.NotificationType.TOKEN_CREATION_DONE.value,
        )
        general.commit()
        send_notification_created(project_id, user_id, False)
    except Exception:
        general.rollback()
        print(traceback.format_exc(), flush=True)
        tokenization_task.state = enums.TokenizerTask.STATE_FAILED.value
        send_websocket_update(
            project_id, False, ["rats", "state", str(tokenization_task.state)]
        )
        notification.create(
            project_id,
            user_id,
            "An error occured during token statistic calculation. Please contact the support.",
            "ERROR",
            enums.NotificationType.TOKEN_CREATION_FAILED.value,
        )
        general.commit()
        send_notification_created(project_id, user_id, False)
    general.remove_and_refresh_session(session_token, False)


def __get_docs_from_db(project_id: str, record_id: str, vocab: Vocab) -> Dict[str, Doc]:
    tbl_entry = tokenization.get_record_tokenized_entry(project_id, record_id)
    if not tbl_entry:
        raise ValueError(f"Can't find docbin for record {record_id}")

    doc_bin_loaded = DocBin().from_bytes(tbl_entry.bytes)
    docs = list(doc_bin_loaded.get_docs(vocab))
    doc_dict = {}
    for (col, doc) in zip(tbl_entry.columns, docs):
        doc_dict[col] = doc
    return doc_dict


def __get_docbin_and_columns(
    project_id: str, tokenizer: Language, record: Any
) -> Tuple[bytes, List[str], List[str]]:
    columns = []
    missing_columns = []
    doc_bin = DocBin()
    for key in record.data:
        attribute_item = attribute.get_by_name(project_id, key)
        if (
            isinstance(record.data[key], str)
            and attribute_item.data_type == DataTypes.TEXT.value
        ):
            doc = tokenizer(record.data[key])
            doc_bin.add(doc)
            columns.append(key)
        else:
            missing_columns.append(key)

    return doc_bin.to_bytes(), columns, missing_columns


def tokenize_record(project_id: str, record_id: str) -> int:
    if record.has_byte_data(project_id, record_id):
        return 200
    try:
        if project_id not in __prioritized_records:
            __prioritized_records[project_id] = {}
        if record_id not in __prioritized_records[project_id]:
            __prioritized_records[project_id][record_id] = True

        text_attributes = attribute.get_text_attributes(
            project_id,
            state_filter=[
                AttributeState.UPLOADED.value,
                AttributeState.USABLE.value,
                AttributeState.RUNNING.value,
            ],
        )
        tokenizer = get_tokenizer_by_project(project_id)
        record_item = record.get(project_id, record_id)
        columns = []
        doc_bin = DocBin()
        for key in record_item.data:
            if isinstance(record_item.data[key], str):
                doc = tokenizer(record_item.data[key])
                doc_bin.add(doc)
                columns.append(key)
                if text_attributes and key in text_attributes:
                    record.create_or_update_token_statistic(
                        project_id, record_id, str(text_attributes[key].id), len(doc)
                    )

        doc_bin_byte = doc_bin.to_bytes()

        tbl_entry = RecordTokenized(
            project_id=project_id,
            record_id=record_id,
            bytes=doc_bin_byte,
            columns=columns,
        )
        general.add(tbl_entry)
        general.commit()
        return 200
    except Exception:
        del __prioritized_records[project_id][record_id]
        print(traceback.format_exc(), flush=True)
        return 418


def get_migration_user() -> str:
    return user.get_migration_user()


def send_websocket_update(
    project_id: str, throttle: bool, arguments: List[str]
) -> None:
    if throttle:
        send_project_update_throttle(project_id, f"tokenization:{':'.join(arguments)}")
    else:
        send_project_update(project_id, f"tokenization:{':'.join(arguments)}")
