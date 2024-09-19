from typing import Any, Dict, List, Tuple
from controller.tokenizer import (
    add_attribute_to_docbin,
    tokenize_record as tokenize_single_record,
)
import traceback
from controller.task_util import (
    finalize_task,
    set_task_to_started,
    update_tokenization_progress,
)
from submodules.model.models import Attribute, RecordTokenizationTask
from misc.notification import send_notification_created
from submodules.model import enums
from submodules.model.business_objects import (
    attribute,
    general,
    notification,
    project,
    record,
    tokenization,
)
from handler.tokenizer_handler import get_tokenizer_by_project
from misc.util import send_websocket_update, upload_to_minio_after_every_10th_chunk

__prioritized_records = {}


def tokenize_calculated_attribute(
    project_id: str,
    user_id: str,
    task_id: str,
    initial_count: int,
    attribute_name: str,
    include_rats: bool = True,
) -> None:
    session_token = general.get_ctx_token()
    try:
        tokenization_task, tokenizer = __set_up_tokenization(
            project_id, task_id, initial_count
        )

        attribute_item = attribute.get_by_name(project_id, attribute_name)
        non_text_attributes = attribute.get_non_text_attributes(project_id).keys()
        __check_attribute_is_text(attribute_item)

        chunk_size, progress_per_chunk = __get_chunk_size_and_progress_per_chunk(
            initial_count
        )
        record_tokenized_entries = record.get_attribute_data_with_doc_bins_of_records(
            project_id, attribute_name
        )
        chunks = [
            record_tokenized_entries[x : x + chunk_size]
            for x in range(0, len(record_tokenized_entries), chunk_size)
        ]
        for idx, chunk in enumerate(chunks):
            values = [
                add_attribute_to_docbin(tokenizer, record_tokenized_item)
                for record_tokenized_item in chunk
            ]

            record.update_bytes_of_record_tokenized(values, project_id)
            rt_ids_string_for_update = __get_value_ids_string_for_update(values)
            record.update_columns_of_tokenized_records(
                rt_ids_string_for_update, attribute_name
            )
            upload_to_minio_after_every_10th_chunk(idx, project_id, non_text_attributes)
            update_tokenization_progress(
                project_id, tokenization_task, progress_per_chunk
            )
        finalize_task(
            project_id, user_id, non_text_attributes, tokenization_task, include_rats
        )
    except Exception:
        __handle_error(project_id, user_id, task_id)
    finally:
        general.remove_and_refresh_session(session_token, False)


def tokenize_initial_project(
    project_id: str,
    user_id: str,
    task_id: str,
    initial_count: int,
    only_uploaded_attributes: bool = False,
    include_rats: bool = True,
) -> None:
    session_token = general.get_ctx_token()
    try:
        tokenization_task, tokenizer = __set_up_tokenization(
            project_id, task_id, initial_count
        )
        if only_uploaded_attributes:
            text_attributes = attribute.get_text_attributes(
                project_id,
                state_filter=[
                    enums.AttributeState.UPLOADED.value,
                ],
            ).keys()
            non_text_attributes = attribute.get_non_text_attributes(
                project_id,
                state_filter=[
                    enums.AttributeState.UPLOADED.value,
                ],
            ).keys()
        else:
            text_attributes = attribute.get_text_attributes(project_id).keys()
            non_text_attributes = attribute.get_non_text_attributes(project_id).keys()

        full_count = record.count_records_without_tokenization(project_id)
        chunk_size, progress_per_chunk = __get_chunk_size_and_progress_per_chunk(
            full_count
        )
        records = record.get_records_without_tokenization(project_id)
        chunks = [
            records[x : x + chunk_size] for x in range(0, len(records), chunk_size)
        ]
        tokenization_cancelled = False
        for idx, record_chunk in enumerate(chunks):
            record_tokenization_task = tokenization.get(project_id, task_id)
            if record_tokenization_task.state == enums.TokenizerTask.STATE_FAILED.value:
                tokenization_cancelled = True
                break
            entries = []
            for record_item in record_chunk:
                if __remove_from_priority_queue(project_id, record_item.id):
                    continue
                entries.append(
                    tokenize_single_record(
                        project_id, tokenizer, record_item, text_attributes
                    )
                )
            general.add_all(entries)
            upload_to_minio_after_every_10th_chunk(idx, project_id, non_text_attributes)
            update_tokenization_progress(
                project_id, tokenization_task, progress_per_chunk
            )
        if not tokenization_cancelled:
            finalize_task(
                project_id,
                user_id,
                non_text_attributes,
                tokenization_task,
                include_rats,
                only_uploaded_attributes,
            )
        else:
            send_websocket_update(
                project_id,
                False,
                ["docbin", "state", str(record_tokenization_task.state)],
            )
    except Exception:
        __handle_error(project_id, user_id, task_id)
    finally:
        general.remove_and_refresh_session(session_token)


def tokenize_record(project_id: str, record_id: str) -> int:
    if record.has_byte_data(project_id, record_id):
        return 200
    try:
        __add_to_priority_queue(project_id, record_id)

        text_attributes = attribute.get_text_attributes(
            project_id,
            state_filter=[
                enums.AttributeState.UPLOADED.value,
                enums.AttributeState.USABLE.value,
                enums.AttributeState.RUNNING.value,
            ],
        )
        tokenizer = get_tokenizer_by_project(project_id)
        record_item = record.get(project_id, record_id)
        entry = tokenize_single_record(
            project_id, tokenizer, record_item, text_attributes, update_statistic=True
        )
        general.add(entry)
        general.commit()
        return 200
    except Exception:
        __remove_from_priority_queue(project_id, record_id)
        print(traceback.format_exc(), flush=True)
        return 418


def __get_value_ids_string_for_update(values: List[Dict[str, Any]]) -> str:
    value_ids = [f"'{value['_id']}'" for value in values]
    value_ids = ", ".join(value_ids)
    return "(" + value_ids + ")"


def __get_chunk_size_and_progress_per_chunk(full_count: int) -> Tuple[int, float]:
    chunk_size = 500
    progress_per_chunk = round(chunk_size / full_count, 3)
    return chunk_size, progress_per_chunk


def __check_attribute_is_text(attribute_item: Attribute) -> None:
    if not attribute_item:
        raise Exception("Attribute does not exist.")
    if attribute_item.data_type != enums.DataTypes.TEXT.value:
        raise Exception("Attribute is not of type text.")


def __add_to_priority_queue(project_id: str, record_id: str) -> None:
    if project_id not in __prioritized_records:
        __prioritized_records[project_id] = {}
    __prioritized_records[project_id][record_id] = True


def __remove_from_priority_queue(project_id: str, record_id: str) -> bool:
    if project_id in __prioritized_records:
        if record_id in __prioritized_records[project_id]:
            del __prioritized_records[project_id][record_id]
            return True
    else:
        return False


def __set_up_tokenization(
    project_id: str, task_id: str, initial_count: int
) -> RecordTokenizationTask:
    tokenization_task = tokenization.get(project_id, task_id)
    tokenizer = get_tokenizer_by_project(project_id)
    set_task_to_started(project_id, tokenization_task, initial_count)

    return tokenization_task, tokenizer


def __handle_error(project_id: str, user_id: str, task_id: str) -> None:
    try:
        general.rollback()
    except Exception:
        print("couldn't rollback session", flush=True)
    project_item = project.get(project_id)
    if (
        project_item is not None
        and project_item.status != enums.ProjectStatus.IN_DELETION.value
    ):
        tokenization_task = tokenization.get(project_id, task_id)
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
        print("Stopping since no project exists to complete the task", flush=True)
