import datetime
import traceback
import daemon
from notification import send_notification_created
from submodules.model import enums
from submodules.model.business_objects import (
    attribute,
    general,
    notification,
    project,
    record,
    tokenization,
)
from submodules.model.models import RecordTokenized
from tokenizer_handler import get_tokenizer_by_project
import tokenizer as tokenizer_manager
from util import (
    put_data_in_minio_bucket,
    send_websocket_update,
    start_rats_task,
)


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
        daemon.run(
            tokenize_new_project,
            project_id,
            user_id,
            str(task.id),
            initial_count,
        )
    else:
        start_rats_task(project_id, user_id)
    return 200


def tokenize_new_project(project_id, user_id, task_id, initial_count):
    try:
        # initial workload
        session_token = general.get_ctx_token()
        tokenization_task = tokenization.get(project_id, task_id)
        tokenizer = get_tokenizer_by_project(project_id)

        set_task_to_started(project_id, tokenization_task, initial_count)

        text_attributes = attribute.get_text_attributes(project_id).keys()
        non_text_attributes = attribute.get_non_text_attributes(project_id).keys()

        chunk_size = 100
        progress_per_chunk = (initial_count / chunk_size) / 100
        record_chunk = record.get_records_without_tokenization(project_id, chunk_size)
        chunk = 1
        while record_chunk:
            for record_item in record_chunk:
                entries = []
                entries.append(
                    tokenize_record(tokenizer, record_item, text_attributes, project_id)
                )

            if not add_entries_to_database(project_id, entries):
                break

            upload_to_minio_after_every_10th_chunk(
                chunk, project_id, non_text_attributes
            )
            update_progress(project_id, tokenization_task, progress_per_chunk)
            record_chunk = record.get_records_without_tokenization(project_id, 100)

        finalize_task(project_id, user_id, non_text_attributes, tokenization_task)
    except Exception:
        handle_error(project_id, user_id, task_id)
    finally:
        general.remove_and_refresh_session(session_token, False)


def set_task_to_started(project_id, tokenization_task, initial_count):
    tokenization_task.workload = initial_count
    tokenization_task.state = enums.TokenizerTask.STATE_IN_PROGRESS.value
    send_websocket_update(
        project_id, False, ["docbin", "state", str(tokenization_task.state)]
    )
    general.commit()


def update_progress(project_id, tokenization_task, progress_per_chunk):
    print("Progress", progress_per_chunk)
    tokenization_task.progress += progress_per_chunk
    send_websocket_update(
        project_id,
        True,
        ["docbin", "progress", str(tokenization_task.progress)],
    )
    general.commit()


def finalize_task(project_id, user_id, non_text_attributes, tokenization_task):
    tokenization.delete_dublicated_tokenization(project_id)
    put_data_in_minio_bucket(project_id, non_text_attributes)
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


def tokenize_record(tokenizer, record, text_attributes, project_id):
    doc_bin_in_bytes = tokenizer_manager.get_doc_bin_in_bytes(
        tokenizer, record, text_attributes
    )
    return RecordTokenized(
        project_id=project_id, record_id=record.id, bytes=doc_bin_in_bytes
    )


def upload_to_minio_after_every_10th_chunk(chunk, project_id, non_text_attributes):
    if chunk % 10 == 0:
        put_data_in_minio_bucket(project_id, non_text_attributes)


def add_entries_to_database(project_id, entries):
    # docbings may cant be added (e.g. project deleted)
    if project.get(project_id):
        general.add_all(entries, True)
        return True
    else:
        return False


def handle_error(project_id, user_id, task_id):
    general.rollback()
    tokenization_task = tokenization.get(project_id, task_id)
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
