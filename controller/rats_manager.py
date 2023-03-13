import time
import traceback
from datetime import datetime
from typing import Optional

from misc import daemon
from submodules.model import enums
from misc.notification import (
    send_notification_created,
)
from submodules.model import (
    RecordAttributeTokenStatistics,
)
from submodules.model.enums import AttributeState
from submodules.model.business_objects import (
    project,
    attribute,
    record,
    tokenization,
    general,
    notification,
)
from handler.tokenizer_handler import get_tokenizer_by_project
from misc.util import get_docs_from_db, send_websocket_update
from submodules.model.models import RecordTokenizationTask


def trigger_rats_creation(
    project_id: str,
    user_id: str,
    tokenization_task: Optional[RecordTokenizationTask] = None,
    only_uploaded_attributes: bool = False,
) -> None:
    initial_count = record.count_missing_rats_records(project_id)
    if initial_count == 0:
        return
    task = tokenization.create_tokenization_task(
        project_id,
        user_id,
        enums.TokenizerTask.TYPE_TOKEN_STATISTICS.value,
        scope=tokenization_task.scope,
        attribute_name=tokenization_task.attribute_name,
        with_commit=True,
    )
    daemon.run(
        create_rats_entries,
        project_id,
        user_id,
        str(task.id),
        initial_count,
        only_uploaded_attributes,
        None,
    )


# rats = record_attribute_token_statistics
def create_rats_entries(
    project_id: str,
    user_id: str,
    task_id: str,
    initial_count: int,
    only_uploaded_attributes: bool = False,
    attribute_id: Optional[str] = None,
) -> None:
    session_token = general.get_ctx_token()
    if not project.get(project_id):
        # project was deleted in the meantime
        return
    try:
        tokenization_task = __set_up_statistic_calculation(
            project_id, task_id, initial_count
        )
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
            if only_uploaded_attributes:
                text_attributes = attribute.get_text_attributes(
                    project_id,
                    state_filter=[
                        AttributeState.UPLOADED.value,
                    ],
                )
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
        record_set = record.get_missing_rats_records(project_id, attribute_id, 100)
        chunk = 0
        while record_set:
            entries = []
            for record_item in record_set:
                docs = get_docs_from_db(record_item, vocab)
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
                __update_progress(
                    project_id, attribute_id, tokenization_task, initial_count
                )
            record_set = record.get_missing_rats_records(project_id, attribute_id, 100)
            chunk += 1
        __finalize_rats_calculation(project_id, user_id, tokenization_task)
    except Exception:
        __handle_error(project_id, user_id, tokenization_task)
    finally:
        general.remove_and_refresh_session(session_token, False)


def __set_up_statistic_calculation(
    project_id: str, task_id: str, initial_count: int
) -> RecordTokenizationTask:
    tokenization_task = tokenization.get(project_id, task_id)
    tokenization_task.workload = initial_count
    tokenization_task.state = enums.TokenizerTask.STATE_IN_PROGRESS.value
    send_websocket_update(project_id, False, ["rats", "state", tokenization_task.state])
    general.commit()
    return tokenization_task


def __update_progress(
    project_id: str,
    attribute_id: str,
    tokenization_task: RecordTokenizationTask,
    initial_count: int,
) -> None:
    current_count = record.count_missing_rats_records(project_id, attribute_id)
    tokenization_task.progress = round(1 - current_count / initial_count, 4)
    send_websocket_update(
        project_id, True, ["rats", "progress", str(tokenization_task.progress)]
    )
    general.commit()


def __finalize_rats_calculation(
    project_id: str, user_id: str, tokenization_task: RecordTokenizationTask
) -> None:
    record.delete_duplicated_rats()
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


def __handle_error(
    project_id: str, user_id: str, tokenization_task: RecordTokenizationTask
) -> None:
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
