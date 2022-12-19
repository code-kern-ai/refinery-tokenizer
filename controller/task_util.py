from typing import List
from datetime import datetime
from controller.rats_manager import trigger_rats_creation
from submodules.model import enums
from submodules.model.business_objects import general
from submodules.model.business_objects import tokenization
from misc.util import put_data_in_minio_bucket, send_websocket_update
from submodules.model.models import RecordTokenizationTask


def set_task_to_started(
    project_id: str, tokenization_task: RecordTokenizationTask, initial_count: int
) -> None:
    tokenization_task.workload = initial_count
    tokenization_task.state = enums.TokenizerTask.STATE_IN_PROGRESS.value
    send_websocket_update(
        project_id, False, ["docbin", "state", str(tokenization_task.state)]
    )
    general.commit()


def update_tokenization_progress(
    project_id: str,
    tokenization_task: RecordTokenizationTask,
    progress_per_chunk: float,
) -> None:
    tokenization_task.progress += progress_per_chunk
    send_websocket_update(
        project_id,
        True,
        ["docbin", "progress", str(tokenization_task.progress)],
    )
    general.commit()


def finalize_task(
    project_id: str,
    user_id: str,
    non_text_attributes: List[str],
    tokenization_task: RecordTokenizationTask,
) -> None:
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
    trigger_rats_creation(project_id, user_id)
