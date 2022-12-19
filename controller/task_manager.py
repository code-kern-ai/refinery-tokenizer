from typing import Optional
from controller.rats_manager import create_rats_entries
from controller.tokenization_manager import (
    manage_calculated_attribute_tokenization,
    manage_initial_project_tokenization,
)
from submodules.model import enums
from submodules.model.business_objects import general, notification, record
from submodules.model.business_objects import tokenization
from submodules.model.business_objects.tokenization import create_tokenization_task
from misc import daemon, notification as notification_util
from submodules.model.models import RecordTokenizationTask


def set_up_tokenization_task(
    project_id: str,
    user_id: str,
) -> RecordTokenizationTask:
    notification.create(
        project_id,
        user_id,
        "Started tokenization.",
        "INFO",
        enums.NotificationType.TOKEN_CREATION_STARTED.value,
    )
    general.commit()
    notification_util.send_notification_created(project_id, user_id, False)
    return create_tokenization_task(project_id, user_id, with_commit=True)


def start_tokenization_task(
    project_id: str, user_id: str, type: str, attribute_name=None
) -> int:

    initial_count = record.get_count_all_records(project_id)
    if type == "PROJECT":
        if initial_count != 0:
            task = set_up_tokenization_task(
                project_id,
                user_id,
            )
            daemon.run(
                manage_initial_project_tokenization,
                project_id,
                user_id,
                str(task.id),
                initial_count,
            )
        else:
            start_rats_task(project_id, user_id)

    elif type == "ATTRIBUTE":
        task = set_up_tokenization_task(
            project_id,
            user_id,
        )
        daemon.run(
            manage_calculated_attribute_tokenization,
            project_id,
            user_id,
            str(task.id),
            initial_count,
            attribute_name,
        )
    return 200


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
