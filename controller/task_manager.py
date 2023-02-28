from typing import Optional
from controller.rats_manager import create_rats_entries
from controller.tokenization_manager import (
    tokenize_calculated_attribute,
    tokenize_initial_project,
)
from submodules.model import enums
from submodules.model.business_objects import attribute, general, notification, record
from submodules.model.business_objects import tokenization
from submodules.model.business_objects.tokenization import create_tokenization_task
from misc import daemon, notification as notification_util
from submodules.model.models import RecordTokenizationTask
from fastapi import status


def set_up_tokenization_task(
    project_id: str, user_id: str, scope: str, attribute_name: Optional[str] = None
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
    return create_tokenization_task(
        project_id,
        user_id,
        scope=scope,
        attribute_name=attribute_name,
        with_commit=True,
    )


def start_tokenization_task(
    project_id: str, user_id: str, type: str, attribute_id: Optional[str] = None
) -> int:

    if type == enums.RecordTokenizationScope.PROJECT.value:
        initial_count = record.count_records_without_tokenization(project_id)
        if initial_count != 0:
            task = set_up_tokenization_task(
                project_id, user_id, enums.RecordTokenizationScope.PROJECT.value
            )
            daemon.run(
                tokenize_initial_project,
                project_id,
                user_id,
                str(task.id),
                initial_count,
            )
        else:
            start_rats_task(project_id, user_id)

    elif type == enums.RecordTokenizationScope.ATTRIBUTE.value:
        attribute_name = attribute.get(project_id, attribute_id).name
        initial_count = record.get_count_all_records(project_id)
        task = set_up_tokenization_task(
            project_id,
            user_id,
            enums.RecordTokenizationScope.ATTRIBUTE.value,
            attribute_name,
        )
        daemon.run(
            tokenize_calculated_attribute,
            project_id,
            user_id,
            str(task.id),
            initial_count,
            attribute_name,
        )
    return status.HTTP_200_OK


def start_rats_task(
    project_id: str, user_id: str, attribute_id: Optional[str] = None
) -> int:
    if tokenization.is_doc_bin_creation_running(project_id):
        # at the end of doc bin creation rats will be calculated
        return

    initial_count = record.count_missing_rats_records(project_id, attribute_id)

    attribute_name = None
    if attribute_id:
        attribute_name = attribute.get(project_id, attribute_id)

    if initial_count != 0:
        task = tokenization.create_tokenization_task(
            project_id,
            user_id,
            enums.TokenizerTask.TYPE_TOKEN_STATISTICS.value,
            scope=enums.RecordTokenizationScope.ATTRIBUTE.value
            if attribute_id
            else enums.RecordTokenizationScope.PROJECT.value,
            attribute_name=attribute_name,
            with_commit=True,
        )
        daemon.run(
            create_rats_entries,
            project_id,
            user_id,
            str(task.id),
            initial_count,
            attribute_id,
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
    return status.HTTP_200_OK
