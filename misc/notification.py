import requests
import os
from .decorator import param_throttle
from submodules.model.business_objects import project


def send_notification_created(project_id: str, user_id: str, throttle: bool) -> None:
    if throttle:
        send_project_update_throttle(
            project_id, f"notification_created:{user_id}", True
        )
    else:
        send_project_update(project_id, f"notification_created:{user_id}", True)


@param_throttle(seconds=5)
def send_project_update_throttle(
    project_id: str, message: str, is_global: bool = False
) -> None:
    send_project_update(project_id, message, is_global)


def send_project_update(project_id: str, message: str, is_global: bool = False) -> None:
    endpoint = os.getenv("WS_NOTIFY_ENDPOINT")
    if not endpoint:
        print(
            "- WS_NOTIFY_ENDPOINT not set -- did you run the start script?", flush=True
        )
        return

    if is_global:
        message = f"GLOBAL:{message}"
    else:
        message = f"{project_id}:{message}"
    project_item = project.get(project_id)
    organization_id = project_item.organization_id
    req = requests.post(
        f"{endpoint}/notify",
        json={"organization": str(organization_id), "message": message},
    )
    if req.status_code != 200:
        print("Could not send notification update", flush=True)
