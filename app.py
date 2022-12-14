from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Tuple
import util
import manager
from submodules.model.business_objects import general
import config_handler

app = FastAPI()


class Request(BaseModel):
    project_id: str
    record_id: str
    user_id: str


class RatsRequest(BaseModel):
    project_id: str
    user_id: str
    attribute_id: str


class ReuploadDocbins(BaseModel):
    project_id: str


@app.post("/tokenize_record")
def tokenize_record(request: Request) -> Tuple[int, str]:
    session_token = general.get_ctx_token()
    value = util.tokenize_record(request.project_id, request.record_id)
    general.remove_and_refresh_session(session_token)
    return value, ""


@app.post("/tokenize_project")
def tokenize_project(request: Request) -> Tuple[int, str]:
    session_token = general.get_ctx_token()
    value = manager.start_tokenization_task(request.project_id, request.user_id)
    general.remove_and_refresh_session(session_token)
    return value, ""


# rats = record_attribute_token_statistics
@app.post("/create_rats")
def create_rats(request: RatsRequest) -> Tuple[int, str]:
    session_token = general.get_ctx_token()
    attribute_id = request.attribute_id if request.attribute_id != "" else None
    value = util.start_rats_task(request.project_id, request.user_id, attribute_id)
    general.remove_and_refresh_session(session_token)
    return value, ""


@app.put("/tokenize_project_for_migration/{project_id}")
def tokenize_project_no_use(project_id: str) -> int:
    user_id = util.get_migration_user()
    return util.start_tokenization_task(project_id, user_id)


@app.post("/reupload_docbins")
def reupload_docbins(request: ReuploadDocbins) -> Tuple[int, str]:
    session_token = general.get_ctx_token()
    value = util.reupload_docbins(request.project_id)
    general.remove_and_refresh_session(session_token)
    return value, ""


@app.exception_handler(Exception)
async def error_handler() -> JSONResponse:
    general.rollback()
    return JSONResponse(
        status_code=400,
        content={"message": "Oops! Something went wrong. Database gets a rollback..."},
    )


@app.put("/config_changed")
def config_changed() -> int:
    config_handler.refresh_config()
    return 200
