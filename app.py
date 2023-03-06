from fastapi import FastAPI, responses, status


from controller import task_manager, tokenization_manager
from misc import util
from handler import config_handler, tokenizer_handler
from request_classes import (
    AttributeTokenizationRequest,
    RatsRequest,
    Request,
    ReuploadDocbins,
    SaveTokenizer,
)
from submodules.model.business_objects import general
from submodules.model import enums

app = FastAPI()


@app.post("/tokenize_record")
def tokenize_record(request: Request) -> responses.PlainTextResponse:
    session_token = general.get_ctx_token()
    tokenization_manager.tokenize_record(request.project_id, request.record_id)
    general.remove_and_refresh_session(session_token)
    return responses.PlainTextResponse(status_code=status.HTTP_200_OK)


@app.post("/tokenize_calculated_attribute")
def tokenize_calculated_attribute(
    request: AttributeTokenizationRequest,
) -> responses.PlainTextResponse:
    session_token = general.get_ctx_token()
    task_manager.start_tokenization_task(
        request.project_id,
        request.user_id,
        enums.TokenizationTaskTypes.ATTRIBUTE.value,
        request.attribute_id,
    )
    general.remove_and_refresh_session(session_token)
    return responses.PlainTextResponse(status_code=status.HTTP_200_OK)


@app.post("/tokenize_project")
def tokenize_project(request: Request) -> responses.PlainTextResponse:
    session_token = general.get_ctx_token()
    task_manager.start_tokenization_task(
        request.project_id, request.user_id, enums.TokenizationTaskTypes.PROJECT.value
    )
    general.remove_and_refresh_session(session_token)
    return responses.PlainTextResponse(status_code=status.HTTP_200_OK)


# rats = record_attribute_token_statistics
@app.post("/create_rats")
def create_rats(request: RatsRequest) -> responses.PlainTextResponse:
    session_token = general.get_ctx_token()
    attribute_id = request.attribute_id if request.attribute_id != "" else None
    task_manager.start_rats_task(request.project_id, request.user_id, attribute_id)
    general.remove_and_refresh_session(session_token)
    return responses.PlainTextResponse(status_code=status.HTTP_200_OK)


@app.put("/tokenize_project_for_migration/{project_id}")
def tokenize_project_no_use(project_id: str) -> responses.PlainTextResponse:
    user_id = util.get_migration_user()
    status_code = task_manager.start_tokenization_task(
        project_id, user_id, enums.TokenizationTaskTypes.PROJECT.value
    )
    return responses.PlainTextResponse(status_code=status_code)


@app.post("/reupload_docbins")
def reupload_docbins(request: ReuploadDocbins) -> responses.PlainTextResponse:
    session_token = general.get_ctx_token()
    util.reupload_docbins(request.project_id)
    general.remove_and_refresh_session(session_token)
    return responses.PlainTextResponse(status_code=status.HTTP_200_OK)


@app.post("/save_tokenizer")
def save_tokenizer_as_pickle(request: SaveTokenizer) -> responses.PlainTextResponse:
    tokenizer_handler.save_tokenizer_as_pickle(request.config_string, request.overwrite)
    return responses.PlainTextResponse(status_code=status.HTTP_200_OK)


@app.exception_handler(Exception)
async def error_handler() -> responses.PlainTextResponse:
    general.rollback()
    return responses.PlainTextResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content="Oops! Something went wrong. Database gets a rollback...",
    )


@app.put("/config_changed")
def config_changed() -> responses.PlainTextResponse:
    config_handler.refresh_config()
    return responses.PlainTextResponse(status_code=status.HTTP_200_OK)
