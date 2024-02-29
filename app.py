from fastapi import FastAPI, responses, status


from controller import task_manager, tokenization_manager, markdown_file_content
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


@app.middleware("http")
async def handle_db_session(request: Request, call_next):
    session_token = general.get_ctx_token()

    request.state.session_token = session_token
    try:
        response = await call_next(request)
    finally:
        general.remove_and_refresh_session(session_token)

    return response


@app.post("/tokenize_record")
def tokenize_record(request: Request) -> responses.PlainTextResponse:
    tokenization_manager.tokenize_record(request.project_id, request.record_id)
    return responses.PlainTextResponse(status_code=status.HTTP_200_OK)


@app.post("/tokenize_calculated_attribute")
def tokenize_calculated_attribute(
    request: AttributeTokenizationRequest,
) -> responses.PlainTextResponse:
    task_manager.start_tokenization_task(
        request.project_id,
        request.user_id,
        enums.TokenizationTaskTypes.ATTRIBUTE.value,
        request.include_rats,
        False,
        request.attribute_id,
    )
    return responses.PlainTextResponse(status_code=status.HTTP_200_OK)


@app.post("/tokenize_project")
def tokenize_project(request: Request) -> responses.PlainTextResponse:
    task_manager.start_tokenization_task(
        request.project_id,
        request.user_id,
        enums.TokenizationTaskTypes.PROJECT.value,
        request.include_rats,
        request.only_uploaded_attributes,
    )
    return responses.PlainTextResponse(status_code=status.HTTP_200_OK)


# rats = record_attribute_token_statistics
@app.post("/create_rats")
def create_rats(request: RatsRequest) -> responses.PlainTextResponse:
    attribute_id = request.attribute_id if request.attribute_id != "" else None
    task_manager.start_rats_task(
        request.project_id, request.user_id, False, attribute_id
    )
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
    util.reupload_docbins(request.project_id)
    return responses.PlainTextResponse(status_code=status.HTTP_200_OK)


@app.post("/save_tokenizer")
def save_tokenizer_as_pickle(request: SaveTokenizer) -> responses.PlainTextResponse:
    tokenizer_handler.save_tokenizer_as_pickle(request.config_string, request.overwrite)
    return responses.PlainTextResponse(status_code=status.HTTP_200_OK)


@app.put("/cognition/rework-content/{org_id}/{file_id}/{step}")
def rework_markdown_file_content(
    org_id: str, file_id: str, step: str
) -> responses.Response:
    try:
        r = markdown_file_content.rework_markdown_file_content(
            org_id, file_id, step.upper()
        )
    except Exception:
        pass
    if not r:
        return responses.Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    return responses.Response(status_code=status.HTTP_200_OK)


@app.put("/config_changed")
def config_changed() -> responses.PlainTextResponse:
    config_handler.refresh_config()
    return responses.PlainTextResponse(status_code=status.HTTP_200_OK)


@app.get("/healthcheck")
def healthcheck() -> responses.PlainTextResponse:
    text = ""
    status_code = status.HTTP_200_OK
    database_test = general.test_database_connection()
    if not database_test.get("success"):
        error_name = database_test.get("error")
        text += f"database_error:{error_name}:"
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    if not text:
        text = "OK"
    return responses.PlainTextResponse(text, status_code=status_code)
