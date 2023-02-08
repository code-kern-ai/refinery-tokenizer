from pydantic import BaseModel


class Request(BaseModel):
    project_id: str
    record_id: str
    user_id: str


class AttributeTokenizationRequest(BaseModel):
    project_id: str
    user_id: str
    attribute_id: str


class RatsRequest(BaseModel):
    project_id: str
    user_id: str
    attribute_id: str


class ReuploadDocbins(BaseModel):
    project_id: str


class SaveTokenizer(BaseModel):
    config_string: str
    overwrite: bool = False
