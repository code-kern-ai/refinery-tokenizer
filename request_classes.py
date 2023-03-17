from pydantic import BaseModel


class Request(BaseModel):
    project_id: str
    record_id: str
    user_id: str
    include_rats: bool
    only_uploaded_attributes: bool  # for uploading later project records, we only need the uploaded ones, the other ones are handled in the gateway


class AttributeTokenizationRequest(BaseModel):
    project_id: str
    user_id: str
    attribute_id: str
    include_rats: bool


class RatsRequest(BaseModel):
    project_id: str
    user_id: str
    attribute_id: str


class ReuploadDocbins(BaseModel):
    project_id: str


class SaveTokenizer(BaseModel):
    config_string: str
    overwrite: bool = False
