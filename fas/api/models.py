from pydantic import BaseModel


class ResourceID(BaseModel):
    id: int


class Message(BaseModel):
    detail: str
