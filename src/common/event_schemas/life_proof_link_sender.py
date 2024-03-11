from pydantic import BaseModel


class ShareIdSchema(BaseModel):
    shareId: str
