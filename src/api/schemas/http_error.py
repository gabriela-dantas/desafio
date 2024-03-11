from pydantic import BaseModel


class HTTPErrorSchema(BaseModel):
    detail: str

    class Config:
        schema_extra = {"example": {"detail": "Erro ao processar requisição."}}
