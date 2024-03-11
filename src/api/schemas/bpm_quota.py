from pydantic import BaseModel, validator

from api.schemas.http_error import HTTPErrorSchema
from api.constants import BPM_OPT_IN_ID


class CardDestinationSchema(BaseModel):
    id: int
    name: str

    @validator("id", pre=True)
    def validate_id(cls, value: int) -> int:
        if value != BPM_OPT_IN_ID:
            raise ValueError(
                f"ID informado é diferente do ID de OPT-IN: {BPM_OPT_IN_ID}"
            )

        return value


class CardSchema(BaseModel):
    id: int
    title: str


class DataSchema(BaseModel):
    to: CardDestinationSchema
    card: CardSchema


class BPMEventSchema(BaseModel):
    data: DataSchema

    class Config:
        schema_extra = {
            "example": {
                "data": {
                    "to": {"id": 318196139, "name": "Opt-in"},
                    "card": {"id": 633833604, "title": "BZ0000190"},
                }
            }
        }


class BPMQuotaCreateNotFoundError(HTTPErrorSchema):
    class Config:
        schema_extra = {
            "example": {
                "detail": "Cota com código BZ0000018, recebida do pipefy, "
                "não encontrada no banco do MD Cota."
            }
        }


class BPMQuotaCreateTimeoutError(HTTPErrorSchema):
    class Config:
        schema_extra = {
            "example": {
                "detail": "Requisição para https://staging.bazardoconsorcio.com.br/api/internal/md_cotas/quotas "
                "gerou timeout após 10 segundos."
            }
        }


class BPMQuotaTooManyRedirectsError(HTTPErrorSchema):
    class Config:
        schema_extra = {
            "example": {
                "detail": "Requisição para https://staging.bazardoconsorcio.com.br/api/internal/md_cotas/quotas "
                "excedeu redirecionamentos."
            }
        }


class BPMQuotaInternalError(HTTPErrorSchema):
    class Config:
        schema_extra = {
            "example": {
                "detail": "Requisição para criar cota no BPM retornou status de erro: 403 - Forbidden"
            }
        }


class CreatedBPMQuotaSchema(BaseModel):
    quota_code: str
