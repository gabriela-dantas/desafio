from dateutil import parser

from pydantic import BaseModel, validator
from typing import Optional


class PayloadSchema(BaseModel):
    shareId: str
    proposalId: str
    externalId: Optional[str]


class WebhookEventSchema(BaseModel):
    dispatchId: str
    platform: str
    action: str
    eventDate: str
    payload: PayloadSchema

    class Config:
        schema_extra = {
            "dispatchId": "26e17183-3d4e-4496-9ace-94ee4dc596c7",
            "platform": "Mercado Secundário",
            "action": "PROPOSAL_SELECTED",
            "eventDate": "2023-08-24T23:34:18.398Z",
            "payload": {
                "shareId": "401c3d64-0777-47d4-9f53-6f9071629550",
                "proposalId": "fb332012-dfa3-45d1-91a4-0f151f00d643",
            },
        }

    @validator("eventDate", pre=False)
    def validate_event_date(cls, value: str) -> str:
        try:
            valid_date = parser.isoparse(value)
            return valid_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            raise ValueError("eventDate não corresponde a um formato de data ISO.")
