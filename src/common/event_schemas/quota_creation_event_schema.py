from pydantic import BaseModel
from typing import List, Optional


class QuotaSchema(BaseModel):
    quota_code: str
    share_id: Optional[str]


class ExtractEventSchema(BaseModel):
    quota_code_list: List[QuotaSchema]
