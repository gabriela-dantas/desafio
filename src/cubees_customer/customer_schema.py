from pydantic import BaseModel, Field
from typing import List


class CustomerDataSchema(BaseModel):
    quota_id: int
    ownership_percentage: float = Field(None, gt=0, le=1)
    main_owner: str
    cubees_request: List[dict]
