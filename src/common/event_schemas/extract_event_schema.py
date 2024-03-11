import re

from pydantic import BaseModel, validator
from datetime import datetime


class ExtractEventSchema(BaseModel):
    quota_id: int
    person_code: str
    extract_url: str
    extract_created_at: datetime
    extract_filename: str
    extract_s3_path: str

    @validator("extract_created_at", pre=True)
    def validate_extract_created_at(cls, value: str) -> str:
        value = str(value).replace("T", " ")
        value_match = re.match(
            r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+Z?)?$", value
        )

        if value_match is not None:
            return value

        raise ValueError(
            "Invalid datetime format for extract_created_at. "
            "Must be: %Y-%m-%d %H:%M:%S or %Y-%m-%dT%H:%M:%S"
        )
