import os

from sqlalchemy import Boolean, Column, DateTime
from datetime import datetime


class StagingBaseModel:
    __table_args__ = {"schema": os.environ.get("STAGING_SCHEMA")}

    is_processed = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.now)
