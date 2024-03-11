from sqlalchemy import Column, DateTime
from datetime import datetime

from common.models.md_quota.md_quota_base import MDQuotaBaseModel


class ValidFromToBaseModel(MDQuotaBaseModel):
    valid_from = Column(DateTime, default=datetime.now)
    valid_to = Column(DateTime, nullable=True)
