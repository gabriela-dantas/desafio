from sqlalchemy import Column, BigInteger
from sqlalchemy.orm import relationship

from common.database.connection import Base
from common.models.md_quota.md_quota_base import MDQuotaBaseModel
from common.models.md_quota.common.quota_origin import QuotaOriginCommonModel


class QuotaOriginModel(Base, MDQuotaBaseModel, QuotaOriginCommonModel):
    __tablename__ = "pl_quota_origin"

    quota_origin_id = Column(BigInteger, primary_key=True, index=True)

    quotas = relationship("QuotaModel", back_populates="origin")
