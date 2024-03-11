from sqlalchemy import Column, BigInteger
from sqlalchemy.orm import relationship

from common.database.connection import Base
from common.models.md_quota.md_quota_base import MDQuotaBaseModel
from common.models.md_quota.common.quota_status_cat import QuotaStatusCatCommonModel


class QuotaStatusCatModel(Base, MDQuotaBaseModel, QuotaStatusCatCommonModel):
    __tablename__ = "pl_quota_status_cat"

    quota_status_cat_id = Column(BigInteger, primary_key=True, index=True)

    status_types = relationship("QuotaStatusTypeModel", back_populates="status_cat")
