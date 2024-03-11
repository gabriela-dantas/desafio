from sqlalchemy import Column, BigInteger, Integer, ForeignKey
from sqlalchemy.orm import relationship

from common.database.connection import Base
from common.models.md_quota.md_quota_base import MDQuotaBaseModel
from common.models.md_quota.quota_status_cat import QuotaStatusCatModel
from common.models.md_quota.common.quota_status_type import QuotaStatusTypeCommonModel


class QuotaStatusTypeModel(Base, MDQuotaBaseModel, QuotaStatusTypeCommonModel):
    __tablename__ = "pl_quota_status_type"

    quota_status_type_id = Column(BigInteger, primary_key=True, index=True)
    quota_status_cat_id = Column(
        Integer, ForeignKey(QuotaStatusCatModel.quota_status_cat_id), nullable=False
    )

    quotas = relationship("QuotaModel", back_populates="status_type")
    status_cat = relationship("QuotaStatusCatModel", back_populates="status_types")
    status = relationship("QuotaStatusModel", back_populates="status_type")
