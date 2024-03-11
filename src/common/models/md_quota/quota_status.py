from sqlalchemy import Column, BigInteger, Integer, ForeignKey
from sqlalchemy.orm import relationship

from common.database.connection import Base
from common.models.md_quota.valid_from_to_base import ValidFromToBaseModel
from common.models.md_quota.quota import QuotaModel
from common.models.md_quota.quota_status_type import QuotaStatusTypeModel


class QuotaStatusModel(Base, ValidFromToBaseModel):
    __tablename__ = "pl_quota_status"

    quota_status_id = Column(BigInteger, primary_key=True, index=True)
    quota_id = Column(Integer, ForeignKey(QuotaModel.quota_id), nullable=False)
    quota_status_type_id = Column(
        Integer, ForeignKey(QuotaStatusTypeModel.quota_status_type_id), nullable=False
    )

    quota = relationship("QuotaModel", back_populates="status")
    status_type = relationship("QuotaStatusTypeModel", back_populates="status")
