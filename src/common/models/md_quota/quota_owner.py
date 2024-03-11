from sqlalchemy import Column, BigInteger, Integer, ForeignKey, Float, String, Boolean
from sqlalchemy.orm import relationship

from common.database.connection import Base
from common.models.md_quota.valid_from_to_base import ValidFromToBaseModel
from common.models.md_quota.quota import QuotaModel


class QuotaOwnerModel(Base, ValidFromToBaseModel):
    __tablename__ = "pl_quota_owner"

    quota_owner_id = Column(BigInteger, primary_key=True, index=True)
    ownership_percent = Column(Float, nullable=False, default=100)
    quota_id = Column(Integer, ForeignKey(QuotaModel.quota_id), nullable=False)
    person_code = Column(String(255), nullable=False)
    main_owner = Column(Boolean, nullable=False, default=False)

    quota = relationship("QuotaModel", back_populates="owners")
