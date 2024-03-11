from sqlalchemy import Column, BigInteger, String
from sqlalchemy.orm import relationship

from common.database.connection import Base
from common.models.md_quota.md_quota_base import MDQuotaBaseModel


class QuotaPersonTypeModel(Base, MDQuotaBaseModel):
    __tablename__ = "pl_quota_person_type"

    quota_person_type_id = Column(BigInteger, primary_key=True, index=True)
    quota_person_type_code = Column(String(20), nullable=False)
    quota_person_type_desc = Column(String(255), nullable=False)

    quotas = relationship("QuotaModel", back_populates="person_type")
