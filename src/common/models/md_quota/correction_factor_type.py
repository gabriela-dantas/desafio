from sqlalchemy import Column, BigInteger, String
from sqlalchemy.orm import relationship

from common.database.connection import Base
from common.models.md_quota.md_quota_base import MDQuotaBaseModel


class CorrectionFactorTypeModel(Base, MDQuotaBaseModel):
    __tablename__ = "pl_correction_factor_type"

    correction_factor_type_id = Column(BigInteger, primary_key=True, index=True)
    correction_factor_type_code = Column(String(50), nullable=False)
    correction_factor_type_desc = Column(String(255), nullable=False)

    groups = relationship("GroupModel", back_populates="correction_factor_type")
