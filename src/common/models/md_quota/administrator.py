from sqlalchemy import Column, BigInteger
from sqlalchemy.orm import relationship

from common.database.connection import Base
from common.models.md_quota.md_quota_base import MDQuotaBaseModel
from common.models.md_quota.common.administrator import AdministratorCommonModel


class AdministratorModel(Base, MDQuotaBaseModel, AdministratorCommonModel):
    __tablename__ = "pl_administrator"

    administrator_id = Column(BigInteger, primary_key=True, index=True)

    quotas = relationship("QuotaModel", back_populates="administrator")
    groups = relationship("GroupModel", back_populates="administrator")
