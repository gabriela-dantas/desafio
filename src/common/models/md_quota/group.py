from sqlalchemy import (
    Date,
    ForeignKey,
)
from sqlalchemy.orm import relationship

from common.database.connection import Base
from common.models.md_quota.md_quota_base import MDQuotaBaseModel
from common.models.md_quota.administrator import AdministratorModel
from common.models.md_quota.correction_factor_type import CorrectionFactorTypeModel
from sqlalchemy import (
    Column,
    BigInteger,
    Integer,
)
from common.models.md_quota.common.group import GroupCommonModel


class GroupModel(Base, MDQuotaBaseModel, GroupCommonModel):
    __tablename__ = "pl_group"

    group_id = Column(BigInteger, primary_key=True, index=True)
    current_assembly_date = Column(Date, nullable=True)
    current_assembly_number = Column(Integer, nullable=True)

    administrator_id = Column(
        Integer, ForeignKey(AdministratorModel.administrator_id), nullable=False
    )
    correction_factor_type_id = Column(
        Integer,
        ForeignKey(CorrectionFactorTypeModel.correction_factor_type_id),
        nullable=True,
    )

    quotas = relationship("QuotaModel", back_populates="group")
    administrator = relationship("AdministratorModel", back_populates="groups")
    correction_factor_type = relationship(
        "CorrectionFactorTypeModel", back_populates="groups"
    )
    bids = relationship("BidModel", back_populates="group")
    assets = relationship("AssetModel", back_populates="group")
    vacancies = relationship("GroupVacanciesModel", back_populates="group")
