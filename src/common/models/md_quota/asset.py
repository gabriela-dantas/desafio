from sqlalchemy import Column, BigInteger, DateTime, Float, Integer, ForeignKey, String
from sqlalchemy.orm import relationship

from common.database.connection import Base
from common.models.md_quota.valid_from_to_base import ValidFromToBaseModel
from common.models.md_quota.group import GroupModel
from common.models.md_quota.asset_type import AssetTypeModel


class AssetModel(Base, ValidFromToBaseModel):
    __tablename__ = "pl_asset"

    asset_id = Column(BigInteger, primary_key=True, index=True)
    asset_code = Column(String(20), nullable=False)
    asset_adm_code = Column(Integer, nullable=True)
    asset_desc = Column(String(255), nullable=False)
    asset_value = Column(Float, nullable=False)
    PLAN = Column(String(255), nullable=True)
    administrator_fee = Column(Float, nullable=True)
    fund_reservation_fee = Column(Float, nullable=True)
    info_date = Column(DateTime, nullable=False)

    group_id = Column(Integer, ForeignKey(GroupModel.group_id), nullable=False)
    asset_type_id = Column(
        Integer, ForeignKey(AssetTypeModel.asset_type_id), nullable=False
    )

    group = relationship("GroupModel", back_populates="assets")
    asset_type = relationship("AssetTypeModel", back_populates="assets")
