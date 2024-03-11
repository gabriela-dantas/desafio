from sqlalchemy import Column, BigInteger
from sqlalchemy.orm import relationship

from common.database.connection import Base
from common.models.md_quota.md_quota_base import MDQuotaBaseModel
from common.models.md_quota.common.asset_type import AssetTypeCommonModel


class AssetTypeModel(Base, MDQuotaBaseModel, AssetTypeCommonModel):
    __tablename__ = "pl_asset_type"

    asset_type_id = Column(BigInteger, primary_key=True, index=True)

    history_details = relationship(
        "QuotaHistoryDetailModel", back_populates="asset_type"
    )
    assets = relationship("AssetModel", back_populates="asset_type")
