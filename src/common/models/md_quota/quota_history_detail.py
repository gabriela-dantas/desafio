from sqlalchemy import (
    Column,
    BigInteger,
    DateTime,
    Integer,
    ForeignKey,
)
from sqlalchemy.orm import relationship

from common.database.connection import Base
from common.models.md_quota.valid_from_to_base import ValidFromToBaseModel
from common.models.md_quota.asset_type import AssetTypeModel
from common.models.md_quota.quota import QuotaModel
from common.models.md_quota.common.quota_history_detail import (
    QuotaHistoryDetailCommonModel,
)


class QuotaHistoryDetailModel(
    Base, ValidFromToBaseModel, QuotaHistoryDetailCommonModel
):
    __tablename__ = "pl_quota_history_detail"

    quota_history_detail_id = Column(BigInteger, primary_key=True, index=True)
    info_date = Column(DateTime, nullable=False)
    quota_id = Column(Integer, ForeignKey(QuotaModel.quota_id), nullable=False)
    asset_type_id = Column(
        Integer, ForeignKey(AssetTypeModel.asset_type_id), nullable=False
    )

    quota = relationship("QuotaModel", back_populates="history_details")
    asset_type = relationship("AssetTypeModel", back_populates="history_details")
