from sqlalchemy import Column, BigInteger, DateTime, Float, Integer, ForeignKey, Date
from sqlalchemy.orm import relationship

from common.database.connection import Base
from common.models.md_quota.md_quota_base import MDQuotaBaseModel
from common.models.md_quota.group import GroupModel
from common.models.md_quota.bid_type import BidTypeModel
from common.models.md_quota.bid_value_type import BidValueTypeModel


class BidModel(Base, MDQuotaBaseModel):
    __tablename__ = "pl_bid"

    bid_id = Column(BigInteger, primary_key=True, index=True)
    value = Column(Float, nullable=False)
    assembly_date = Column(Date, nullable=False)
    assembly_order = Column(Integer, nullable=True)
    info_date = Column(DateTime, nullable=False)
    group_id = Column(Integer, ForeignKey(GroupModel.group_id), nullable=False)
    bid_type_id = Column(Integer, ForeignKey(BidTypeModel.bid_type_id), nullable=False)
    bid_value_type_id = Column(
        Integer, ForeignKey(BidValueTypeModel.bid_value_type_id), nullable=False
    )

    bid_type = relationship("BidTypeModel", back_populates="bids")
    bid_value_type = relationship("BidValueTypeModel", back_populates="bids")
    group = relationship("GroupModel", back_populates="bids")
