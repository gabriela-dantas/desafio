from sqlalchemy import Column, BigInteger, String
from sqlalchemy.orm import relationship

from common.database.connection import Base
from common.models.md_quota.md_quota_base import MDQuotaBaseModel


class BidValueTypeModel(Base, MDQuotaBaseModel):
    __tablename__ = "pl_bid_value_type"

    bid_value_type_id = Column(BigInteger, primary_key=True, index=True)
    bid_value_type_code = Column(String(10), nullable=False)
    bid_value_type_desc = Column(String(255), nullable=False)

    bids = relationship("BidModel", back_populates="bid_value_type")
