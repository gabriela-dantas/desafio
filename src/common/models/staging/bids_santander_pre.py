from sqlalchemy import Column, BigInteger, Float

from common.database.connection import Base
from common.models.staging.base_santander_pre import BaseSantanderPreModel


class BidsSantanderPreModel(Base, BaseSantanderPreModel):
    __tablename__ = "tb_lances_santander_pre"

    id_lances_santader = Column(BigInteger, primary_key=True, index=True)
    maior_lance = Column(Float, nullable=False)
    medio_lance = Column(Float, nullable=False)
    menor_lance = Column(Float, nullable=False)
