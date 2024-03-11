from sqlalchemy import Column, BigInteger

from common.database.connection import Base
from common.models.staging.base_santander_pre import BaseSantanderPreModel


class PrizeDrawSantanderPreModel(Base, BaseSantanderPreModel):
    __tablename__ = "tb_sorteios_santander_pre"

    id_sorteios_santander = Column(BigInteger, primary_key=True, index=True)
