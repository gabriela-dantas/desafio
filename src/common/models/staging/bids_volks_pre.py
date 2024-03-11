from sqlalchemy import Column, BigInteger, String, DateTime, Float

from common.database.connection import Base
from common.models.staging.adm_base_model import AdmBaseModel


class BidsVolksPreModel(Base, AdmBaseModel):
    __tablename__ = "tb_lances_volks_pre"

    id_lances_volks = Column(BigInteger, primary_key=True, index=True)
    atsf_stgrupo = Column(String(10), nullable=False)
    pkni_grupo = Column(String(10), nullable=False)
    pkni_cota = Column(String(10), nullable=False)
    pkni_subst = Column(String(10), nullable=False)
    pkni_digcontr = Column(String(10), nullable=False)
    pkni_assembleia = Column(String(10), nullable=False)
    atnd_lancebruto = Column(Float, nullable=False)
    atnd_lanceliqdo = Column(Float, nullable=False)
    atnd_percamortz = Column(Float, nullable=False)
    atsf_origem = Column(String(10), nullable=True)
    atdt_captacao = Column(DateTime, nullable=False)
    atni_horacapta = Column(String(10), nullable=False)
    atsf_stlcvenc = Column(String(10), nullable=False)
