from sqlalchemy import Column, BigInteger, String, Numeric, Integer, DateTime

from common.database.connection import Base
from common.models.staging.adm_base_model import AdmBaseModel


class ContempladosItauModel(Base, AdmBaseModel):
    __tablename__ = "tb_contemplados_itau"

    id_contemplados_itau = Column(BigInteger, primary_key=True, index=True)
    cd_cota = Column(String(5), nullable=False)
    cd_grupo = Column(String(10), nullable=False)
    cd_ponto_venda = Column(String(10), nullable=False)
    dt_adesao = Column(DateTime, nullable=False)
    pz_cota = Column(Integer, nullable=False)
    versao = Column(String(5), nullable=False)
    dt_contemplacao = Column(DateTime, nullable=False)
    pe_lance = Column(Numeric(precision=12, scale=4), nullable=True)
    vl_credito = Column(Numeric(precision=12, scale=2), nullable=True)
    vl_credito_atualizado = Column(Numeric(precision=12, scale=2), nullable=True)
    vl_fgts = Column(Numeric(precision=12, scale=2), nullable=True)
    vl_lance = Column(Numeric(precision=12, scale=2), nullable=True)
    vl_lance_embutido = Column(Numeric(precision=12, scale=2), nullable=True)
    vl_lance_pago = Column(Numeric(precision=12, scale=2), nullable=True)
    vl_recurso_proprio = Column(Numeric(precision=12, scale=2), nullable=True)
    st_contemplacao = Column(String(5), nullable=False)
    st_modalidade = Column(String(30), nullable=False)
