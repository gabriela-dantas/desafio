from sqlalchemy import Column, BigInteger, String, Float, DateTime, Integer

from common.database.connection import Base
from common.models.staging.adm_base_model import AdmBaseModel


class QuotasGMACPreModel(Base, AdmBaseModel):
    __tablename__ = "tb_quotas_gmac_pre"

    id_quota_gmac = Column(BigInteger, primary_key=True, index=True)
    data_extracao = Column(DateTime, nullable=False)
    grupo = Column(String(10), nullable=False)
    cota = Column(String(10), nullable=False)
    versao = Column(String(10), nullable=False)
    n_contrato = Column(String(50), nullable=False)
    pz_grupo = Column(Integer, nullable=False)
    pz_atual = Column(Integer, nullable=False)
    pz_cota = Column(Integer, nullable=False)
    perc_fc_pg = Column(Float, nullable=False)
    perc_tx_adm_pg = Column(Float, nullable=False)
    perc_fr_pg = Column(Float, nullable=False)
    valor_bem_atual = Column(Float, nullable=False)
    tx_adm = Column(Float, nullable=False)
    tx_fr = Column(Float, nullable=False)
    total_parc_pagas = Column(Integer, nullable=False)
    tipo_pessoa = Column(String(10), nullable=False)
    codigo_bem = Column(String(10), nullable=False)
    bem_atual = Column(String(255), nullable=False)
    dia_venc = Column(Integer, nullable=False)
    data_ult_assembleia = Column(DateTime, nullable=True)
