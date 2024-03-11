from sqlalchemy import Column, BigInteger, String, Float, DateTime, Integer

from common.database.connection import Base
from common.models.staging.adm_base_model import AdmBaseModel


class QuotasVolksPreModel(Base, AdmBaseModel):
    __tablename__ = "tb_quotas_volks_pre"

    id_quotas_volks = Column(BigInteger, primary_key=True, index=True)
    cd_grupo = Column(String(10), nullable=False)
    cd_cota = Column(String(10), nullable=False)
    cd_subst = Column(String(10), nullable=False)
    cd_digito = Column(String(10), nullable=False)
    cd_bem_basico = Column(String(10), nullable=False)
    ds_bem_basico = Column(String(255), nullable=False)
    tp_pes_pes = Column(String(10), nullable=False)
    ds_status_cota = Column(String(10), nullable=False)
    ds_status_contempla = Column(String(10), nullable=False)
    dt_contempla = Column(DateTime, nullable=True)
    ds_status_cobranca = Column(String(10), nullable=False)
    ds_st_cobranca_vc = Column(String(50), nullable=False)
    ds_tipo_venda = Column(String(50), nullable=False)
    ds_status_grupo = Column(String(10), nullable=False)
    ds_st_grupo_vc = Column(String(50), nullable=False)
    ds_stacordo = Column(String(10), nullable=False)
    qt_acordo = Column(Integer, nullable=True)
    ds_tipo_acordo = Column(String(255), nullable=True)
    dt_cancelamento_calculada_vc = Column(DateTime, nullable=True)
    dt_desistencia = Column(DateTime, nullable=True)
    dt_cancel_cota = Column(DateTime, nullable=True)
    vl_taxa_adm = Column(Float, nullable=False)
    vl_tx_fundo_reserva = Column(Float, nullable=False)
    nr_assembleia_desistencia = Column(Integer, nullable=False)
    nr_assembl_cancela = Column(Integer, nullable=False)
    dt_prim_assembl_grupo = Column(DateTime, nullable=True)
    dt_ult_assembleia = Column(DateTime, nullable=True)
    nr_assembleia_vigente = Column(Integer, nullable=False)
    nr_prazo_grupo = Column(Integer, nullable=False)
    nr_prazo_cota = Column(Integer, nullable=False)
    nr_prim_assembl_particip = Column(Integer, nullable=False)
    nr_plano = Column(Integer, nullable=False)
    vl_bem_basico_atu = Column(Float, nullable=False)
    vl_percatr = Column(Float, nullable=False)
    vl_percmes = Column(Float, nullable=False)
    vl_perc_amortiz = Column(Float, nullable=True)
    vl_perc_pago = Column(Float, nullable=False)
    cd_comis_subst = Column(String(10), nullable=False)
    cd_comis_venda_nova = Column(String(10), nullable=False)
