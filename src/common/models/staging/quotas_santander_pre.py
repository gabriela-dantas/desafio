from sqlalchemy import Column, BigInteger, String, Float, DateTime, Integer

from common.database.connection import Base
from common.models.staging.adm_base_model import AdmBaseModel


class QuotasSantanderPreModel(Base, AdmBaseModel):
    __tablename__ = "tb_quotas_santander_pre"

    id_quotas_santander = Column(BigInteger, primary_key=True, index=True)
    cd_grupo = Column(String(10), nullable=False)
    cd_cota = Column(String(10), nullable=False)
    nr_contrato = Column(String(30), nullable=False)
    vl_devolver = Column(Float, nullable=False)
    vl_bem_atual = Column(Float, nullable=False)
    cd_produto = Column(String(10), nullable=False)
    pc_fc_pago = Column(Float, nullable=False)
    dt_canc = Column(DateTime, nullable=True)
    dt_venda = Column(DateTime, nullable=True)
    pz_restante_grupo = Column(Integer, nullable=False)
    qt_parcela_a_pagar = Column(Integer, nullable=False)
    nm_situ_entrega_bem = Column(String(255), nullable=False)
    pc_fr_pago = Column(Float, nullable=False)
    pc_tx_adm = Column(Float, nullable=False)
    pc_tx_pago = Column(Float, nullable=False)
    pz_contratado = Column(Integer, nullable=False)
    qt_parcela_paga = Column(Integer, nullable=False)
    pc_fundo_reserva = Column(Float, nullable=False)
    pz_decorrido_grupo = Column(Integer, nullable=False)
