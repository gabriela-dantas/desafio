from sqlalchemy import Column, BigInteger, String, DateTime, Integer

from common.database.connection import Base
from common.models.staging.adm_base_model import AdmBaseModel


class GroupsVolksPreModel(Base, AdmBaseModel):
    __tablename__ = "tb_grupos_volks_pre"

    id_grupos_volks = Column(BigInteger, primary_key=True, index=True)
    grupo_quota = Column(String(10), nullable=False)
    status = Column(String(20), nullable=False)
    dt_formacao = Column(DateTime, nullable=False)
    serie = Column(String(10), nullable=False)
    primeira_ass = Column(DateTime, nullable=False)
    prazo = Column(Integer, nullable=False)
    ult_ass = Column(DateTime, nullable=False)
    participantes = Column(Integer, nullable=False)
    nro_ass_atual = Column(Integer, nullable=False)
    data_ass_atual = Column(DateTime, nullable=True)
    contemplados = Column(Integer, nullable=False)
    a_contemplar = Column(Integer, nullable=False)
    liberados = Column(Integer, nullable=False)
    cancelados = Column(Integer, nullable=False)
    desistentes = Column(Integer, nullable=False)
    inativos = Column(Integer, nullable=False)
    inadimplentes = Column(Integer, nullable=False)
    data_encerramento = Column(DateTime, nullable=True)
