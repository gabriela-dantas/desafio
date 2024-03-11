from sqlalchemy import Column, BigInteger, String, Float, Integer

from common.database.connection import Base
from common.models.staging.adm_base_model import AdmBaseModel


class GroupsGMACModel(Base, AdmBaseModel):
    __tablename__ = "tb_grupos_gmac"

    id_grupo_gmac = Column(BigInteger, primary_key=True, index=True)
    codigo_grupo = Column(String(10), nullable=False)
    exclusividade = Column(String(30), nullable=False)
    codigo_bem = Column(String(10), nullable=False)
    descricao = Column(String(255), nullable=False)
    tipo_plano = Column(String(30), nullable=False)
    valor_bem = Column(Float, nullable=False)
    prazo = Column(Integer, nullable=False)
    parcela_pj = Column(Float, nullable=False)
    parcela_pf = Column(Float, nullable=False)
    taxa_adm = Column(Float, nullable=False)
    taxa_fr = Column(Float, nullable=False)
    taxa_adm_mes = Column(Float, nullable=False)
