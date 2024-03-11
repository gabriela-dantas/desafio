from sqlalchemy import Column, BigInteger, String, Float

from common.database.connection import Base
from common.models.staging.adm_base_model import AdmBaseModel


class GroupsSantanderPreModel(Base, AdmBaseModel):
    __tablename__ = "tb_grupos_santander_pre"

    id_grupos_santander = Column(BigInteger, primary_key=True, index=True)
    grupo = Column(String(10), nullable=False)
    cd_bem = Column(String(10), nullable=False)
    modalidade = Column(String(20), nullable=False)
    nm_bem = Column(String(255), nullable=False)
    vl_bem_atual = Column(Float, nullable=False)
    nm_situ_grupo = Column(String(30), nullable=False)
