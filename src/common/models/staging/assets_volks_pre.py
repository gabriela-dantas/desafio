from sqlalchemy import Column, BigInteger, String, Float

from common.database.connection import Base
from common.models.staging.adm_base_model import AdmBaseModel


class AssetsVolksPreModel(Base, AdmBaseModel):
    __tablename__ = "tb_bens_volks_pre"

    id_bens_volks = Column(BigInteger, primary_key=True, index=True)
    atsf_stgrupo = Column(String(10), nullable=False)
    pkni_grupo = Column(String(10), nullable=False)
    pkni_plano = Column(String(10), nullable=False)
    fkni_codbem = Column(String(10), nullable=False)
    atnd_taxafr = Column(Float, nullable=False)
    atnd_taxadm = Column(Float, nullable=False)
    valor_do_bem = Column(Float, nullable=False)
    valor_da_categoria = Column(Float, nullable=False)
    atsv_descrbem = Column(String(255), nullable=False)
    atdt_descont = Column(String(10), nullable=True)
