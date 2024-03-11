from sqlalchemy import Column, BigInteger, String

from common.database.connection import Base
from common.models.staging.adm_base_model import AdmBaseModel


class CustomersGMACPreModel(Base, AdmBaseModel):
    __tablename__ = "tb_clientes_gmac_pre"

    id_cliente_gmac = Column(BigInteger, primary_key=True, index=True)
    grupo = Column(String(10), nullable=False)
    cota = Column(String(10), nullable=False)
    versao = Column(String(10), nullable=False)
    nome_razao = Column(String(255), nullable=False)
    cpf_cnpj = Column(String(14), nullable=False)
    cep = Column(String(10), nullable=False)
    ddd = Column(String(10), nullable=True)
    celular = Column(String(20), nullable=True)
    telefone = Column(String(20), nullable=True)
    ddd1 = Column(String(10), nullable=True)
    tel1 = Column(String(20), nullable=True)
    ddd2 = Column(String(10), nullable=True)
    tel2 = Column(String(20), nullable=True)
    ddd3 = Column(String(10), nullable=True)
    tel3 = Column(String(20), nullable=True)
    ddd4 = Column(String(10), nullable=True)
    tel4 = Column(String(20), nullable=True)
    ddd5 = Column(String(10), nullable=True)
    tel5 = Column(String(20), nullable=True)
    ddd6 = Column(String(10), nullable=True)
    tel6 = Column(String(20), nullable=True)
