from sqlalchemy import Column, BigInteger, String

from common.database.connection import Base
from common.models.staging.adm_base_model import AdmBaseModel


class CustomersVolksPreModel(Base, AdmBaseModel):
    __tablename__ = "tb_clientes_volks_pre"

    id_cliente_vilks = Column(BigInteger, primary_key=True, index=True)
    cd_grupo = Column(String(10), nullable=False)
    cd_cota = Column(String(10), nullable=False)
    cd_subst = Column(String(10), nullable=False)
    cd_digito = Column(String(255), nullable=False)
    cd_cpf_cnpj = Column(String(14), nullable=False)
    nm_pessoa = Column(String(10), nullable=False)
    ds_ddd_tel_cel = Column(String(10), nullable=True)
    ds_numero_tel_cel = Column(String(20), nullable=True)
    ds_ddd_tel_adi = Column(String(10), nullable=True)
    ds_numero_tel_adi = Column(String(20), nullable=True)
    ds_contato_tel_adi = Column(String(255), nullable=True)
    ds_ddd_tel_com = Column(String(10), nullable=True)
    ds_numero_tel_com = Column(String(20), nullable=True)
    ds_contato_tel_com = Column(String(255), nullable=True)
    ds_ddd_tel_res = Column(String(10), nullable=True)
    ds_numero_tel_res = Column(String(20), nullable=True)
    ds_contato_tel_res = Column(String(255), nullable=True)
    ds_endereco_eml_p = Column(String(255), nullable=True)
    ds_endereco_eml_c = Column(String(255), nullable=True)
    ds_endereco_eml_a = Column(String(255), nullable=True)
    ds_regional = Column(String(10), nullable=True)
