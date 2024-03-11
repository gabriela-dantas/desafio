from sqlalchemy import Column, BigInteger, String, Text

from common.database.connection import Base
from common.models.staging.staging_base_model import StagingBaseModel


class QuotasAPIModel(Base, StagingBaseModel):
    __tablename__ = "tb_quotas_api"

    id_quotas_itau = Column(BigInteger, primary_key=True, index=True)
    request_body = Column(Text)
    administrator = Column(String(255))
    endpoint_generator = Column(String(255))
