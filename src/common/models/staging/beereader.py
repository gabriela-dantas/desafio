from sqlalchemy import Column, BigInteger, String, Text, DateTime, Integer

from common.database.connection import Base
from common.models.staging.staging_base_model import StagingBaseModel


class BeeReaderModel(Base, StagingBaseModel):
    __tablename__ = "tb_beereader"

    id_beereader = Column(BigInteger, primary_key=True, index=True)
    file_name = Column(String(255))
    bpm_quota_id = Column(Integer)
    adm = Column(String(255))
    quota_data = Column(Text)
    s3_path = Column(String(255))
    attachment_date = Column(DateTime)
