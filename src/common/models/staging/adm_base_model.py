from sqlalchemy import Column, DateTime

from common.models.staging.staging_base_model import StagingBaseModel


class AdmBaseModel(StagingBaseModel):
    data_info = Column(DateTime, nullable=False)
