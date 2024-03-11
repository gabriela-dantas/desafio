from sqlalchemy import Column, String, DateTime, Integer

from common.models.staging.staging_base_model import StagingBaseModel


class BaseSantanderPreModel(StagingBaseModel):
    grupo = Column(String(10), nullable=False)
    vagas = Column(Integer, nullable=False)
    dt_contmp = Column(DateTime, nullable=False)
    qtde_contmp = Column(Integer, nullable=False)
    data_info = Column(DateTime, nullable=False)
