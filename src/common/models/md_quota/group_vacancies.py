from sqlalchemy import Column, BigInteger, DateTime, Integer, ForeignKey
from sqlalchemy.orm import relationship

from common.database.connection import Base
from common.models.md_quota.valid_from_to_base import ValidFromToBaseModel
from common.models.md_quota.group import GroupModel


class GroupVacanciesModel(Base, ValidFromToBaseModel):
    __tablename__ = "pl_group_vacancies"

    group_vacancies_id = Column(BigInteger, primary_key=True, index=True)
    vacancies = Column(Integer, nullable=False)
    info_date = Column(DateTime, nullable=False)
    group_id = Column(Integer, ForeignKey(GroupModel.group_id), nullable=False)

    group = relationship("GroupModel", back_populates="vacancies")
