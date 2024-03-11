from sqlalchemy import Column, String, Integer, ForeignKey, DateTime
from sqlalchemy.orm import relationship

from common.database.connection import Base
from common.models.md_quota.md_quota_base import MDQuotaBaseModel
from common.models.md_quota.administrator import AdministratorModel
from common.models.md_quota.group import GroupModel
from common.models.md_quota.quota_origin import QuotaOriginModel
from common.models.md_quota.quota_person_type import QuotaPersonTypeModel
from common.models.md_quota.quota_status_type import QuotaStatusTypeModel
from common.models.md_quota.common.quota import QuotaCommonModel


class QuotaModel(Base, MDQuotaBaseModel, QuotaCommonModel):
    __tablename__ = "pl_quota"

    quota_plan = Column(String(255), nullable=True)
    info_date = Column(DateTime, nullable=False)

    quota_status_type_id = Column(
        Integer, ForeignKey(QuotaStatusTypeModel.quota_status_type_id), nullable=False
    )
    administrator_id = Column(
        Integer, ForeignKey(AdministratorModel.administrator_id), nullable=False
    )
    group_id = Column(Integer, ForeignKey(GroupModel.group_id), nullable=False)
    quota_origin_id = Column(
        Integer, ForeignKey(QuotaOriginModel.quota_origin_id), nullable=False
    )
    quota_person_type_id = Column(
        Integer, ForeignKey(QuotaPersonTypeModel.quota_person_type_id), nullable=False
    )

    status_type = relationship("QuotaStatusTypeModel", back_populates="quotas")
    administrator = relationship("AdministratorModel", back_populates="quotas")
    group = relationship("GroupModel", back_populates="quotas")
    origin = relationship("QuotaOriginModel", back_populates="quotas")
    person_type = relationship("QuotaPersonTypeModel", back_populates="quotas")
    status = relationship("QuotaStatusModel", back_populates="quota")
    owners = relationship("QuotaOwnerModel", back_populates="quota")
    history_details = relationship("QuotaHistoryDetailModel", back_populates="quota")
