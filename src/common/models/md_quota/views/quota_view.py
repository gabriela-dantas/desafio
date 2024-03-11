import os

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    Date,
)

from common.database.connection import Base
from common.models.md_quota.common.quota import QuotaCommonModel
from common.models.md_quota.common.group import GroupCommonModel
from common.models.md_quota.common.quota_history_detail import (
    QuotaHistoryDetailCommonModel,
)
from common.models.md_quota.common.quota_status_type import QuotaStatusTypeCommonModel
from common.models.md_quota.common.quota_status_cat import QuotaStatusCatCommonModel
from common.models.md_quota.common.quota_origin import QuotaOriginCommonModel
from common.models.md_quota.common.asset_type import AssetTypeCommonModel
from common.models.md_quota.common.administrator import AdministratorCommonModel


class QuotaViewModel(
    Base,
    QuotaCommonModel,
    GroupCommonModel,
    QuotaHistoryDetailCommonModel,
    QuotaStatusTypeCommonModel,
    QuotaStatusCatCommonModel,
    QuotaOriginCommonModel,
    AssetTypeCommonModel,
    AdministratorCommonModel,
):
    __table_args__ = {"schema": os.environ.get("MD_COTA_SCHEMA")}

    __tablename__ = "pl_quota_view"

    quota_info_date = Column(DateTime, nullable=False)
    quota_created_at = Column(DateTime, nullable=True)
    quota_modified_at = Column(DateTime, nullable=True)

    grp_current_assembly_date = Column(Date, nullable=True)
    grp_current_assembly_number = Column(Integer, nullable=True)

    quota_history_info_date = Column(DateTime, nullable=False)

    vacancies = Column(Integer, nullable=False)
    vacancies_info_date = Column(DateTime, nullable=False)

    first_assembly_number = Column(Integer, nullable=True)
