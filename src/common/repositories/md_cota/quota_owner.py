from datetime import datetime
from typing import List

from common.repositories.abstract_repository import AbstractRepository
from common.models.md_quota.quota_owner import QuotaOwnerModel


class QuotaOwnerRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(QuotaOwnerModel)

    def find_by_quota(self, quota_id: int) -> List[QuotaOwnerModel]:
        filters = QuotaOwnerModel.quota_id == quota_id

        return self.find_many(filters)

    def invalidate_by_quota(self, quota_id: int, commit: bool = True) -> None:
        filters = QuotaOwnerModel.quota_id == quota_id
        attributes_to_update = {"valid_to": datetime.now()}

        self.update(attributes_to_update, filters, commit)

    def get_for_many_quotas(self, quota_ids: List[int]) -> List[QuotaOwnerModel]:
        filters = (QuotaOwnerModel.quota_id.in_(quota_ids)) & (
            QuotaOwnerModel.valid_to.is_(None)
        )

        return self.find_many(filters)
