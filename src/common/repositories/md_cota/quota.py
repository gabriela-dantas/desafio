from common.repositories.abstract_repository import AbstractRepository
from common.models.md_quota import QuotaModel


class QuotaRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(QuotaModel)

    def get_by_code(self, quota_code: str) -> QuotaModel:
        filters = (
            (QuotaModel.quota_code == quota_code)
            & (QuotaModel.owners.any(valid_to=None))
            & (QuotaModel.history_details.any(valid_to=None))
            & (QuotaModel.status.any(valid_to=None))
        )

        return self.find_one(filters)

    def update_person_type_id(self, quota_id: int, person_type_id: int) -> None:
        filters = QuotaModel.quota_id == quota_id
        attributes = {"quota_person_type_id": person_type_id}
        self.update(attributes, filters)
