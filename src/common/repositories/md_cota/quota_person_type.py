from common.repositories.abstract_repository import AbstractRepository
from common.models.md_quota import QuotaPersonTypeModel


class QuotaPersonTypeRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(QuotaPersonTypeModel)

    def get_by_status(self, person_type_code: str) -> QuotaPersonTypeModel:
        filters = QuotaPersonTypeModel.quota_person_type_code == person_type_code
        return self.find_one(filters)
