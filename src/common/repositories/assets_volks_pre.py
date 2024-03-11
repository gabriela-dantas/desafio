from common.repositories.abstract_repository import AbstractRepository
from common.models.staging import AssetsVolksPreModel


class AssetsVolksPreRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(AssetsVolksPreModel)
