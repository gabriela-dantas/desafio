from abc import abstractmethod, ABCMeta

from common.repositories.abstract_repository import AbstractRepository


class AbstractService(metaclass=ABCMeta):
    @property
    @abstractmethod
    def _repository(self) -> AbstractRepository:
        pass  # pragma: no cover
