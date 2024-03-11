from abc import abstractmethod, ABCMeta
from botocore.exceptions import ClientError
from typing import Dict, List, Any

from common.exceptions import InternalServerError, EntityNotFound
from simple_common.logger import logger
from common.dynamo_connection import dynamodb
from simple_common.utils import serialize_dynamo_item, deserialize_dynamo_item


class DynamoAbstractRepository(metaclass=ABCMeta):
    def __init__(self) -> None:
        self._logger = logger
        self._table = dynamodb.Table(self.table_name)

    @staticmethod
    def _serialize(item: Dict[str, Any]) -> Dict[str, Any]:
        return serialize_dynamo_item(item)

    @staticmethod
    def _deserialize(item: Dict[str, Any]) -> Dict[str, Any]:
        return deserialize_dynamo_item(item)

    @property
    @abstractmethod
    def table_name(self) -> str:
        pass  # pragma: no cover

    def scan_all(self) -> List[Dict[str, Any]]:
        items = []
        response = self._table.scan()

        for item in response["Items"]:
            deserialized_item = self._deserialize(item)
            items.append(deserialized_item)

        return items

    def get_item(self, key: Dict[str, Any]) -> Dict[str, Any]:
        try:
            response = self._table.get_item(Key=key)
            return self._deserialize(response["Item"])
        except KeyError:
            message = f"Item não encontrado na tabela {self.table_name}."
            self._logger.error(message)

            raise EntityNotFound(
                detail=message,
                headers={"key": key},
            )
        except Exception as error:
            message = (
                f"Falha desconhecido ao consultar tabela {self.table_name}: {error}."
            )
            self._logger.error(message)

            raise InternalServerError(
                detail=message,
                headers={
                    "error_message": message,
                    "key": key,
                },
            )

    def put_item(self, item: Dict[str, Any]) -> None:
        try:
            item = self._serialize(item)
            self._table.put_item(Item=item)
        except ClientError as client_error:
            error_message = f"Falha ao tentar criar item na tabela {self.table_name}: {client_error.args}"
            self._logger.error(error_message)

            raise InternalServerError(error_message)

    def batch_delete(self, keys: List[Dict[str, Any]]) -> None:
        self._logger.debug(
            f"Iniciando a remoção de {len(keys)} itens, em batch, "
            f"na tabela {self.table_name}."
        )

        try:
            with self._table.batch_writer() as batch:
                for key in keys:
                    batch.delete_item(Key=key)
        except ClientError as client_error:
            error_message = (
                f"Falha ao tentar remover items, em batch, na tabela "
                f"{self.table_name}: {client_error.args}"
            )
            self._logger.error(error_message)

            raise InternalServerError(error_message)
