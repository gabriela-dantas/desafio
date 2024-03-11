import boto3
import os

from typing import Any, Dict, Callable


class DynamoConnection:
    def __init__(self) -> None:
        self.__connections: Dict[str, Callable] = {
            "Local": self.get_local_connection,
            "Cloud": self.get_cloud_connection,
        }

    @staticmethod
    def get_local_connection() -> Any:
        return boto3.resource(
            "dynamodb",
            endpoint_url="http://localhost:8000",
            aws_access_key_id="anything",
            aws_secret_access_key="anything",
            region_name="us-west-2",
        )

    @staticmethod
    def get_cloud_connection() -> Any:
        return boto3.resource("dynamodb")  # pragma: no cover

    def get_connection(self) -> Any:
        dynamo_env = os.environ["DYNAMO_ENV"]
        connection_method = self.__connections[dynamo_env]
        return connection_method()


dynamodb = DynamoConnection().get_connection()
