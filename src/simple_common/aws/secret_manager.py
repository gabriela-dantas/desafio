import boto3
import json

from typing import Dict, Any


class SecretManager:
    def __init__(self) -> None:
        self.__secrets_manager_client = boto3.client("secretsmanager")

    def get_secret_value(self, secret_name: str) -> Dict[str, Any]:  # pragma: no cover
        response = self.__secrets_manager_client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
