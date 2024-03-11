import boto3
import json
import os

from botocore.exceptions import ClientError
from fastapi.encoders import jsonable_encoder

from common.event_schemas.extract_event_schema import ExtractEventSchema
from simple_common.logger import logger
from common.exceptions import InternalServerError


class LambdaInvoke:
    def __init__(self) -> None:
        self.__logger = logger
        self.__lambda_client = boto3.client("lambda")
        self.__lambda_name = os.environ["BEEREADER_LAMBDA_NAME"]

    def invoke(self, event_data: ExtractEventSchema) -> None:
        try:
            self.__logger.debug(f"A invocar Lambda {self.__lambda_name}...")

            json_event = jsonable_encoder(event_data)
            json_event["extract_created_at"] = event_data.extract_created_at.strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )
            payload = json.dumps(json_event)

            self.__lambda_client.invoke(
                FunctionName=self.__lambda_name,
                InvocationType="Event",
                Payload=payload,
            )

            self.__logger.debug(f"Invocação da Lambda {self.__lambda_name} concluída.")

        except ClientError as client_error:
            message = f"Não foi possível invocar Lambda {self.__lambda_name}: {client_error.args}"
            self.__logger.error(message, exc_info=client_error)
            raise InternalServerError(message)
