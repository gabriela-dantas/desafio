import json

from aws_lambda_typing import context as context_
from typing import Dict, Any
from fastapi import HTTPException, status
from fastapi.exceptions import ValidationError
from fastapi.encoders import jsonable_encoder

from beereader_invoke.lambda_invoke import LambdaInvoke
from simple_common.logger import logger
from common.event_schemas.extract_event_schema import ExtractEventSchema
from simple_common.utils import set_lambda_response


logger = logger


def log_request(event: Dict[str, Any]) -> None:
    body = json.loads(event.get("body", "{}"))
    body["extract_url"] = body.get("extract_url", "").split("&X-Amz-Credential=")[0]

    data = {
        "endpoint": f"{event.get('httpMethod')} {event.get('path')}",
        "path_params": event.get("pathParameters"),
        "query_params": event.get("queryStringParameters"),
        "body": body,
    }

    logger.info(f"Recebido evento (Dados sensíveis da URL do S3 truncados): {data}")


def lambda_handler(event: Dict[str, Any], _context: context_.Context) -> Dict[str, Any]:
    log_request(event)

    try:
        event_data = json.loads(event.get("body", "{}"))
        event_data = ExtractEventSchema(**event_data)
        LambdaInvoke().invoke(event_data)
    except ValidationError as validation_error:
        logger.exception(
            f"Erro de validação nos dados de extrato recebidos do evento: {validation_error.errors()}",
            exc_info=validation_error,
        )
        return set_lambda_response(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            jsonable_encoder({"detail": validation_error.errors()}),
        )

    except HTTPException as http_exception:
        logger.exception(
            f"Erro identificado ao tentar invocar lambda do BeeReader: {http_exception.detail}",
            exc_info=http_exception,
        )
        return set_lambda_response(http_exception.status_code, http_exception.detail)

    except Exception as generic_exception:
        logger.exception(
            f"Erro desconhecido ao tentar invocar lambda do BeeReader: {generic_exception}",
            exc_info=generic_exception,
        )
        return set_lambda_response(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"Erro desconhecido: {generic_exception}",
        )

    message = "Invocação do BeaReader feita com sucesso."
    logger.info(message)

    return set_lambda_response(status.HTTP_200_OK, message)
