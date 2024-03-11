from typing import Any, Dict

from aws_lambda_typing import context as context_
from fastapi import status
from fastapi.exceptions import ValidationError

from simple_common.logger import logger
from simple_common.utils import set_lambda_response
from cubees_customer.customer_schema import CustomerDataSchema
from cubees_customer.customer_processing import CustomerProcessing
from common.exceptions import UnprocessableEntity

logger = logger


def log_request(event: Dict[str, Any]) -> None:
    logger.info(f"Recebido evento: {event}")


def lambda_handler(event: Dict[str, Any], _context: context_.Context) -> Dict[str, Any]:
    log_request(event)

    try:
        customer_data = CustomerDataSchema(**event)
        created_customers = CustomerProcessing(customer_data).create()
    except ValidationError as validation_error:
        logger.exception(
            f"Erro de validação nos dados recebidos do evento: {validation_error.errors()}",
            exc_info=validation_error,
        )
        raise UnprocessableEntity(str(validation_error.errors()))

    logger.info(
        f"Execução finalizada com sucesso, criados clientes: {created_customers}"
    )

    return set_lambda_response(status.HTTP_201_CREATED, created_customers)
