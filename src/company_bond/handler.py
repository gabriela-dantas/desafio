from aws_lambda_typing import context as context_
from typing import Dict, Any


from fastapi import status
from fastapi.exceptions import ValidationError


from common.event_schemas.company_bond_event_schema import CompanyBondEventSchema
from company_bond.lambda_company_bond import LamdaCompanyBond
from simple_common.logger import logger
from common.exceptions import UnprocessableEntity
from simple_common.utils import set_lambda_response

logger = logger


def lambda_handler(event: Dict[str, Any], _context: context_.Context) -> Dict[str, Any]:
    logger.info(f"Evento recebido {event}")
    try:
        data_company_bond = CompanyBondEventSchema(**event)
        LamdaCompanyBond().invoke(data_company_bond)
    except ValidationError as validation_error:
        logger.exception(
            f"Erro de validação nos dados recebidos do evento: {validation_error.errors()}",
            exc_info=validation_error,
        )
        raise UnprocessableEntity(str(validation_error.errors()))

    message = (
        "Invocação da lambda de vínculo para representantes executada com sucesso."
    )
    logger.info(message)

    return set_lambda_response(status.HTTP_201_CREATED, message)
