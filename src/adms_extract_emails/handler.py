from aws_lambda_typing import context as context_, events
from typing import Dict, Any
from fastapi import HTTPException, status

from adms_extract_emails.extract_process import ExtractProcess
from simple_common.logger import logger
from simple_common.utils import set_lambda_response
from common.database.session import db_session


logger = logger


def log_request(event: events.ses) -> None:
    logger.info(f"Recebido evento: {event}")


def lambda_handler(event: events.ses, _context: context_.Context) -> Dict[str, Any]:
    log_request(event)

    try:
        ExtractProcess(event).start()
    except HTTPException as http_exception:
        logger.exception(
            f"Erro identificado ao tentar processar extrato: {http_exception.detail}",
            exc_info=http_exception,
        )

        db_session.rollback()
        return set_lambda_response(http_exception.status_code, http_exception.detail)

    except Exception as generic_exception:
        logger.exception(
            f"Erro desconhecido ao tentar processar extrato: {generic_exception}",
            exc_info=generic_exception,
        )

        db_session.rollback()
        return set_lambda_response(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"Erro desconhecido: {generic_exception}",
        )

    message = "Extrato processado com sucesso."
    logger.info(message)

    return set_lambda_response(status.HTTP_200_OK, message)
