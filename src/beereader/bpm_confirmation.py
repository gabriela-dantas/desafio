import os
import requests

from requests.exceptions import Timeout, TooManyRedirects

from simple_common.logger import logger
from beereader.constants import BPM_WEBHOOK_TIMEOUT


class BPMConfirmation:
    def __init__(self) -> None:
        self.__logger = logger

    def send_confirmation(self, quota_id: int) -> str:
        endpoint = (
            os.environ["BPM_WEBHOOK_URL"]
            + f"/api/internal/quotas/{quota_id}/invalid_extracts"
        )
        self.__logger.info(f"Chamando Webhook do BPM: {endpoint}")

        try:
            response = requests.post(
                endpoint,
                headers={"token": os.environ["BPM_WEBHOOK_TOKEN"]},
                timeout=BPM_WEBHOOK_TIMEOUT,
            )
            bpm_response = f"status code: {response.status_code}"

            self.__logger.info(f"Resposta do Webhook: {bpm_response}")
            return bpm_response
        except TooManyRedirects as error:
            message = "Requisição Webhook excedeu redirecionamentos."
            self.__logger.error(message, exc_info=error)
            return message
        except Timeout as error:
            message = (
                f"Requisição ao Webhook gerou Timeout "
                f"após {BPM_WEBHOOK_TIMEOUT} segundos."
            )
            self.__logger.error(message, exc_info=error)
            return message
