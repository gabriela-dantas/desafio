import requests
import io
import pdfplumber

from requests.exceptions import TooManyRedirects, Timeout
from fastapi import status
from pdfminer.pdfparser import PDFSyntaxError

from simple_common.logger import logger
from common.exceptions import InternalServerError, UnprocessableEntity
from beereader.constants import S3_DOWNLOAD_TIMEOUT


class FileHandler:
    def __init__(self) -> None:
        self.__logger = logger

    def __get_file_content(self, s3_presigned_url: str) -> bytes:
        truncated_url = s3_presigned_url.split("&X-Amz-Credential=")[0]
        self.__logger.debug(
            f"iniciando download de extrato do S3 via URL "
            f"(Truncada para dados sensíveis): {truncated_url}"
        )

        try:
            response = requests.get(
                s3_presigned_url, stream=True, timeout=S3_DOWNLOAD_TIMEOUT
            )
        except TooManyRedirects as error:
            message = "Requisição de download do extrato excedeu redirecionamentos."
            self.__logger.error(message, exc_info=error)
            raise InternalServerError(detail=message)
        except Timeout as error:
            message = (
                f"Requisição de download do extrato gerou Timeout "
                f"após {S3_DOWNLOAD_TIMEOUT} segundos."
            )
            self.__logger.error(message, exc_info=error)
            raise InternalServerError(detail=message)

        if response.status_code != status.HTTP_200_OK:
            message = (
                f"URL de download do extrato retornou resposta inválida "
                f"status: {response.status_code}, text: {response.text}"
            )
            self.__logger.error(message)
            raise InternalServerError(detail=message)

        file_content = response.content
        self.__logger.debug(
            f"Efetuado download do extrato de tamanho {len(file_content)} Bytes."
        )
        return file_content

    def get_file_text(self, s3_presigned_url: str) -> str:
        file_content = self.__get_file_content(s3_presigned_url)

        file_buffer = io.BytesIO()
        file_buffer.write(file_content)
        file_text = ""

        self.__logger.debug("Iniciada leitura do conteúdo do extrato...")
        try:
            with pdfplumber.open(file_buffer) as pdf_file:
                for i, page in enumerate(pdf_file.pages):
                    self.__logger.debug(
                        f"Lendo página {i + 1} de {len(pdf_file.pages)}..."
                    )
                    file_text += page.extract_text()

            file_text = file_text.replace("%", "").replace("'", "").replace('"', "")
            self.__logger.debug(
                f"Finalizada leitura do extrato. "
                f"Obtido texto de tamanho {len(file_text)} caracteres."
            )
            return file_text
        except PDFSyntaxError as error:
            message = f"Falha na leitura do extrato obtido do S3: {error}"
            self.__logger.error(message)
            raise UnprocessableEntity(detail=message)
