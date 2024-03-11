import pytest
import json
import email
import os

from typing import Dict, Any
from fastapi import status
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from email import encoders
from email.mime.base import MIMEBase

from io import BytesIO
from botocore.response import StreamingBody

from adms_extract_emails.handler import lambda_handler
from adms_extract_emails.constantes import EXCEL_TYPE, CSV_TYPE


@pytest.fixture(scope="function")
def event() -> Dict[str, Any]:
    return {
        "Records": [
            {
                "eventSource": "aws:ses",
                "eventVersion": "1.0",
                "ses": {
                    "mail": {
                        "commonHeaders": {
                            "date": "Wed, 7 Oct 2015 12:34:56 -0700",
                            "from": ["Jane Doe <janedoe@example.com>"],
                            "messageId": "a841f56kqsbsh4033etkhps16na6s7h80dasfu81",
                            "returnPath": "janedoe@example.com",
                            "subject": "Test Subject",
                            "to": ["johndoe@example.com"],
                        },
                        "destination": ["gmac@example.com"],
                        "headersTruncated": False,
                        "messageId": "a841f56kqsbsh4033etkhps16na6s7h80dasfu81",
                        "source": "janedoe@example.com",
                        "timestamp": "1970-01-01T00:00:00Z",
                    }
                },
            }
        ]
    }


@pytest.fixture(scope="function")
def event_itau() -> Dict[str, Any]:
    return {
        "Records": [
            {
                "eventSource": "aws:ses",
                "eventVersion": "1.0",
                "ses": {
                    "mail": {
                        "commonHeaders": {
                            "date": "Wed, 7 Oct 2015 12:34:56 -0700",
                            "from": ["Jane Doe <janedoe@example.com>"],
                            "messageId": "a841f56kqsbsh4033etkhps16na6s7h80dasfu81",
                            "returnPath": "janedoe@example.com",
                            "subject": "Test Subject",
                            "to": ["johndoe@example.com"],
                        },
                        "destination": ["itau@example.com"],
                        "headersTruncated": False,
                        "messageId": "a841f56kqsbsh4033etkhps16na6s7h80dasfu81",
                        "source": "janedoe@example.com",
                        "timestamp": "1970-01-01T00:00:00Z",
                    }
                },
            }
        ]
    }


def get_extract_mine_data(file_name: str) -> bytes:
    target_dir = os.path.join(
        os.path.realpath(__file__), *["..", "test_gmac_quotas_extract.xlsx"]
    )
    target_dir = os.path.normpath(target_dir)

    with open(target_dir, "rb") as excel_file:
        file_content = excel_file.read()

    part = MIMEBase("application", EXCEL_TYPE)
    part.set_payload(file_content)
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{file_name}"')

    message = email.message_from_bytes(part.as_bytes())
    message_data = message.as_bytes()

    return message_data


@pytest.fixture(scope="function")
def mocked_itau_extract_mine_data() -> bytes:
    return get_extract_mine_data_itau("teste_relacao_contemplados_23052023.csv")


def get_extract_mine_data_itau(file_name: str) -> bytes:
    target_dir = os.path.join(
        os.path.realpath(__file__), *["..", "teste_relacao_contemplados_23052023.csv"]
    )
    target_dir = os.path.normpath(target_dir)

    with open(target_dir, "rb") as excel_file:
        file_content = excel_file.read()

    part = MIMEBase("text", CSV_TYPE)
    part.set_payload(file_content)
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{file_name}"')

    message = email.message_from_bytes(part.as_bytes())
    message_data = message.as_bytes()

    return message_data


@pytest.fixture(scope="function")
def mocked_gmac_extract_mine_data() -> bytes:
    return get_extract_mine_data("Base_Bazar_GMAC01062023.xlsx")


@pytest.fixture(scope="function")
def mocked_gmac_extract_mine_data_with_wrong_date() -> bytes:
    return get_extract_mine_data("Base_Bazar_GMAC0102.xlsx")


@pytest.fixture(scope="function")
def mocked_gmac_extract_mine_data_with_wrong_name() -> bytes:
    return get_extract_mine_data("Bazar_GMAC01062023.xlsx")


class TestExtractProcess:
    @staticmethod
    def test_process_extract_with_unprocessable_entity_if_required_key_is_missing(
        event: Dict[str, Any], context
    ) -> None:
        del event["Records"][0]["ses"]["mail"]["messageId"]
        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "Falha ao tentar obter chave do S3 no evento recebido: 'messageId'",
        )

    @staticmethod
    @patch("boto3.client")
    def test_process_extract_with_internal_error_if_boto3_raises_client_error(
        mock_boto_client: MagicMock, event: Dict[str, Any], context
    ) -> None:
        mock_boto_client.return_value = mock_boto_client
        mock_boto_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "403", "Message": "Access Denied"}}, "s3"
        )

        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "Falha ao tentar obter arquivo do S3: "
            "('An error occurred (403) when calling the s3 operation: Access Denied',)",
        )

    @staticmethod
    @patch("boto3.client")
    def test_process_extract_with_unprocessable_entity_if_file_date_is_wrong(
        mock_boto_client: MagicMock,
        mocked_gmac_extract_mine_data_with_wrong_date: bytes,
        event: Dict[str, Any],
        context,
    ) -> None:
        body = StreamingBody(
            BytesIO(mocked_gmac_extract_mine_data_with_wrong_date),
            len(mocked_gmac_extract_mine_data_with_wrong_date),
        )

        mocked_response = {"Body": body}

        mock_boto_client.return_value = mock_boto_client
        mock_boto_client.get_object.return_value = mocked_response

        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "Nome Base_Bazar_GMAC0102.xlsx não contém data nos "
            "formatos de data mapeados.",
        )

    @staticmethod
    @patch("boto3.client")
    def test_process_extract_with_internal_error_if_file_name_is_not_mapped(
        mock_boto_client: MagicMock,
        mocked_gmac_extract_mine_data_with_wrong_name: bytes,
        event: Dict[str, Any],
        context,
    ) -> None:
        body = StreamingBody(
            BytesIO(mocked_gmac_extract_mine_data_with_wrong_name),
            len(mocked_gmac_extract_mine_data_with_wrong_name),
        )

        mocked_response = {"Body": body}

        mock_boto_client.return_value = mock_boto_client
        mock_boto_client.get_object.return_value = mocked_response

        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "Extrato Bazar_GMAC01062023.xlsxPlanilha1 não possui repositório mapeado.",
        )

    @staticmethod
    @patch("boto3.client")
    def test_process_extract_with_internal_error_if_adm_is_not_mapped(
        mock_boto_client: MagicMock,
        mocked_gmac_extract_mine_data: bytes,
        event: Dict[str, Any],
        context,
    ) -> None:
        body = StreamingBody(
            BytesIO(mocked_gmac_extract_mine_data), len(mocked_gmac_extract_mine_data)
        )

        mocked_response = {"Body": body}

        mock_boto_client.return_value = mock_boto_client
        mock_boto_client.get_object.return_value = mocked_response

        event["Records"][0]["ses"]["mail"]["destination"][0] = "gm@example.com"

        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "ADM gm não possui extratos mapeados para armazenamento.",
        )

    @staticmethod
    @patch("boto3.client")
    def test_process_extract_with_internal_error_if_upload_to_s3_fails(
        mock_boto_client: MagicMock,
        mocked_gmac_extract_mine_data: bytes,
        event: Dict[str, Any],
        context,
    ) -> None:
        body = StreamingBody(
            BytesIO(mocked_gmac_extract_mine_data), len(mocked_gmac_extract_mine_data)
        )

        mocked_response = {"Body": body}

        mock_boto_client.return_value = mock_boto_client
        mock_boto_client.get_object.return_value = mocked_response
        mock_boto_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "403", "Message": "Access Denied"}}, "s3"
        )

        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "Falha ao tentar salvar extrato no S3: "
            "('An error occurred (403) when calling the s3 operation: Access Denied',)",
        )

    @staticmethod
    @patch("boto3.client")
    def test_process_extract_with_success(
        mock_boto_client: MagicMock,
        mocked_gmac_extract_mine_data: bytes,
        event: Dict[str, Any],
        context,
    ) -> None:
        body = StreamingBody(
            BytesIO(mocked_gmac_extract_mine_data), len(mocked_gmac_extract_mine_data)
        )

        mocked_response = {"Body": body}

        mock_boto_client.return_value = mock_boto_client
        mock_boto_client.get_object.return_value = mocked_response
        mock_boto_client.put_object.return_value = "OK"

        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_200_OK,
            "Extrato processado com sucesso.",
        )

    @staticmethod
    @patch("boto3.client")
    def test_process_extract_with_success_itau(
        mock_boto_client: MagicMock,
        mocked_itau_extract_mine_data: bytes,
        event_itau: Dict[str, Any],
        context,
    ) -> None:
        body = StreamingBody(
            BytesIO(mocked_itau_extract_mine_data), len(mocked_itau_extract_mine_data)
        )

        mocked_response = {"Body": body}

        mock_boto_client.return_value = mock_boto_client
        mock_boto_client.get_object.return_value = mocked_response
        mock_boto_client.put_object.return_value = "OK"

        response = lambda_handler(event_itau, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_200_OK,
            "Extrato processado com sucesso.",
        )
