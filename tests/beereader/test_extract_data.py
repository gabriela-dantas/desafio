import pytest
import json

from unittest.mock import Mock, patch
from requests_mock.mocker import Mocker
from requests.exceptions import Timeout, TooManyRedirects
from typing import Dict, Any
from fastapi import status

from beereader.handler import lambda_handler
from beereader.constants import S3_DOWNLOAD_TIMEOUT


@pytest.fixture(scope="function")
def event() -> Dict[str, Any]:
    return {
        "quota_id": 149368,
        "person_code": "0000000010",
        "extract_url": "https://bazar-production-documents.s3.amazonaws.com/"
        "ah8ezgohc9oognjo2srbp9kjevxy?response"
        "-content-disposition=inline%3B%20filename%3D%2230570970.pdf%22%3B%20"
        "filename%2A%3DUTF-8%27%2730570970.pdf&response-content-type=application"
        "%2Fpdf&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=DUMMY"
        "us-east-DUMMYFaws4_request&X-Amz-Date=20230527T165920Z&X-Amz-Expires=300"
        "&X-Amz-SignedHeaders=host&X-Amz-Signature=DUMMY",
        "extract_created_at": "2023-05-24 16:52:20.306Z",
        "extract_filename": "extratocota533.pdf",
        "extract_s3_path": "s3://bazar-production-documents/ah8ezgohc9oognjo2srbp9kjevxy",
    }


class TestExtractData:
    bpm_endpoint = (
        "https://staging.bazardoconsorcio.com.br"
        "/api/internal/quotas/<quota_id>/invalid_extracts"
    )

    @classmethod
    def test_extract_data_with_validation_error_if_event_has_invalid_datetime(
        cls, requests_mock: Mocker, event: Dict[str, Any], context
    ) -> None:
        bpm_endpoint = cls.bpm_endpoint.replace("<quota_id>", str(event["quota_id"]))
        requests_mock.post(bpm_endpoint, status_code=200)

        event["extract_created_at"] = "2023-01-01T 10:00:00"
        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            {
                "error_description": [
                    {
                        "loc": ["extract_created_at"],
                        "msg": "Invalid datetime format for extract_created_at. "
                        "Must be: %Y-%m-%d %H:%M:%S or %Y-%m-%dT%H:%M:%S",
                        "type": "value_error",
                    }
                ],
                "bpm_response": "status code: 200",
            },
        )

    @classmethod
    @patch("requests.get")
    def test_extract_data_with_internal_error_if_request_to_s3_raises_timeout_error(
        cls,
        requests_get_mock: Mock,
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
    ) -> None:
        bpm_endpoint = cls.bpm_endpoint.replace("<quota_id>", str(event["quota_id"]))
        requests_mock.post(bpm_endpoint, status_code=200)

        requests_get_mock.side_effect = Timeout("Created Timeout")
        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            {
                "error_description": f"Requisição de download do extrato "
                f"gerou Timeout após {S3_DOWNLOAD_TIMEOUT} segundos.",
                "bpm_response": "status code: 200",
            },
        )

    @classmethod
    @patch("requests.get")
    def test_extract_data_with_internal_error_if_request_to_s3_raises_redirect_error(
        cls,
        requests_get_mock: Mock,
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
    ) -> None:
        bpm_endpoint = cls.bpm_endpoint.replace("<quota_id>", str(event["quota_id"]))
        requests_mock.post(bpm_endpoint, status_code=200)

        requests_get_mock.side_effect = TooManyRedirects("Created TooManyRedirects")
        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            {
                "error_description": "Requisição de download do extrato "
                "excedeu redirecionamentos.",
                "bpm_response": "status code: 200",
            },
        )

    @classmethod
    def test_extract_data_with_internal_error_if_s3_response_returns_403(
        cls, requests_mock: Mocker, event: Dict[str, Any], context
    ) -> None:
        requests_mock.get(event["extract_url"], status_code=403, text="Access Denied")

        bpm_endpoint = cls.bpm_endpoint.replace("<quota_id>", str(event["quota_id"]))
        requests_mock.post(bpm_endpoint, status_code=200)

        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            {
                "error_description": "URL de download do extrato retornou resposta "
                "inválida status: 403, text: Access Denied",
                "bpm_response": "status code: 200",
            },
        )

    @classmethod
    def test_extract_data_with_semantic_error_if_s3_response_returns_invalid_content(
        cls, requests_mock: Mocker, event: Dict[str, Any], context
    ) -> None:
        requests_mock.get(event["extract_url"], status_code=200, content=b"aaa")

        bpm_endpoint = cls.bpm_endpoint.replace("<quota_id>", str(event["quota_id"]))
        requests_mock.post(bpm_endpoint, status_code=200)

        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            {
                "error_description": "Falha na leitura do extrato obtido "
                "do S3: No /Root object! - Is this really a PDF?",
                "bpm_response": "status code: 200",
            },
        )

    @classmethod
    @patch("requests.post")
    def test_extract_data_with_semantic_error_and_webhook_timeout_if_pdf_is_not_mapped(
        cls,
        request_post_mock: Mock,
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
        not_mapped_pdf_content: bytes,
    ) -> None:
        requests_mock.get(
            event["extract_url"], status_code=200, content=not_mapped_pdf_content
        )

        request_post_mock.side_effect = Timeout("Created Error")

        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            {
                "error_description": "Extrato informadp possui formato não mapeado no Beereader.",
                "bpm_response": "Requisição ao Webhook gerou Timeout após 5 segundos.",
            },
        )

    @classmethod
    @patch("requests.post")
    def test_extract_data_with_semantic_error_and_webhook_redirects_if_pdf_is_not_mapped(
        cls,
        request_post_mock: Mock,
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
        not_mapped_pdf_content: bytes,
    ) -> None:
        requests_mock.get(
            event["extract_url"], status_code=200, content=not_mapped_pdf_content
        )

        request_post_mock.side_effect = TooManyRedirects("Created TooManyRedirects")

        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            {
                "error_description": "Extrato informadp possui formato não mapeado "
                "no Beereader.",
                "bpm_response": "Requisição Webhook excedeu redirecionamentos.",
            },
        )

    @classmethod
    def test_extract_data_with_created(
        cls,
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
        santander_1_pdf_content: bytes,
    ) -> None:
        requests_mock.get(
            event["extract_url"], status_code=200, content=santander_1_pdf_content
        )

        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_201_CREATED,
            {"quota_id": event["quota_id"]},
        )
