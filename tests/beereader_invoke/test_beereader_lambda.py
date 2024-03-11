import pytest
import json

from unittest.mock import patch, MagicMock
from typing import Dict, Any
from fastapi import status
from botocore.exceptions import ClientError

from beereader_invoke.handler import lambda_handler


@pytest.fixture(scope="function")
def event() -> Dict[str, Any]:
    return {
        "body": '{\n  "quota_id": 149368,\n  "person_code": "0000000010",\n  "extract_url": '
        '"https://bazar-production-documents.s3.amazonaws.com/ah8ezgohc9oognjo2srbp9kjevxy?response-content'
        "-disposition=inline%3B%20filename%3D%2230570970.pdf%22%3B%20filename%2A%3DUTF-8%27%2730570970.pdf"
        "&response-content-type=application%2Fpdf&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential"
        "=DUMMYus-east-DUMMYFaws4_request&X-Amz-Date=20230527T221333Z&X-Amz"
        "-Expires=300&X-Amz-SignedHeaders=host&X-Amz-Signature"
        '=d61cb12ea5b303236a5545b20e90720564a3b079ea5015507f09e7015003bdda",\n  "extract_created_at": '
        '"2023-05-24 16:52:20.306Z",\n  "extract_filename": "extratocota533.pdf",\n  "extract_s3_path": '
        '"s3://bazar-production-documents/ah8ezgohc9oognjo2srbp9kjevxy"\n}',
        "path": "/beereader",
        "httpMethod": "GET",
        "queryStringParameters": {"foo": "bar"},
        "pathParameters": {"proxy": "path/to/resource"},
        "stageVariables": {"baz": "qux"},
    }


@pytest.fixture(scope="function")
def invalid_body_event(event) -> Dict[str, Any]:
    event["body"] = json.loads(event["body"])
    event["body"]["extract_created_at"] = "2023-01-01"
    event["body"] = json.dumps(event["body"])

    return event


class TestExtractData:
    @staticmethod
    def test_beereader_lambda_with_validation_error_if_event_has_invalid_body(
        invalid_body_event: Dict[str, Any], context
    ) -> None:
        response = lambda_handler(invalid_body_event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body["detail"]) == (
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            [
                {
                    "loc": ["extract_created_at"],
                    "msg": "Invalid datetime format for extract_created_at. "
                    "Must be: %Y-%m-%d %H:%M:%S or %Y-%m-%dT%H:%M:%S",
                    "type": "value_error",
                }
            ],
        )

    @staticmethod
    @patch("boto3.client")
    def test_beereader_lambda_with_internal_error_if_invoke_raises_client_error(
        mock_boto_client: MagicMock, event: Dict[str, Any], context
    ) -> None:
        mock_boto_client.return_value = mock_boto_client
        mock_boto_client.invoke.side_effect = ClientError(
            {"Error": {"Code": "403", "Message": "Access Denied"}}, "lambda"
        )

        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            "Não foi possível invocar Lambda md-cota-beereader-sandbox: "
            "('An error occurred (403) when calling the lambda operation: Access Denied',)",
        )

    @staticmethod
    @patch("boto3.client")
    def test_beereader_lambda_with_success(
        mock_boto_client: MagicMock,
        event: Dict[str, Any],
        context,
    ) -> None:
        mock_boto_client.return_value = mock_boto_client
        mock_boto_client.invoke.return_value = ""

        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_200_OK,
            "Invocação do BeaReader feita com sucesso.",
        )
