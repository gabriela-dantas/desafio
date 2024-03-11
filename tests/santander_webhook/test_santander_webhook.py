import pytest
import json

from unittest.mock import patch, MagicMock
from typing import Dict, Any
from fastapi import status
from botocore.exceptions import ClientError

from santander_webhook.handler import lambda_handler
from common.repositories.dynamo.santander_webhook_event import (
    SantanderWebhookEventRepository,
)
from common.exceptions import InternalServerError, UnprocessableEntity


@pytest.fixture(scope="function")
def event() -> Dict[str, Any]:
    return {
        "body": """
            {
                "dispatchId": "26e17183-3d4e-4496-9ace-94ee4dc596c7",
                "platform": "Mercado Secundário",
                "action": "PROPOSAL_SELECTED",
                "eventDate": "2023-08-24T23:34:18.398Z",
                "payload": {
                  "shareId": "401c3d64-0777-47d4-9f53-6f9071629550",
                  "proposalId": "fb332012-dfa3-45d1-91a4-0f151f00d643"
                 }
            }
        """
    }


@pytest.fixture(scope="function")
def invalid_body_event(event) -> Dict[str, Any]:
    event["body"] = json.loads(event["body"])
    del event["body"]["payload"]
    event["body"] = json.dumps(event["body"])

    return event


class TestSantanderWebhook:
    @staticmethod
    def test_santander_invoke_with_validation_error_if_event_is_missing_fields(
        invalid_body_event: Dict[str, Any], context
    ) -> None:
        except_message = (
            "[{'loc': ('payload',), 'msg': 'field required', 'type': "
            "'value_error.missing'}]"
        )

        with pytest.raises(UnprocessableEntity) as error:
            lambda_handler(invalid_body_event, context)

        assert error.value.args[0] == except_message

    @staticmethod
    @patch("boto3.client")
    def test_santander_invoke_with_internal_error_if_invoke_raises_client_error(
        mock_boto_client: MagicMock, event: Dict[str, Any], context
    ) -> None:
        except_message = (
            "Não foi possível invocar Lambda md-cota-santander-flow-sandbox: ('An error "
            "occurred (403) when calling the lambda operation: Access Denied',)"
        )

        with pytest.raises(InternalServerError) as error:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = ClientError(
                {"Error": {"Code": "403", "Message": "Access Denied"}}, "lambda"
            )
            lambda_handler(event, context)

        assert error.value.args[0] == except_message

    @staticmethod
    @patch("boto3.client")
    def test_santander_invoke_with_success(
        mock_boto_client: MagicMock,
        event: Dict[str, Any],
        context,
    ) -> None:
        mock_boto_client.return_value = mock_boto_client
        mock_boto_client.invoke.return_value = ""
        body = json.loads(event["body"])

        response = lambda_handler(event, context)
        item = SantanderWebhookEventRepository().get_item(
            {
                "share_id": body["payload"]["shareId"],
                "event_date": "2023-08-24T23:34:18Z",
            }
        )

        assert (response["statusCode"], item) == (
            status.HTTP_200_OK,
            {
                "share_id": "401c3d64-0777-47d4-9f53-6f9071629550",
                "event_type": "PROPOSAL_SELECTED",
                "data": {
                    "payload": {
                        "externalId": None,
                        "proposalId": "fb332012-dfa3-45d1-91a4-0f151f00d643",
                    },
                    "platform": "Mercado Secundário",
                    "dispatchId": "26e17183-3d4e-4496-9ace-94ee4dc596c7",
                },
                "event_date": "2023-08-24T23:34:18Z",
            },
        )
