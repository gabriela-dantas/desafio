import pytest

from unittest.mock import patch
from botocore.exceptions import ClientError
from typing import Dict, List
from botocore.response import StreamingBody
from io import BytesIO

from asset_portfolio_emails_sending.handler import lambda_handler, ADMEmailsSending
from simple_common.aws.secret_manager import SecretManager


@pytest.fixture(scope="function")
def email_secret() -> Dict[str, str]:
    emails = {
        f"recipient_{adm}": f"customer1@{adm}.com.br" for adm in ADMEmailsSending().adms
    }
    emails["sender"] = "bazar@bazardoconsorcio.com.br"
    return emails


@pytest.fixture(scope="function")
def templates_html() -> List[dict]:
    responses = []
    for adm in ADMEmailsSending().adms:
        body_encoded = f"<h1>Template {adm}<h1>".encode("utf-8")
        body = StreamingBody(BytesIO(body_encoded), len(body_encoded))
        responses.append({"Body": body})

    return responses


@pytest.fixture(scope="function")
def message_ids() -> List[dict]:
    return [{"MessageId": f"id{adm}"} for adm in ADMEmailsSending().adms]


class TestAssetPortfolioEmailsSending:
    @staticmethod
    def test_asset_portfolio_emails_sending_with_key_error_if_secret_is_missing(
        context,
    ) -> None:
        with patch("boto3.client"):
            with patch.object(SecretManager, "get_secret_value") as mock_secret:
                mock_secret.return_value = {}
                with pytest.raises(KeyError) as error:
                    lambda_handler({}, context)

        assert error.value.args[0] == "recipient_porto"

    @staticmethod
    def test_asset_portfolio_emails_sending_with_client_error_if_template_not_in_bucket(
        context, email_secret: Dict[str, str]
    ) -> None:
        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.get_object.side_effect = ClientError(
                {"Error": {"Code": "NoSuchKey"}}, "s3"
            )
            with patch.object(SecretManager, "get_secret_value") as mock_secret:
                mock_secret.return_value = email_secret
                with pytest.raises(ClientError) as error:
                    lambda_handler({}, context)

        assert (
            error.value.args[0]
            == "An error occurred (NoSuchKey) when calling the s3 operation: Unknown"
        )

    @staticmethod
    def test_asset_portfolio_emails_sending_with_client_error_if_ses_fails(
        context, email_secret: Dict[str, str], templates_html: List[dict]
    ) -> None:
        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.get_object.side_effect = templates_html
            mock_boto_client.send_email.side_effect = ClientError(
                {"Error": {"Code": "403", "Message": "Access Denied"}}, "ses"
            )
            with patch.object(SecretManager, "get_secret_value") as mock_secret:
                mock_secret.return_value = email_secret
                with pytest.raises(ClientError) as error:
                    lambda_handler({}, context)

        assert (
            error.value.args[0]
            == "An error occurred (403) when calling the ses operation: Access Denied"
        )

    @staticmethod
    def test_asset_portfolio_emails_sending_with_success_if_all_emails_are_sent(
        context,
        email_secret: Dict[str, str],
        templates_html: List[dict],
        message_ids: List[dict],
    ) -> None:
        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.get_object.side_effect = templates_html
            mock_boto_client.send_email.side_effect = message_ids
            with patch.object(SecretManager, "get_secret_value") as mock_secret:
                mock_secret.return_value = email_secret
                response = lambda_handler({}, context)

        assert response == {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": '"Emails enviados com sucesso!"',
        }
