import pytest
import json

from unittest.mock import patch
from botocore.exceptions import ClientError

from sftp_file_handler.handler import lambda_handler


@pytest.fixture(scope="function")
def event() -> dict:
    return {
        "detail": {
            "requestParameters": {
                "bucketName": "sftp-archive-adms-sandbox",
                "key": "volkswagen/CotasDoClienteSPBAZAR20231010.xlsx"
            }
        }
    }


@pytest.fixture(scope="function")
def event_wrong_key() -> dict:
    return {
        "detail": {
            "requestParameters": {
                "bucketName": "sftp-archive-adms-sandbox",
                "key": "volkswagen/dadosBAZAR20231010.xlsx"
            }
        }
    }


class TestSftpFileHandler:
    @staticmethod
    def test_sftp_file_handler_with_key_error_if_event_has_no_s3_key(
        event: dict, context
    ) -> None:
        del event["detail"]["requestParameters"]["bucketName"]
        with patch("boto3.client"):
            with pytest.raises(KeyError) as error:
                lambda_handler(event, context)

        assert error.value.args[0] == 'bucketName'

    @staticmethod
    def test_sftp_file_handler_with_key_error_if_file_prefix_not_mapped(
            event_wrong_key: dict, context
    ) -> None:
        with patch("boto3.client"):
            response = lambda_handler(event_wrong_key, context)

        body = json.loads(response["body"])
        assert body == "'Prefixo do arquivo não mapeado: dadosbazar20231010.xlsx'"

    @staticmethod
    def test_sftp_file_handler_with_client_error_if_copy_in_bucket_is_denied(
            event: dict, context
    ) -> None:
        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.copy_object.side_effect = ClientError(
                {"Error": {"Code": "Access Denied"}}, "s3"
            )
            with pytest.raises(ClientError) as error:
                lambda_handler(event, context)

        assert (
                error.value.args[0]
                == 'An error occurred (Access Denied) when calling the s3 operation: Unknown'
        )

    @staticmethod
    def test_sftp_file_handler_with_client_error_if_glue_start_is_denied(
            event: dict, context
    ) -> None:
        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.copy_object.side_effect = None
            mock_boto_client.delete_object.side_effect = None
            mock_boto_client.start_job_run.side_effect = [
                ClientError(
                    {"Error": {"Code": "Access Denied"}}, "glue"
                )
            ]
            with pytest.raises(ClientError) as error:
                lambda_handler(event, context)

        assert (
                error.value.args[0]
                == 'An error occurred (Access Denied) when calling the glue operation: Unknown'
        )

    @staticmethod
    def test_sftp_file_handler_with_success_if_file_is_moved(
            event: dict, context
    ) -> None:
        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.copy_object.side_effect = None
            mock_boto_client.delete_object.side_effect = None

            mock_boto_client.start_job_run.return_value = {"JobRunId": "id_1"}
            mock_boto_client.get_job_run.return_value = {"status": "success"}
            response = lambda_handler(event, context)

        body = json.loads(response["body"])
        assert body == 'Arquivo movido para diretório correspondente no bucket.'
