from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
import pytest
import os

from unittest.mock import patch
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from io import BytesIO

from email_file_extraction.handler import lambda_handler


@pytest.fixture(scope="function")
def event() -> dict:
    return {
        "Records": [
            {
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "bucket": {
                        "name": "cota-adms-sandbox",
                        "arn": "arn:{partition}:s3:::mybucket",
                    },
                    "object": {
                        "key": "all-adms-emails/ij9o5b85hm5dtth5cm4gtgk24j2n4jjla2u2s401",
                        "size": 1024,
                    },
                }
            }
        ]
    }


@pytest.fixture(scope="function")
def message_with_no_adm() -> dict:
    msg = MIMEText("")
    msg["Subject"] = "Re: Cotas"
    msg["From"] = "customer@sanatnder.com.br"
    msg["To"] = "operacoes@bazardoconsorcio.com.br"
    msg_bytes = msg.as_bytes()

    body = StreamingBody(BytesIO(msg_bytes), len(msg_bytes))
    return {"Body": body}


@pytest.fixture(scope="function")
def message_with_no_file() -> dict:
    msg = MIMEText("")
    msg["Subject"] = "Re: Cotas na Carteira - Mensal - 09/11/2023 - SANTANDER"
    msg["From"] = "customer@santander.com.br"
    msg["To"] = "operacoes@bazardoconsorcio.com.br"
    msg_bytes = msg.as_bytes()

    body = StreamingBody(BytesIO(msg_bytes), len(msg_bytes))
    return {"Body": body}


@pytest.fixture(scope="function")
def message_with_txt_file() -> dict:
    msg = MIMEMultipart("")
    msg["Subject"] = "Re: Cotas na Carteira - Mensal - 09/11/2023 - SANTANDER"
    msg["From"] = "customer@santander.com.br"
    msg["To"] = "operacoes@bazardoconsorcio.com.br"

    part = MIMEBase("application", "json")
    part.set_payload(b'{"a": 1}')
    part.add_header("Content-Disposition", 'attachment; filename="file_test.txt"')
    msg.attach(part)
    msg_bytes = msg.as_bytes()

    body = StreamingBody(BytesIO(msg_bytes), len(msg_bytes))
    return {"Body": body}


def message_with_csv(subject: str) -> dict:
    msg = MIMEMultipart("")
    msg["Subject"] = f"Re: {subject} - Mensal - 09/11/2023 - SANTANDER"
    msg["From"] = "customer@santander.com.br"
    msg["To"] = "operacoes@bazardoconsorcio.com.br"

    target_dir = os.path.join(
        os.path.realpath(__file__), *["..", "test_cotas_santander.csv"]
    )
    target_dir = os.path.normpath(target_dir)

    with open(target_dir, "rb") as csv_file:
        file_content = csv_file.read()

    part = MIMEBase("application", "vnd.ms-excel")
    part.set_payload(file_content)
    part.add_header(
        "Content-Disposition", 'attachment; filename="test_cotas_santander.csv"'
    )
    msg.attach(part)
    msg_bytes = msg.as_bytes()

    body = StreamingBody(BytesIO(msg_bytes), len(msg_bytes))
    return {"Body": body}


@pytest.fixture(scope="function")
def message_subject_not_mapped() -> dict:
    return message_with_csv("Dados")


@pytest.fixture(scope="function")
def correct_message() -> dict:
    return message_with_csv("Cotas na Carteira")


class TestEmailFileExtraction:
    @staticmethod
    def test_email_file_extraction_with_key_error_if_event_has_no_s3_key(
        event: dict, context
    ) -> None:
        del event["Records"][0]["s3"]
        with patch("boto3.client"):
            with pytest.raises(KeyError) as error:
                lambda_handler(event, context)

        assert error.value.args[0] == "s3"

    @staticmethod
    def test_email_file_extraction_with_client_error_if_template_not_in_bucket(
        event: dict, context
    ) -> None:
        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.get_object.side_effect = ClientError(
                {"Error": {"Code": "NoSuchKey"}}, "s3"
            )
            with pytest.raises(ClientError) as error:
                lambda_handler(event, context)

        assert (
            error.value.args[0]
            == "An error occurred (NoSuchKey) when calling the s3 operation: Unknown"
        )

    @staticmethod
    def test_email_file_extraction_with_attribute_error_if_message_subject_has_no_adm(
        event: dict, context, message_with_no_adm: dict
    ) -> None:
        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.get_object.return_value = message_with_no_adm
            with pytest.raises(AttributeError) as error:
                lambda_handler(event, context)

        assert (
            error.value.args[0]
            == "Nnehum assunto ou ADM encontrados no formato esperado, "
            "no subject recebido: Re: Cotas"
        )

    @staticmethod
    def test_email_file_extraction_with_error_if_message_has_no_file(
        event: dict, context, message_with_no_file: dict
    ) -> None:
        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.get_object.return_value = message_with_no_file
            with pytest.raises(AttributeError) as error:
                lambda_handler(event, context)

        assert (
            error.value.args[0]
            == "Nenhum arquivo encontrado em anexo nos formatos esperados."
        )

    @staticmethod
    def test_email_file_extraction_with_error_if_message_file_is_not_expected(
        event: dict, context, message_with_txt_file: dict
    ) -> None:
        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.get_object.return_value = message_with_txt_file
            with pytest.raises(AttributeError) as error:
                lambda_handler(event, context)

        assert (
            error.value.args[0]
            == "Nenhum arquivo encontrado em anexo nos formatos esperados."
        )

    @staticmethod
    def test_email_file_extraction_with_key_error_if_message_subject_not_mapped(
        event: dict, context, message_subject_not_mapped: dict
    ) -> None:
        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.get_object.return_value = message_subject_not_mapped
            with pytest.raises(KeyError) as error:
                lambda_handler(event, context)

        assert error.value.args[0] == "Subject do e-mail não mapeado: dados"

    @staticmethod
    def test_email_file_extraction_with_client_error_if_writing_in_bucket_is_denied(
            event: dict, context, correct_message: dict
    ) -> None:
        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.get_object.return_value = correct_message
            mock_boto_client.put_object.side_effect = ClientError(
                {"Error": {"Code": "Access Denied"}}, "s3"
            )
            with pytest.raises(ClientError) as error:
                lambda_handler(event, context)

        assert (
                error.value.args[0]
                == 'An error occurred (Access Denied) when calling the s3 operation: Unknown'
        )

    @staticmethod
    def test_email_file_extraction_with_client_error_if_glue_start_is_denied(
            event: dict, context, correct_message: dict
    ) -> None:
        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.get_object.return_value = correct_message
            mock_boto_client.put_object.return_value = None
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
    def test_email_file_extraction_with_success_if_file_is_written(
            event: dict, context, correct_message: dict
    ) -> None:
        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.get_object.return_value = correct_message
            mock_boto_client.put_object.return_value = None

            mock_boto_client.start_job_run.return_value = {"JobRunId": "id_1"}
            mock_boto_client.get_job_run.return_value = {"status": "success"}
            response = lambda_handler(event, context)

            assert response == {
                'body': '"Arquivos escritos com sucesso no bucket!"',
                'headers': {'Content-Type': 'application/json'},
                'statusCode': 200
            }
