import json
import pytest

from typing import Dict, Any
from requests_mock.mocker import Mocker
from fastapi import status

from requests.exceptions import Timeout, HTTPError, TooManyRedirects

from common.clients.consorciei import ConsorcieiClient
from common.exceptions import UnprocessableEntity
from common.clients.cubees import CubeesClient
from common.exceptions import (
    Timeout as TimeoutException,
    TooManyRedirects as TooManyRedirectsException,
    InternalServerError,
)
from life_proof_link_sender.handler import lambda_handler


@pytest.fixture(scope="function")
def event() -> Dict[str, Any]:
    return {"shareId": "1417727f-6791-4c52-8088-8e8aafa1cf3a"}


@pytest.fixture(scope="function")
def response_representatives() -> Dict[str, Any]:
    return {
        "data": [
            {
                "signerId": "de7490ce-683d-4ae6-87fc-4615ce3506f5",
                "cpf": "45958686852",
                "name": "Fernanda",
                "phone": "11951617467",
                "email": "fernanda.dias@consorciei.com.br",
                "occupation": "analista",
                "rg": "123456",
                "documentDispatcher": "ssp",
                "address": {
                    "addressId": "1c916285-6a99-4a29-8d9b-9d265211cfd9",
                    "address": "Rua Gaspar Fróis Machado",
                    "state": "SP",
                    "complement": "",
                    "district": "Jardim São Bento Novo",
                    "zip": "05872000",
                    "city": "São Paulo",
                    "number": "85",
                },
                "maritalStatus": "Solteiro(a)",
                "nationality": "Brasileiro(a)",
            }
        ]
    }


class TestLifeProofLinkSender:
    @staticmethod
    def test_life_proof_link_with_validation_error_if_event_is_missing_fields(
        event: Dict[str, Any], context
    ) -> None:
        del event["shareId"]
        expected_message = "[{'loc': ('shareId',), 'msg': 'field required', 'type': 'value_error.missing'}]"
        with pytest.raises(UnprocessableEntity) as error:
            lambda_handler(event, context)
        assert error.value.args[0] == expected_message

    @staticmethod
    def test_life_proof_link_with_error_if_url_get_representatives_to_consorciei_has_timeout(
        requests_mock: Mocker, event: Dict[str, Any], context
    ) -> None:
        expected_message = (
            "Requisição para "
            "https://api-sandbox.consorciei.com.br/secondary/shares/1417727f-6791-4c52-8088-8e8aafa1cf3a/signers "
            "gerou timeout após 60 segundos."
        )
        param = {"share_id": event["shareId"]}
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            exc=Timeout("Created Timeout"),
        )
        with pytest.raises(TimeoutException) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == expected_message

    @staticmethod
    def test_life_proof_link_with_error_if_url_get_customer_to_cubees_has_timeout(
        requests_mock: Mocker,
        event: Dict[str, Any],
        response_representatives: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Requisição para https://cubees.bazar-sandbox.technology/customer "
            "gerou timeout após 30 segundos."
        )
        param = {"share_id": event["shareId"]}
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            json=response_representatives,
        )
        requests_mock.get(
            CubeesClient().create_customer_url, exc=Timeout("Created Timeout")
        )
        with pytest.raises(TimeoutException) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == expected_message

    @staticmethod
    def test_life_proof_link_with_error_if_url_send_link_proof_life_to_consorciei_has_timeout(
        requests_mock: Mocker,
        event: Dict[str, Any],
        response_representatives: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Requisição para "
            "https://api-sandbox.consorciei.com.br/secondary/shares"
            "/1417727f-6791-4c52-8088-8e8aafa1cf3a/"
            "signers/de7490ce-683d-4ae6-87fc-4615ce3506f5/liveness "
            "gerou timeout após 60 segundos."
        )
        param = {"share_id": event["shareId"]}
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            json=response_representatives,
        )
        requests_mock.get(
            CubeesClient().create_customer_url,
            json={"person_type_data": {"token": "1234"}},
        )
        param = {
            "share_id": event["shareId"],
            "signer_id": response_representatives["data"][0]["signerId"],
        }
        requests_mock.post(
            ConsorcieiClient().send_link_proof_life_endpoint.format(**param),
            exc=Timeout("Created Timeout"),
        )
        with pytest.raises(TimeoutException) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == expected_message

    @staticmethod
    def test_life_proof_link_with_redirect_error_if_url_get_representatives_to_consorciei_has_many_redirects(
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Requisição para "
            "https://api-sandbox.consorciei.com.br/secondary/shares/1417727f-6791-4c52-8088-8e8aafa1cf3a/signers "
            "excedeu redirecionamentos."
        )
        param = {"share_id": event["shareId"]}
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            exc=TooManyRedirects("Created TooManyRedirects"),
        )
        with pytest.raises(TooManyRedirectsException) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == expected_message

    @staticmethod
    def test_life_proof_link_with_redirect_error_if_url_get_customer_to_cubees_has_many_redirects(
        requests_mock: Mocker,
        event: Dict[str, Any],
        response_representatives: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Requisição para https://cubees.bazar-sandbox.technology/customer "
            "excedeu redirecionamentos."
        )
        param = {"share_id": event["shareId"]}
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            json=response_representatives,
        )
        requests_mock.get(
            CubeesClient().create_customer_url,
            exc=TooManyRedirects("Created TooManyRedirects"),
        )
        with pytest.raises(TooManyRedirectsException) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == expected_message

    @staticmethod
    def test_life_proof_with_error_if_url_send_link_proof_life_to_consorciei_has_many_redirects(
        event: Dict[str, Any],
        requests_mock: Mocker,
        response_representatives: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Requisição para "
            "https://api-sandbox.consorciei.com.br/secondary/shares"
            "/1417727f-6791-4c52-8088-8e8aafa1cf3a/"
            "signers/de7490ce-683d-4ae6-87fc-4615ce3506f5/liveness "
            "excedeu redirecionamentos."
        )
        param = {"share_id": event["shareId"]}
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            json=response_representatives,
        )
        requests_mock.get(
            CubeesClient().create_customer_url,
            json={"person_type_data": {"token": "1234"}},
        )
        param = {
            "share_id": event["shareId"],
            "signer_id": response_representatives["data"][0]["signerId"],
        }

        requests_mock.post(
            ConsorcieiClient().send_link_proof_life_endpoint.format(**param),
            exc=TooManyRedirects("Created TooManyRedirects"),
        )
        with pytest.raises(TooManyRedirectsException) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == expected_message

    @staticmethod
    def test_life_proof_link_with_error_if_url_get_representatives_to_consorciei_has_http_error(
        requests_mock: Mocker, event: Dict[str, Any], context
    ) -> None:
        expected_message = (
            "Requisição para "
            "https://api-sandbox.consorciei.com.br/secondary/shares/1417727f-6791-4c52-8088-8e8aafa1cf3a/signers "
            "gerou erro HTTP: ('Created HTTPError',)"
        )
        param = {"share_id": event["shareId"]}
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            exc=HTTPError("Created HTTPError"),
        )
        with pytest.raises(InternalServerError) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == expected_message

    @staticmethod
    def test_life_proof_link_with_error_if_url_get_customer_to_cubees_has_http_error(
        requests_mock: Mocker,
        event: Dict[str, Any],
        response_representatives: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Requisição para https://cubees.bazar-sandbox.technology/customer "
            "gerou erro HTTP: ('Created HTTPError',)"
        )
        param = {"share_id": event["shareId"]}
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            json=response_representatives,
        )
        requests_mock.get(
            CubeesClient().create_customer_url, exc=HTTPError("Created HTTPError")
        )
        with pytest.raises(InternalServerError) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == expected_message

    @staticmethod
    def test_life_proof_link_with_error_if_url_send_link_proof_life_to_consorciei_has_http_error(
        requests_mock: Mocker,
        event: Dict[str, Any],
        response_representatives: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Requisição para "
            "https://api-sandbox.consorciei.com.br/secondary/shares"
            "/1417727f-6791-4c52-8088-8e8aafa1cf3a/"
            "signers/de7490ce-683d-4ae6-87fc-4615ce3506f5/liveness "
            "gerou erro HTTP: ('Created HTTPError',)"
        )
        param = {"share_id": event["shareId"]}
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            json=response_representatives,
        )
        requests_mock.get(
            CubeesClient().create_customer_url,
            json={"person_type_data": {"token": "1234"}},
        )
        param = {
            "share_id": event["shareId"],
            "signer_id": response_representatives["data"][0]["signerId"],
        }
        requests_mock.post(
            ConsorcieiClient().send_link_proof_life_endpoint.format(**param),
            exc=HTTPError("Created HTTPError"),
        )
        with pytest.raises(InternalServerError) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == expected_message

    @staticmethod
    def test_life_proof_link_with_error_if_url_get_representatives_to_consorciei_has_internal_server_error(
        requests_mock: Mocker, event: Dict[str, Any], context
    ) -> None:
        expected_message = (
            "Requisição para obter lista de representantes não retornou 200: 500 - "
        )
        param = {"share_id": event["shareId"]}
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
        with pytest.raises(InternalServerError) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == expected_message

    @staticmethod
    def test_life_proof_link_with_error_if_url_get_customer_to_cubees_has_internal_server_error(
        requests_mock: Mocker,
        event: Dict[str, Any],
        response_representatives: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Requisição para criar cliente no Cubees não retornou 200: 500 - "
        )
        param = {"share_id": event["shareId"]}
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            json=response_representatives,
        )
        requests_mock.get(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
        with pytest.raises(InternalServerError) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == expected_message

    @staticmethod
    def test_life_proof_link_with_error_if_url_send_link_proof_life_to_consorciei_has_internal_server_error(
        requests_mock: Mocker,
        event: Dict[str, Any],
        response_representatives: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Requisição para enviar link de prova de vida não retornou 201: 500 - "
        )
        param = {"share_id": event["shareId"]}
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            json=response_representatives,
        )
        requests_mock.get(
            CubeesClient().create_customer_url,
            json={"person_type_data": {"token": "1234"}},
        )
        param = {
            "share_id": event["shareId"],
            "signer_id": response_representatives["data"][0]["signerId"],
        }
        requests_mock.post(
            ConsorcieiClient().send_link_proof_life_endpoint.format(**param),
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
        with pytest.raises(InternalServerError) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == expected_message

    @staticmethod
    def test_list_proof_with_error_if_key_error(
        requests_mock: Mocker,
        event: Dict[str, Any],
        response_representatives: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "A chave token não foi encontrada na resposta da API do cubees"
        )
        param = {"share_id": event["shareId"]}
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            json=response_representatives,
        )
        requests_mock.get(CubeesClient().create_customer_url, json={"data": "data"})

        with pytest.raises(InternalServerError) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == expected_message

    @staticmethod
    def test_life_proof_link_with_success(
        requests_mock: Mocker,
        event: Dict[str, Any],
        response_representatives: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Invocação da lambda de link de prova de vida executada com sucesso."
        )
        param = {"share_id": event["shareId"]}
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            json=response_representatives,
        )
        requests_mock.get(
            CubeesClient().create_customer_url,
            json={"person_type_data": {"token": "1234"}},
        )
        param = {
            "share_id": event["shareId"],
            "signer_id": response_representatives["data"][0]["signerId"],
        }

        requests_mock.post(
            ConsorcieiClient().send_link_proof_life_endpoint.format(**param),
            json={"data": "sucesso"},
        )

        response = lambda_handler(event, context)
        body = json.loads(response["body"])
        assert (response["statusCode"], body) == (
            status.HTTP_201_CREATED,
            expected_message,
        )
