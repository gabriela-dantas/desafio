import json
import pytest

from typing import Dict, Any
from requests_mock.mocker import Mocker
from fastapi import status

from requests.exceptions import Timeout, HTTPError, TooManyRedirects
from common.exceptions import UnprocessableEntity
from common.clients.cubees import CubeesClient
from common.exceptions import (
    Timeout as TimeoutException,
    TooManyRedirects as TooManyRedirectsException,
    InternalServerError,
)

from company_bond.handler import lambda_handler


@pytest.fixture(scope="function")
def event() -> Dict[str, Any]:
    return {
        "cnpj": "52.780.016/0001-68",
        "bond_type": "PARTNER",
        "representatives": [
            {
                "person_ext_code": "52.780.016/0001-68",
                "person_type": "LEGAL",
                "channel_type": "WHATSAPP",
                "administrator_code": "0000000002",
                "legal_person": {
                    "company_name": "Akatsuki",
                    "company_fantasy_name": "Akatsuki Unlimited",
                    "founding_date": "2000-12-31",
                },
                "addresses": [
                    {
                        "address": "Rua Jounin",
                        "address_2": "Próximo do campo de treino",
                        "street_number": "8001",
                        "district": "Hokage",
                        "zip_code": "22222-111",
                        "address_label": "Principal",
                        "address_category": "COMM",
                        "city": "São Paulo",
                        "state": "SP",
                    }
                ],
                "contacts": [
                    {
                        "contact_desc": "EMAIL Comercial",
                        "contact": "konoha.leaf@konoha.com",
                        "contact_category": "BUSINESS",
                        "contact_type": "EMAIL",
                        "preferred_contact": True,
                    },
                    {
                        "contact_desc": "TELEFONE Comercial",
                        "contact": "+5532984671893",
                        "contact_category": "BUSINESS",
                        "contact_type": "MOBILE",
                        "preferred_contact": False,
                    },
                ],
                "documents": [
                    {
                        "document_number": "22.333.888-9",
                        "expiring_date": "2030-12-01",
                        "person_document_type": "RG",
                    }
                ],
                "reactive": True,
            }
        ],
    }


@pytest.fixture(scope="function")
def event2() -> Dict[str, Any]:
    return {
        "cnpj": "02.180.016/0001-68",
        "bond_type": "PARTNER",
        "representatives": [
            {
                "person_ext_code": "52.780.016/0001-68",
                "person_type": "LEGAL",
                "channel_type": "WHATSAPP",
                "administrator_code": "0000000002",
                "legal_person": {
                    "company_name": "Akatsuki",
                    "company_fantasy_name": "Akatsuki Unlimited",
                    "founding_date": "2000-12-31",
                },
                "addresses": [
                    {
                        "address": "Rua Jounin",
                        "address_2": "Próximo do campo de treino",
                        "street_number": "8001",
                        "district": "Hokage",
                        "zip_code": "22222-111",
                        "address_label": "Principal",
                        "address_category": "COMM",
                        "city": "São Paulo",
                        "state": "SP",
                    }
                ],
                "contacts": [
                    {
                        "contact_desc": "EMAIL Comercial",
                        "contact": "konoha.leaf@konoha.com",
                        "contact_category": "BUSINESS",
                        "contact_type": "EMAIL",
                        "preferred_contact": True,
                    },
                    {
                        "contact_desc": "TELEFONE Comercial",
                        "contact": "+5532984671893",
                        "contact_category": "BUSINESS",
                        "contact_type": "MOBILE",
                        "preferred_contact": False,
                    },
                ],
                "documents": [
                    {
                        "document_number": "22.333.888-9",
                        "expiring_date": "2030-12-01",
                        "person_document_type": "RG",
                    }
                ],
                "reactive": True,
            },
            {
                "person_ext_code": "42.280.016/0001-68",
                "person_type": "LEGAL",
                "channel_type": "WHATSAPP",
                "administrator_code": "0000000001",
                "legal_person": {
                    "company_name": "Akatsuki",
                    "company_fantasy_name": "Akatsuki Unlimited",
                    "founding_date": "2000-12-31",
                },
                "addresses": [
                    {
                        "address": "Rua Santos",
                        "address_2": "Próximo do campo de treino",
                        "street_number": "8001",
                        "district": "Hokage",
                        "zip_code": "22222-111",
                        "address_label": "Principal",
                        "address_category": "COMM",
                        "city": "São Paulo",
                        "state": "SP",
                    }
                ],
                "contacts": [
                    {
                        "contact_desc": "EMAIL Comercial",
                        "contact": "konoha.leaf@konoha.com",
                        "contact_category": "BUSINESS",
                        "contact_type": "EMAIL",
                        "preferred_contact": True,
                    },
                    {
                        "contact_desc": "TELEFONE Comercial",
                        "contact": "+5532984671893",
                        "contact_category": "BUSINESS",
                        "contact_type": "MOBILE",
                        "preferred_contact": False,
                    },
                ],
                "documents": [
                    {
                        "document_number": "22.333.888-9",
                        "expiring_date": "2030-12-01",
                        "person_document_type": "RG",
                    }
                ],
                "reactive": True,
            },
        ],
    }


@pytest.fixture(scope="function")
def response_related_person() -> Dict[str, Any]:
    return {
        "created_at": "2023-09-08T09:30:05.017075",
        "modified_at": "2023-09-08T09:30:05.017075",
        "deleted_at": None,
        "created_by": 2,
        "modified_by": 2,
        "deleted_by": "None",
        "is_deleted": False,
        "modified_by_app": 2,
        "deleted_by_app": None,
        "created_by_app": 2,
        "related_person_id": 10,
        "related_person_type_id": 4,
        "legal_person_id": 470640,
        "person_id": 470537,
        "valid_from": "2023-09-08T09:30:05.017075",
        "valid_to": None,
    }


class TestCompanyBond:
    @staticmethod
    def test_company_bond_with_validation_error_if_event_is_missing_fields(
        event: Dict[str, Any], context
    ) -> None:
        del event["cnpj"]
        expected_message = "[{'loc': ('cnpj',), 'msg': 'field required', 'type': 'value_error.missing'}]"
        with pytest.raises(UnprocessableEntity) as error:
            lambda_handler(event, context)
        assert error.value.args[0] == expected_message

    @staticmethod
    def test_company_bond_with_error_if_url_create_customer_request_to_cubees_has_timeout(
        requests_mock: Mocker, event: Dict[str, Any], context
    ) -> None:
        expected_message = (
            "Requisição para https://cubees.bazar-sandbox.technology/customer "
            "gerou timeout após 30 segundos."
        )
        requests_mock.post(
            CubeesClient().create_customer_url, exc=Timeout("Created Timeout")
        )

        with pytest.raises(TimeoutException) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == expected_message

    @staticmethod
    def test_company_bond_with_error_if_url_related_person_request_to_cubees_has_timeout(
        requests_mock: Mocker, event: Dict[str, Any], context
    ) -> None:
        expected_message = (
            "Requisição para https://cubees.bazar-sandbox.technology/customer/related-person "
            "gerou timeout após 30 segundos."
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={"person_code": "000041234"},
        )
        requests_mock.post(
            CubeesClient().create_related_person_url, exc=Timeout("Created Timeout")
        )
        with pytest.raises(TimeoutException) as error:
            lambda_handler(event, context)
        assert error.value.args[0] == expected_message

    @staticmethod
    def test_company_bond_with_redirect_error_if_url_create_customer_to_cubees_has_many_redirects(
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Requisição para https://cubees.bazar-sandbox.technology/customer excedeu "
            "redirecionamentos."
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            exc=TooManyRedirects("Created TooManyRedirects"),
        )
        with pytest.raises(TooManyRedirectsException) as error:
            lambda_handler(event, context)
        assert error.value.args[0] == expected_message

    @staticmethod
    def test_company_bond_with_redirect_error_if_url_related_person_to_cubees_has_many_redirects(
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Requisição para https://cubees.bazar-sandbox.technology/customer/related-person excedeu "
            "redirecionamentos."
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={"person_code": "000041234"},
        )
        requests_mock.post(
            CubeesClient().create_related_person_url,
            exc=TooManyRedirects("Created TooManyRedirects"),
        )
        with pytest.raises(TooManyRedirectsException) as error:
            lambda_handler(event, context)
        assert error.value.args[0] == expected_message

    @staticmethod
    def test_company_bond_with_internal_server_error_if_url_create_customer_request_to_cubees_has_http_error(
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Requisição para https://cubees.bazar-sandbox.technology/customer "
            "gerou erro HTTP: ('Created HTTPError',)"
        )
        requests_mock.post(
            CubeesClient().create_customer_url, exc=HTTPError("Created HTTPError")
        )
        with pytest.raises(InternalServerError) as error:
            lambda_handler(event, context)
        assert error.value.args[0] == expected_message

    @staticmethod
    def test_company_bond_with_internal_server_error_if_url_related_person_request_to_cubees_has_http_error(
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Requisição para https://cubees.bazar-sandbox.technology/customer/related-person "
            "gerou erro HTTP: ('Created HTTPError',)"
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={"person_code": "000041234"},
        )
        requests_mock.post(
            CubeesClient().create_related_person_url, exc=HTTPError("Created HTTPError")
        )
        with pytest.raises(InternalServerError) as error:
            lambda_handler(event, context)
        assert error.value.args[0] == expected_message

    @staticmethod
    def test_company_bond_with_internal_server_error_if_not_create_customer_in_cubees(
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Requisição para criar cliente no Cubees não retornou 201: 500 - "
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
        with pytest.raises(InternalServerError) as error:
            lambda_handler(event, context)
        assert error.value.args[0] == expected_message

    @staticmethod
    def test_company_bond_with_internal_server_error_if_key_error_in_create_customer_in_cubees(
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Request ao Cubees não retornou person code: 201 - {'detail': '0123'}"
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={"detail": "0123"},
        )
        with pytest.raises(InternalServerError) as error:
            lambda_handler(event, context)
        assert error.value.args[0] == expected_message

    @staticmethod
    def test_company_bond_with_internal_server_error_if_not_create_related_person_in_cubees(
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
    ) -> None:
        expected_message = (
            "Requisição para relacionar cliente no Cubees não retornou 201: 500 - "
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={"person_code": "000041234"},
        )
        requests_mock.post(
            CubeesClient().create_related_person_url,
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
        with pytest.raises(InternalServerError) as error:
            lambda_handler(event, context)
        assert error.value.args[0] == expected_message

    @staticmethod
    def test_company_bond_with_success(
        requests_mock: Mocker,
        event: Dict[str, Any],
        response_related_person: Dict[str, Any],
        context,
    ) -> None:
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={"person_code": "000041234"},
        )
        requests_mock.post(
            CubeesClient().create_related_person_url,
            status_code=status.HTTP_201_CREATED,
            json={"json": response_related_person},
        )
        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_201_CREATED,
            "Invocação da lambda de vínculo para representantes executada com sucesso.",
        )

    @staticmethod
    def test_company_bond_with_success_with_two_representatives(
        requests_mock: Mocker,
        event2: Dict[str, Any],
        response_related_person: Dict[str, Any],
        context,
    ) -> None:
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={"person_code": "000041234"},
        )
        requests_mock.post(
            CubeesClient().create_related_person_url,
            status_code=status.HTTP_201_CREATED,
            json={"json": response_related_person},
        )
        response = lambda_handler(event2, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_201_CREATED,
            "Invocação da lambda de vínculo para representantes executada com sucesso.",
        )
