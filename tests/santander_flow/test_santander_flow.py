import pytest
import json
import io

from datetime import date, datetime
from unittest.mock import patch
from requests_mock.mocker import Mocker
from requests.exceptions import Timeout
from typing import Dict, Any
from fastapi import status
from botocore.exceptions import ClientError
from botocore.response import StreamingBody

from santander_flow.handler import lambda_handler
from common.clients.bpm import BPMClient
from common.clients.consorciei import ConsorcieiClient
from common.clients.cubees import CubeesClient
from common.repositories.md_cota.group import GroupRepository
from common.repositories.md_cota.quota import QuotaRepository
from common.repositories.md_cota.quota_view import QuotaViewRepository
from common.exceptions import UnprocessableEntity, EntityNotFound, InternalServerError


@pytest.fixture(scope="class")
def create_quota_data() -> None:
    group = {
        "group_id": 1,
        "group_code": "20165",
        "group_deadline": 24,
        "group_start_date": date(2023, 1, 1),
        "group_closing_date": date(2025, 1, 1),
        "next_adjustment_date": date(2023, 9, 1),
        "current_assembly_date": date(2023, 8, 1),
        "current_assembly_number": 8,
        "administrator_id": 2,
    }

    GroupRepository().create(group)

    quota = {
        "quota_id": 1,
        "quota_code": "BZ0000022",
        "quota_number": "212",
        "check_digit": "2",
        "external_reference": "asfbgasrgfvasdfvbasd11",
        "total_installments": 24,
        "version_id": "NA",
        "contract_number": "51000135",
        "is_contemplated": False,
        "is_multiple_ownership": False,
        "administrator_fee": 14.50,
        "fund_reservation_fee": 2.00,
        "acquisition_date": date(2023, 1, 1),
        "info_date": date(2023, 1, 1),
        "quota_status_type_id": 1,
        "administrator_id": 2,
        "group_id": 1,
        "quota_origin_id": 1,
        "quota_person_type_id": 1,
    }

    QuotaRepository().create(quota)

    quota_view_1 = {
        "quota_id": 1,
        "quota_code": "BZ0000022",
        "quota_number": "212",
        "check_digit": "2",
        "external_reference": "asfbgasrgfvasdfvbasd11",
        "total_installments": 24,
        "version_id": "NA",
        "contract_number": "51000135",
        "is_contemplated": False,
        "is_multiple_ownership": False,
        "administrator_fee": 14.50,
        "fund_reservation_fee": 2.00,
        "acquisition_date": date(2023, 1, 1),
        "quota_info_date": date(2023, 1, 1),
        "quota_status_type_code": "ACTIVE",
        "quota_status_type_desc": "ACTIVE QUOTA",
        "quota_status_cat_code": "ACTIVE",
        "quota_status_cat_desc": "ACTIVE QUOTA",
        "quota_origin_code": "ADMORGN",
        "quota_origin_desc": "SEND BY ADMINISTRATOR",
        "quota_created_at": datetime.now(),
        "quota_modified_at": datetime.now(),
        "group_code": "20164",
        "group_deadline": 24,
        "group_start_date": date(2023, 1, 1),
        "group_closing_date": date(2025, 1, 1),
        "next_adjustment_date": date(2023, 9, 1),
        "grp_current_assembly_date": date(2023, 8, 1),
        "grp_current_assembly_number": 8,
        "chosen_bid": 25,
        "max_bid_occurrences_perc": 49,
        "bid_calculation_date": date(2023, 7, 1),
        "administrator_code": "0000000234",
        "administrator_desc": "SANTANDER ADM. CONS. LTDA",
        "old_quota_number": 30,
        "old_digit": 1,
        "installments_paid_number": 10,
        "per_mutual_fund_paid": 40.43,
        "per_adm_paid": 11.46,
        "per_adm_to_pay": 6.54,
        "adjustment_date": date(2023, 6, 1),
        "current_assembly_date": date(2023, 1, 1),
        "current_assembly_number": 10,
        "asset_description": "007731 86.64 onix 1.0 - mais facil",
        "asset_value": 66095.00,
        "quota_history_info_date": date(2023, 1, 1),
        "asset_type_code": "CAR",
        "asset_type_desc": "CAR",
        "vacancies": 50,
        "vacancies_info_date": date(2023, 7, 1),
    }

    QuotaViewRepository().create(quota_view_1, True)


@pytest.fixture(scope="function")
def event_proposal_selected() -> Dict[str, Any]:
    return {
        "dispatchId": "26e17183-3d4e-4496-9ace-94ee4dc596c7",
        "platform": "Mercado Secundário",
        "action": "PROPOSAL_SELECTED",
        "eventDate": "2023-08-24T23:34:18",
        "payload": {
            "shareId": "401c3d64-0777-47d4-9f53-6f9071629550",
            "proposalId": "fb332012-dfa3-45d1-91a4-0f151f00d643",
        },
    }


@pytest.fixture(scope="function")
def event_contract_signed_by_seller() -> Dict[str, Any]:
    return {
        "dispatchId": "36e17183-3d4e-4496-9ace-94ee4dc596c7",
        "platform": "Mercado Secundário",
        "action": "CONTRACT_SIGNED_BY_SELLER",
        "eventDate": "2023-08-25T23:34:18",
        "payload": {
            "shareId": "401c3d64-0777-47d4-9f53-6f9071629550",
            "proposalId": "fb332012-dfa3-45d1-91a4-0f151f00d643",
        },
    }


@pytest.fixture(scope="function")
def quota_detail_pj() -> Dict[str, Any]:
    return {
        "data": {
            "shareId": "401c3d64-0777-47d4-9f53-6f9071629550",
            "administratorId": "legacyid-0000-0000-0000-000000000076",
            "number": "658",
            "contract": "51000135",
            "situation": "canceled",
            "isAwarded": False,
            "goodValue": 100194.06,
            "group": {"number": "552", "endGroupDate": "2024-01-03T03:00:00.000Z"},
            "endGroupValue": 1673.64,
            "isPj": True,
            "totalPaid": 3667.1025959999993,
            "cancelationFine": 418.8111708,
            "admFeeTotalValue": 1573.0467419999998,
            "commonFundPaid": 2092.05,
            "playerName": "Bazar",
            "fundCnpj": "42401957000190",
        }
    }


@pytest.fixture(scope="function")
def quota_detail_pf() -> Dict[str, Any]:
    return {
        "data": {
            "shareId": "401c3d64-0777-47d4-9f53-6f9071629550",
            "contract": "51000135",
            "isPj": False,
            "group": "540",
            "number": "562",
            "commonFundPaid": 1620.6,
            "status": "waiting_client_signature",
            "playerName": "Bazar",
            "fundCnpj": "42401957000190",
            "bestProposal": 1000,
            "proposalSelected": True,
        }
    }


@pytest.fixture(scope="function")
def company_detail() -> Dict[str, Any]:
    return {
        "data": {
            "companyId": "fe68b4d7-ab8f-4835-a6f8-2c93113fb873",
            "cnpj": "01122014000140",
            "name": "Konoha LTDA",
            "email": "konoha.leaf@consorciei.com.br",
            "phone": "7121523657",
            "address": {
                "addressId": "e2ca8a7b-1103-4d44-9032-d4c0292ebd7e",
                "address": "Rua Sebastião Miguel da Silva",
                "number": "05",
                "complement": "",
                "district": "Mirna",
                "city": "São Paulo",
                "state": "SP",
                "zip": "08280350",
            },
        }
    }


@pytest.fixture(scope="function")
def company_detail_no_address_and_contact() -> Dict[str, Any]:
    return {
        "data": {
            "companyId": "fe68b4d7-ab8f-4835-a6f8-2c93113fb873",
            "cnpj": "01122014000140",
            "name": "Konoha LTDA",
            "address": {},
        }
    }


@pytest.fixture(scope="function")
def company_detail_no_cnpj_and_name() -> Dict[str, Any]:
    return {
        "data": {
            "companyId": "fe68b4d7-ab8f-4835-a6f8-2c93113fb873",
            "cnpj": "",
            "name": "",
            "address": {},
        }
    }


@pytest.fixture(scope="function")
def representatives_list() -> Dict[str, Any]:
    return {
        "data": [
            {
                "signerId": "de7490ce-683d-4ae6-87fc-4615ce3506f5",
                "cpf": "45958686852",
                "name": "Fernanda",
                "phone": "98969060409",
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


@pytest.fixture(scope="function")
def representatives_list_without_contacts() -> Dict[str, Any]:
    return {
        "data": [
            {
                "signerId": "de7490ce-683d-4ae6-87fc-4615ce3506f5",
                "cpf": "45958686852",
                "name": "Fernanda",
                "phone": None,
                "email": None,
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


@pytest.fixture(scope="function")
def representatives_list_no_phone_and_rg() -> Dict[str, Any]:
    return {
        "data": [
            {
                "signerId": "de7490ce-683d-4ae6-87fc-4615ce3506f5",
                "cpf": "45958686852",
                "name": "Fernanda",
                "email": "fernanda.dias@consorciei.com.br",
                "occupation": "analista",
                "rg": None,
                "documentDispatcher": None,
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


@pytest.fixture(scope="function")
def representatives_list_no_cpf_and_name() -> Dict[str, Any]:
    return {
        "data": [
            {
                "signerId": "de7490ce-683d-4ae6-87fc-4615ce3506f5",
                "cpf": "",
                "name": "",
                "email": "fernanda.dias@consorciei.com.br",
                "occupation": "analista",
                "rg": None,
                "documentDispatcher": None,
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


@pytest.fixture(scope="function")
def representatives_list_no_phone_and_rg() -> Dict[str, Any]:
    return {
        "data": [
            {
                "signerId": "de7490ce-683d-4ae6-87fc-4615ce3506f5",
                "cpf": "45958686852",
                "name": "Fernanda",
                "email": "fernanda.dias@consorciei.com.br",
                "occupation": "analista",
                "rg": None,
                "documentDispatcher": None,
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


@pytest.fixture(scope="function")
def invoke_mocked_error_response() -> Dict[str, Any]:
    response_payload = {
        "errorMessage": "some error",
        "errorType": "InternalServerError",
    }

    encoded_payload = json.dumps(response_payload).encode()

    body = StreamingBody(io.BytesIO(encoded_payload), len(encoded_payload))
    mocked_response = {"StatusCode": 200, "FunctionError": "Unhandled", "Payload": body}

    return mocked_response


@pytest.fixture(scope="function")
def invoke_mocked_success_response() -> Dict[str, Any]:
    response_payload = {
        "statusCode": 201,
        "headers": {"Content-Type": "application/json"},
        "body": [{"person_code": "0000000001"}],
    }

    encoded_payload = json.dumps(response_payload).encode()

    body = StreamingBody(io.BytesIO(encoded_payload), len(encoded_payload))
    mocked_response = {"StatusCode": 200, "Payload": body}

    return mocked_response


@pytest.mark.usefixtures("create_quota_data")
class TestSantanderFlowProposalSelected:
    @staticmethod
    def test_santander_flow_with_validation_error_if_event_is_missing_fields_and_invalid_date(
            event_proposal_selected: Dict[str, Any], context
    ) -> None:
        except_message = (
            "[{'loc': ('action',), 'msg': 'field required', 'type': "
            "'value_error.missing'}, {'loc': ('eventDate',), 'msg': 'eventDate não "
            "corresponde a um formato de data ISO.', 'type': 'value_error'}]"
        )

        del event_proposal_selected["action"]
        event_proposal_selected["eventDate"] = "23-08-24T23:34:18"

        with pytest.raises(UnprocessableEntity) as error:
            lambda_handler(event_proposal_selected, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_quota_detail_error_if_request_to_get_quota_has_timeout(
            requests_mock: Mocker, event_proposal_selected: Dict[str, Any], context
    ) -> None:
        param = {"share_id": event_proposal_selected["payload"]["shareId"]}
        except_message = (
            "Máximo de 2 tentativas excedido ao chamar API: {'1': 'Requisição para "
            "https://api-sandbox.consorciei.com.br/secondary/shares/401c3d64-0777-47d4-9f53-6f9071629550 "
            "gerou timeout após 60 segundos.', '2': 'Requisição para "
            "https://api-sandbox.consorciei.com.br/secondary/shares/401c3d64-0777-47d4-9f53-6f9071629550 "
            "gerou timeout após 60 segundos.'}"
        )

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )

        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            exc=Timeout("Created Timeout"),
        )

        with patch("boto3.client"):
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_proposal_selected, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_quota_detail_error_if_response_is_not_ok(
            requests_mock: Mocker, event_proposal_selected: Dict[str, Any], context
    ) -> None:
        except_message = (
            "Máximo de 2 tentativas excedido ao chamar API: {'1': 'Requisição para obter "
            'cota não retornou 200: 400 - {"error": "SHARE_NOT_FOUND"}\', \'2\': '
            '\'Requisição para obter cota não retornou 200: 400 - {"error": '
            '"SHARE_NOT_FOUND"}\'}'
        )
        param = {"share_id": event_proposal_selected["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_400_BAD_REQUEST,
            json={"error": "SHARE_NOT_FOUND"},
        )

        with patch("boto3.client"):
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_proposal_selected, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_quota_detail_error_if_response_has_unexpected_format(
            requests_mock: Mocker,
            event_proposal_selected: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
    ) -> None:
        except_message = (
            "Cota recebida da API Consorciei não possui os campos esperados: 'data'"
        )
        param = {"share_id": event_proposal_selected["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj["data"],
        )

        with patch("boto3.client"):
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_proposal_selected, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_critical_error_if_quota_not_in_db(
            requests_mock: Mocker,
            event_proposal_selected: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
    ) -> None:
        except_message = "Cota Santander com contrato 1 não existe no MD Cota!"
        param = {"share_id": event_proposal_selected["payload"]["shareId"]}
        quota_detail_pj["data"]["contract"] = "1"

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )

        with patch("boto3.client"):
            with pytest.raises(EntityNotFound) as error:
                lambda_handler(event_proposal_selected, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_company_detail_error_if_response_is_not_ok(
            requests_mock: Mocker,
            event_proposal_selected: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
    ) -> None:
        except_message = (
            "Máximo de 2 tentativas excedido ao chamar API: {'1': 'Requisição para obter "
            'detalhes da empresa não retornou 200: 400 - {"error": "SHARE_NOT_FOUND"}\', '
            "'2': 'Requisição para obter detalhes da empresa não retornou 200: 400 - "
            '{"error": "SHARE_NOT_FOUND"}\'}'
        )
        param = {"share_id": event_proposal_selected["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )
        requests_mock.get(
            ConsorcieiClient().company_detail_endpoint.format(**param),
            status_code=status.HTTP_400_BAD_REQUEST,
            json={"error": "SHARE_NOT_FOUND"},
        )

        with patch("boto3.client"):
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_proposal_selected, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_company_detail_error_if_response_has_unexpected_format(
            requests_mock: Mocker,
            event_proposal_selected: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
            company_detail: Dict[str, Any],
    ) -> None:
        except_message = (
            "Empresa recebida da API Consorciei não possuem os campos esperados para "
            "mapear cliente: 'data'"
        )
        param = {"share_id": event_proposal_selected["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )
        requests_mock.get(
            ConsorcieiClient().company_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=company_detail["data"],
        )

        with patch("boto3.client"):
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_proposal_selected, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_customer_creation_error_if_invoke_raises_client_error(
            requests_mock: Mocker,
            event_proposal_selected: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
            company_detail: Dict[str, Any],
    ) -> None:
        except_message = (
            "Não foi possível invocar Lambda md-cota-cubees-customer-sandbox: "
            "('An error occurred (403) when calling the lambda operation: Access "
            "Denied',)"
        )
        param = {"share_id": event_proposal_selected["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )
        requests_mock.get(
            ConsorcieiClient().company_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=company_detail,
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = ClientError(
                {"Error": {"Code": "403", "Message": "Access Denied"}}, "lambda"
            )
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_proposal_selected, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_customer_creation_error_if_invoke_response_has_error(
            requests_mock: Mocker,
            event_proposal_selected: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
            company_detail: Dict[str, Any],
            invoke_mocked_error_response: Dict[str, Any],
    ) -> None:
        except_message = (
            "Lambda md-cota-cubees-customer-sandbox foi invocada mas retornou erro: "
            '{"errorMessage": "some error", "errorType": "InternalServerError"}'
        )
        param = {"share_id": event_proposal_selected["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )
        requests_mock.get(
            ConsorcieiClient().company_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=company_detail,
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.return_value = invoke_mocked_error_response
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_proposal_selected, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_quota_creation_error_if_invoke_raises_client_error(
            requests_mock: Mocker,
            event_proposal_selected: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
            company_detail: Dict[str, Any],
            invoke_mocked_success_response: Dict[str, Any],
    ) -> None:
        except_message = (
            "Não foi possível invocar Lambda md-oferta-quota-creation-invoke-sandbox: "
            "('An error occurred (403) when calling the lambda operation: Access "
            "Denied',)"
        )
        param = {"share_id": event_proposal_selected["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )
        requests_mock.get(
            ConsorcieiClient().company_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=company_detail,
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = [
                invoke_mocked_success_response,
                ClientError(
                    {"Error": {"Code": "403", "Message": "Access Denied"}}, "lambda"
                ),
            ]
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_proposal_selected, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_success_pj_quota_if_request_to_bpm_has_timeout(
            requests_mock: Mocker,
            event_proposal_selected: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
            company_detail: Dict[str, Any],
            invoke_mocked_success_response: Dict[str, Any],
    ) -> None:
        param = {"share_id": event_proposal_selected["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, exc=Timeout("Created Timeout")
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )
        requests_mock.get(
            ConsorcieiClient().company_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=company_detail,
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = [
                invoke_mocked_success_response,
                {"StatusCode": 202, "Payload": StreamingBody(io.BytesIO(b""), 0)},
            ]
            response = lambda_handler(event_proposal_selected, context)
            body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_200_OK,
            {
                "bpm_response": 408,
                "customer_creation_call": True,
                "quota_creation_call": True,
                "company_bond_call": False,
                "contact_update": False,
                "life_proof_link_call": False,
            },
        )

    @staticmethod
    def test_santander_flow_with_success_pj_quota_if_company_has_no_address_and_contact(
            requests_mock: Mocker,
            event_proposal_selected: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
            company_detail_no_address_and_contact: Dict[str, Any],
            invoke_mocked_success_response: Dict[str, Any],
    ) -> None:
        param = {"share_id": event_proposal_selected["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, exc=Timeout("Created Timeout")
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )
        requests_mock.get(
            ConsorcieiClient().company_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=company_detail_no_address_and_contact,
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = [
                invoke_mocked_success_response,
                {"StatusCode": 202, "Payload": StreamingBody(io.BytesIO(b""), 0)},
            ]
            response = lambda_handler(event_proposal_selected, context)
            body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_200_OK,
            {
                "bpm_response": 408,
                "customer_creation_call": True,
                "quota_creation_call": True,
                "company_bond_call": False,
                "contact_update": False,
                "life_proof_link_call": False,
            },
        )

    @staticmethod
    def test_santander_flow_with_representatives_error_if_response_is_not_ok(
            requests_mock: Mocker,
            event_proposal_selected: Dict[str, Any],
            context,
            quota_detail_pf: Dict[str, Any],
    ) -> None:
        except_message = (
            "Máximo de 2 tentativas excedido ao chamar API: {'1': 'Requisição para obter "
            'lista de representantes não retornou 200: 400 - {"error": '
            "\"SHARE_NOT_FOUND\"}', '2': 'Requisição para obter lista de representantes "
            'não retornou 200: 400 - {"error": "SHARE_NOT_FOUND"}\'}'
        )
        param = {"share_id": event_proposal_selected["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pf,
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_400_BAD_REQUEST,
            json={"error": "SHARE_NOT_FOUND"},
        )

        with patch("boto3.client"):
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_proposal_selected, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_representatives_error_if_response_has_unexpected_format(
            requests_mock: Mocker,
            event_proposal_selected: Dict[str, Any],
            context,
            quota_detail_pf: Dict[str, Any],
            representatives_list: Dict[str, Any],
    ) -> None:
        except_message = (
            "Representantes obtidos da API Consorciei não possuem os campos esperados "
            "para mapear cliente: list indices must be integers or slices, not str"
        )
        param = {"share_id": event_proposal_selected["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pf,
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=representatives_list["data"],
        )

        with patch("boto3.client"):
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_proposal_selected, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_success_pf_quota_if_bpm_response_is_ok(
            requests_mock: Mocker,
            event_proposal_selected: Dict[str, Any],
            context,
            quota_detail_pf: Dict[str, Any],
            representatives_list: Dict[str, Any],
            invoke_mocked_success_response: Dict[str, Any],
    ) -> None:
        param = {"share_id": event_proposal_selected["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint,
            status_code=status.HTTP_202_ACCEPTED,
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pf,
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=representatives_list,
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = [
                invoke_mocked_success_response,
                {"StatusCode": 202, "Payload": StreamingBody(io.BytesIO(b""), 0)},
            ]
            response = lambda_handler(event_proposal_selected, context)
            body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_200_OK,
            {
                "bpm_response": 202,
                "customer_creation_call": True,
                "quota_creation_call": True,
                "company_bond_call": False,
                "contact_update": False,
                "life_proof_link_call": False,
            },
        )

    @staticmethod
    def test_santander_flow_with_success_pf_quota_if_representative_has_no_phone(
            requests_mock: Mocker,
            event_proposal_selected: Dict[str, Any],
            context,
            quota_detail_pf: Dict[str, Any],
            representatives_list_no_phone_and_rg: Dict[str, Any],
            invoke_mocked_success_response: Dict[str, Any],
    ) -> None:
        param = {"share_id": event_proposal_selected["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint,
            status_code=status.HTTP_202_ACCEPTED,
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pf,
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=representatives_list_no_phone_and_rg,
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = [
                invoke_mocked_success_response,
                {"StatusCode": 202, "Payload": StreamingBody(io.BytesIO(b""), 0)},
            ]
            response = lambda_handler(event_proposal_selected, context)
            body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_200_OK,
            {
                "bpm_response": 202,
                "customer_creation_call": True,
                "quota_creation_call": True,
                "company_bond_call": False,
                "contact_update": False,
                "life_proof_link_call": False,
            },
        )


class TestSantanderContractSingnedBySeller:
    @staticmethod
    def test_santander_flow_with_validation_error_if_event_is_missing_fields_and_invalid_date(
            event_contract_signed_by_seller: Dict[str, Any], context
    ) -> None:
        except_message = (
            "[{'loc': ('eventDate',), 'msg': 'eventDate não corresponde a um formato de "
            "data ISO.', 'type': 'value_error'}, {'loc': ('payload',), 'msg': 'field "
            "required', 'type': 'value_error.missing'}]"
        )

        del event_contract_signed_by_seller["payload"]
        event_contract_signed_by_seller["eventDate"] = "24-08-2023T23:34:18"

        with pytest.raises(UnprocessableEntity) as error:
            lambda_handler(event_contract_signed_by_seller, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_quota_detail_error_if_request_to_get_quota_has_timeout(
            requests_mock: Mocker, event_contract_signed_by_seller: Dict[str, Any], context
    ) -> None:
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}
        except_message = (
            "Máximo de 2 tentativas excedido ao chamar API: {'1': 'Requisição para "
            "https://api-sandbox.consorciei.com.br/secondary/shares/401c3d64-0777-47d4-9f53-6f9071629550 "
            "gerou timeout após 60 segundos.', '2': 'Requisição para "
            "https://api-sandbox.consorciei.com.br/secondary/shares/401c3d64-0777-47d4-9f53-6f9071629550 "
            "gerou timeout após 60 segundos.'}"
        )

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )

        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            exc=Timeout("Created Timeout"),
        )

        with patch("boto3.client"):
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_contract_signed_by_seller, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_quota_detail_error_if_response_is_not_ok(
            requests_mock: Mocker, event_contract_signed_by_seller: Dict[str, Any], context
    ) -> None:
        except_message = (
            "Máximo de 2 tentativas excedido ao chamar API: {'1': 'Requisição para obter "
            'cota não retornou 200: 400 - {"error": "SHARE_NOT_FOUND"}\', \'2\': '
            '\'Requisição para obter cota não retornou 200: 400 - {"error": '
            '"SHARE_NOT_FOUND"}\'}'
        )
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_400_BAD_REQUEST,
            json={"error": "SHARE_NOT_FOUND"},
        )

        with patch("boto3.client"):
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_contract_signed_by_seller, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_quota_detail_error_if_response_has_unexpected_format(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
    ) -> None:
        except_message = "Cota recebida da API Consorciei não possui campo isPj como esperados: 'data'"
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj["data"],
        )

        with patch("boto3.client"):
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_contract_signed_by_seller, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_company_detail_error_if_response_is_not_ok(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
    ) -> None:
        except_message = (
            "Máximo de 2 tentativas excedido ao chamar API: {'1': 'Requisição para obter "
            'detalhes da empresa não retornou 200: 400 - {"error": "SHARE_NOT_FOUND"}\', '
            "'2': 'Requisição para obter detalhes da empresa não retornou 200: 400 - "
            '{"error": "SHARE_NOT_FOUND"}\'}'
        )
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )
        requests_mock.get(
            ConsorcieiClient().company_detail_endpoint.format(**param),
            status_code=status.HTTP_400_BAD_REQUEST,
            json={"error": "SHARE_NOT_FOUND"},
        )

        with patch("boto3.client"):
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_contract_signed_by_seller, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_company_detail_error_if_response_has_unexpected_format(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
            company_detail: Dict[str, Any],
    ) -> None:
        except_message = (
            "Empresa recebida da API Consorciei não possuem os campos esperados para "
            "mapear cliente: 'data'"
        )
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )
        requests_mock.get(
            ConsorcieiClient().company_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=company_detail["data"],
        )

        with patch("boto3.client"):
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_contract_signed_by_seller, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_representative_error_for_customer_if_response_is_not_ok(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
            company_detail: Dict[str, Any],
    ) -> None:
        except_message = (
            "Máximo de 2 tentativas excedido ao chamar API: {'1': 'Requisição para obter "
            'lista de representantes não retornou 200: 400 - {"error": '
            "\"SHARE_NOT_FOUND\"}', '2': 'Requisição para obter lista de representantes "
            'não retornou 200: 400 - {"error": "SHARE_NOT_FOUND"}\'}'
        )
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )
        requests_mock.get(
            ConsorcieiClient().company_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=company_detail,
        )

        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={
                "person_code": "0000444534",
                "token": "f295c36b-6eaa-4f24-869b-b292dda6cb51",
            },
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_400_BAD_REQUEST,
            json={"error": "SHARE_NOT_FOUND"},
        )

        with patch("boto3.client"):
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_contract_signed_by_seller, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_representative_error_for_customer_if_response_has_unexpected_format(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
            company_detail: Dict[str, Any],
            representatives_list: Dict[str, Any],
    ) -> None:
        except_message = (
            "Representantes obtidos da API Consorciei não possuem os campos esperados "
            "para mapear cliente: 'cpf'"
        )

        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}
        del representatives_list["data"][0]["cpf"]

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )
        requests_mock.get(
            ConsorcieiClient().company_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=company_detail,
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={
                "person_code": "0000444534",
                "token": "f295c36b-6eaa-4f24-869b-b292dda6cb51",
            },
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=representatives_list,
        )

        with patch("boto3.client"):
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_contract_signed_by_seller, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_company_bond_error_if_invoke_raises_client_error(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
            company_detail: Dict[str, Any],
            representatives_list: Dict[str, Any],
    ) -> None:
        except_message = (
            "Não foi possível invocar Lambda md-cota-company-bond-sandbox: ('An error "
            "occurred (403) when calling the lambda operation: Access Denied',)"
        )
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )
        requests_mock.get(
            ConsorcieiClient().company_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=company_detail,
        )

        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={
                "person_code": "0000444534",
                "token": "f295c36b-6eaa-4f24-869b-b292dda6cb51",
            },
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=representatives_list,
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = [
                ClientError(
                    {"Error": {"Code": "403", "Message": "Access Denied"}}, "lambda"
                ),
            ]
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_contract_signed_by_seller, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_life_proof_error_pj_quota_if_invoke_raises_client_error(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
            company_detail: Dict[str, Any],
            representatives_list: Dict[str, Any],
    ) -> None:
        except_message = (
            "Não foi possível invocar Lambda md-cota-life-proof-link-sender-sandbox: ('An "
            "error occurred (403) when calling the lambda operation: Access Denied',)"
        )
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )
        requests_mock.get(
            ConsorcieiClient().company_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=company_detail,
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={
                "person_code": "0000444534",
                "token": "f295c36b-6eaa-4f24-869b-b292dda6cb51",
            },
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=representatives_list,
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = [
                {"StatusCode": 202, "Payload": StreamingBody(io.BytesIO(b""), 0)},
                ClientError(
                    {"Error": {"Code": "403", "Message": "Access Denied"}}, "lambda"
                ),
            ]
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_contract_signed_by_seller, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_success_pj_quota_if_all_lambda_are_called(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
            company_detail: Dict[str, Any],
            representatives_list: Dict[str, Any],
    ) -> None:
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )
        requests_mock.get(
            ConsorcieiClient().company_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=company_detail,
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={
                "person_code": "0000444534",
                "token": "f295c36b-6eaa-4f24-869b-b292dda6cb51",
            },
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=representatives_list,
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = [
                {"StatusCode": 202, "Payload": StreamingBody(io.BytesIO(b""), 0)},
                {"StatusCode": 202, "Payload": StreamingBody(io.BytesIO(b""), 0)},
            ]

            response = lambda_handler(event_contract_signed_by_seller, context)
            body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_200_OK,
            {
                "bpm_response": 202,
                "contact_update": True,
                "company_bond_call": True,
                "life_proof_link_call": True,
                "customer_creation_call": False,
                "quota_creation_call": False,
            },
        )

    @staticmethod
    def test_santander_flow_with_success_pj_quota_if_company_has_no_contact(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
            company_detail_no_address_and_contact: Dict[str, Any],
            representatives_list: Dict[str, Any],
    ) -> None:
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )
        requests_mock.get(
            ConsorcieiClient().company_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=company_detail_no_address_and_contact,
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=representatives_list,
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={
                "person_code": "0000444534",
                "token": "f295c36b-6eaa-4f24-869b-b292dda6cb51",
            },
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = [
                {"StatusCode": 200, "Payload": StreamingBody(io.BytesIO(b""), 0)},
                {"StatusCode": 202, "Payload": StreamingBody(io.BytesIO(b""), 0)},
            ]

            response = lambda_handler(event_contract_signed_by_seller, context)
            body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_200_OK,
            {
                "bpm_response": 202,
                "contact_update": True,
                "company_bond_call": True,
                "life_proof_link_call": True,
                "customer_creation_call": False,
                "quota_creation_call": False,
            },
        )

    @staticmethod
    def test_santander_flow_with_success_pj_quota_if_company_has_no_cnpj_and_name(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pj: Dict[str, Any],
            company_detail_no_cnpj_and_name: Dict[str, Any],
            representatives_list: Dict[str, Any],
    ) -> None:
        except_message = (
           "Consorciei enviou dado inválido. Dado enviado, Identificador: , nome: "
        )
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pj,
        )
        requests_mock.get(
            ConsorcieiClient().company_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=company_detail_no_cnpj_and_name,
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=representatives_list,
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={
                "person_code": "0000444534",
                "token": "f295c36b-6eaa-4f24-869b-b292dda6cb51",
            },
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = [
                {"StatusCode": 200, "Payload": StreamingBody(io.BytesIO(b""), 0)},
                {"StatusCode": 202, "Payload": StreamingBody(io.BytesIO(b""), 0)},
            ]
            with patch("boto3.client"):
                with pytest.raises(InternalServerError) as error:
                    lambda_handler(event_contract_signed_by_seller, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_representative_error_if_response_is_not_ok(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pf: Dict[str, Any],
    ) -> None:
        except_message = (
            "Máximo de 2 tentativas excedido ao chamar API: {'1': 'Requisição para obter "
            'lista de representantes não retornou 200: 400 - {"error": '
            "\"SHARE_NOT_FOUND\"}', '2': 'Requisição para obter lista de representantes "
            'não retornou 200: 400 - {"error": "SHARE_NOT_FOUND"}\'}'
        )
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pf,
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_400_BAD_REQUEST,
            json={"error": "SHARE_NOT_FOUND"},
        )

        with patch("boto3.client"):
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_contract_signed_by_seller, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_representative_error_if_response_has_unexpected_format(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pf: Dict[str, Any],
            representatives_list: Dict[str, Any],
    ) -> None:
        except_message = (
            "Representantes obtidos da API Consorciei não possuem os campos esperados "
            "para mapear cliente: list indices must be integers or slices, not str"
        )
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pf,
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=representatives_list["data"],
        )

        with patch("boto3.client"):
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_contract_signed_by_seller, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_life_proof_error_pf_quota_if_invoke_raises_client_error(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pf: Dict[str, Any],
            representatives_list: Dict[str, Any],
    ) -> None:
        except_message = (
            "Não foi possível invocar Lambda md-cota-life-proof-link-sender-sandbox: ('An "
            "error occurred (403) when calling the lambda operation: Access Denied',)"
        )
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pf,
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=representatives_list,
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={
                "person_code": "0000444534",
                "token": "f295c36b-6eaa-4f24-869b-b292dda6cb51",
            },
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = [
                ClientError(
                    {"Error": {"Code": "403", "Message": "Access Denied"}}, "lambda"
                ),
            ]
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_contract_signed_by_seller, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_success_pf_quota_if_lambda_is_called(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pf: Dict[str, Any],
            representatives_list: Dict[str, Any],
    ) -> None:
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pf,
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=representatives_list,
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={
                "person_code": "0000444534",
                "token": "f295c36b-6eaa-4f24-869b-b292dda6cb51",
            },
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = [
                {"StatusCode": 202, "Payload": StreamingBody(io.BytesIO(b""), 0)},
            ]
            response = lambda_handler(event_contract_signed_by_seller, context)
            body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_200_OK,
            {
                "bpm_response": 202,
                "contact_update": True,
                "company_bond_call": False,
                "life_proof_link_call": True,
                "customer_creation_call": False,
                "quota_creation_call": False,
            },
        )

    @staticmethod
    def test_santander_flow_without_success_if_representatives_not_has_cpf_and_name(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pf: Dict[str, Any],
            representatives_list_no_cpf_and_name: Dict[str, Any],
    ) -> None:
        except_message = ("Consorciei enviou dado inválido. "
                          "Dado enviado, Identificador: , nome: ")
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pf,
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=representatives_list_no_cpf_and_name,
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={
                "person_code": "0000444534",
                "token": "f295c36b-6eaa-4f24-869b-b292dda6cb51",
            },
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = [
                {"StatusCode": 202, "Payload": StreamingBody(io.BytesIO(b""), 0)},
            ]
            with pytest.raises(InternalServerError) as error:
                lambda_handler(event_contract_signed_by_seller, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_santander_flow_with_success_pf_quota_if_representative_has_no_phone(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pf: Dict[str, Any],
            representatives_list_no_phone_and_rg: Dict[str, Any],
    ) -> None:
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pf,
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=representatives_list_no_phone_and_rg,
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={
                "person_code": "0000444534",
                "token": "f295c36b-6eaa-4f24-869b-b292dda6cb51",
            },
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = [
                {"StatusCode": 202, "Payload": StreamingBody(io.BytesIO(b""), 0)},
            ]
            response = lambda_handler(event_contract_signed_by_seller, context)
            body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_200_OK,
            {
                "bpm_response": 202,
                "contact_update": True,
                "company_bond_call": False,
                "life_proof_link_call": True,
                "customer_creation_call": False,
                "quota_creation_call": False,
            },
        )

    @staticmethod
    def test_santander_flow_with_success_pf_if_representatives_not_has_contacts(
            requests_mock: Mocker,
            event_contract_signed_by_seller: Dict[str, Any],
            context,
            quota_detail_pf: Dict[str, Any],
            representatives_list_without_contacts: Dict[str, Any],
    ) -> None:
        param = {"share_id": event_contract_signed_by_seller["payload"]["shareId"]}

        requests_mock.post(
            BPMClient().santander_events_endpoint, status_code=status.HTTP_202_ACCEPTED
        )
        requests_mock.get(
            ConsorcieiClient().quota_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=quota_detail_pf,
        )
        requests_mock.get(
            ConsorcieiClient().representatives_detail_endpoint.format(**param),
            status_code=status.HTTP_200_OK,
            json=representatives_list_without_contacts,
        )
        requests_mock.post(
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={
                "person_code": "0000444534",
                "token": "f295c36b-6eaa-4f24-869b-b292dda6cb51",
            },
        )

        with patch("boto3.client") as mock_boto_client:
            mock_boto_client.return_value = mock_boto_client
            mock_boto_client.invoke.side_effect = [
                {"StatusCode": 202, "Payload": StreamingBody(io.BytesIO(b""), 0)},
            ]
            response = lambda_handler(event_contract_signed_by_seller, context)
            body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_200_OK,
            {
                "bpm_response": 202,
                "contact_update": True,
                "company_bond_call": False,
                "life_proof_link_call": True,
                "customer_creation_call": False,
                "quota_creation_call": False,
            },
        )
