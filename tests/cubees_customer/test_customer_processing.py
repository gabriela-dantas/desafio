import pytest
import json

from datetime import date
from typing import Dict, Any
from fastapi import status
from operator import itemgetter

from common.clients.cubees import CubeesClient
from cubees_customer.handler import lambda_handler
from requests_mock import Mocker
from common.repositories.md_cota.group import GroupRepository
from common.repositories.md_cota.quota import QuotaRepository
from common.repositories.md_cota.quota_status import QuotaStatusRepository
from common.repositories.md_cota.quota_history_detail import (
    QuotaHistoryDetailRepository,
)
from common.exceptions import UnprocessableEntity, InternalServerError


@pytest.fixture(scope="class")
def create_quota_data() -> None:
    group = {
        "group_id": 1,
        "group_code": "20164",
        "group_deadline": 24,
        "group_start_date": date(2023, 1, 1),
        "group_closing_date": date(2025, 1, 1),
        "current_assembly_date": date(2023, 1, 1),
        "next_adjustment_date": date(2023, 2, 1),
        "current_assembly_number": 1,
        "administrator_id": 1,
    }

    GroupRepository().create(group)

    quota = {
        "quota_id": 1,
        "quota_code": "BZ0000018",
        "quota_number": "210",
        "check_digit": "2",
        "external_reference": "asfbgasrgfvasdfvbasdfg",
        "total_installments": 24,
        "version_id": "NA",
        "contract_number": "3973076",
        "is_contemplated": False,
        "is_multiple_ownership": False,
        "administrator_fee": 14.50,
        "fund_reservation_fee": 2.00,
        "info_date": date(2023, 1, 1),
        "quota_status_type_id": 1,
        "administrator_id": 1,
        "group_id": 1,
        "quota_origin_id": 1,
        "quota_person_type_id": 1,
        "acquisition_date": date(2023, 1, 1),
    }

    QuotaRepository().create(quota)

    quota_status = {"quota_id": 1, "quota_status_type_id": 1}

    QuotaStatusRepository().create(quota_status)

    quota_history_detail = {
        "quota_id": 1,
        "old_quota_number": 30,
        "old_digit": 1,
        "installments_paid_number": 1,
        "per_amount_paid": 2.2243,
        "per_mutual_fund_paid": 0.7197,
        "per_reserve_fund_paid": 0.1307,
        "per_adm_paid": 1.3739,
        "per_mutual_fund_to_pay": 99.2718,
        "per_reserve_fund_to_pay": 5.8677,
        "per_adm_to_pay": 19.6099,
        "per_install_diff_to_pay": 0.0263,
        "per_total_amount_to_pay": 124.7757,
        "amnt_mutual_fund_to_pay": 65613.69,
        "amnt_reserve_fund_to_pay": 3878.22,
        "amnt_adm_to_pay": 12961.18,
        "amnt_install_diff_to_pay": 17.39,
        "amnt_to_pay": 82470.48,
        "adjustment_date": date(2023, 6, 1),
        "current_assembly_date": date(2023, 1, 1),
        "current_assembly_number": 1,
        "asset_adm_code": "007731",
        "asset_description": "007731 86.64 onix 1.0 - mais facil",
        "asset_value": 66095.00,
        "info_date": date(2023, 1, 1),
        "asset_type_id": 2,
    }

    QuotaHistoryDetailRepository().create(quota_history_detail)


@pytest.fixture
def event() -> Dict[str, Any]:
    return {
        "cubees_request": [
            {
                "administrator_code": "0000000289",
                "channel_type": "EMAIL",
                "contacts": [
                    {
                        "contact": "wilmaaraujo45@hotmail.com",
                        "contact_category": "PERSONAL",
                        "contact_desc": "EMAIL PESSOAL",
                        "contact_type": "EMAIL",
                        "preferred_contact": True,
                    },
                    {
                        "contact": "11 967661797",
                        "contact_category": "PERSONAL",
                        "contact_desc": "TELEFONE CELULAR",
                        "contact_type": "MOBILE",
                        "preferred_contact": False,
                    },
                ],
                "documents": [
                    {
                        "document_number": "00090098501",
                        "expiring_date": "2040-12-01",
                        "person_document_type": "CPF",
                    }
                ],
                "natural_person": {"birthdate": None, "full_name": "WILMA ARAUJO MELO"},
                "person_ext_code": "00090098501",
                "person_type": "NATURAL",
                "reactive": False,
            },
            {
                "administrator_code": "0000000289",
                "channel_type": "EMAIL",
                "contacts": [
                    {
                        "contact": "kakashi.hatake@hotmail.com",
                        "contact_category": "PERSONAL",
                        "contact_desc": "EMAIL PESSOAL",
                        "contact_type": "EMAIL",
                        "preferred_contact": True,
                    },
                    {
                        "contact": "11 997891299",
                        "contact_category": "PERSONAL",
                        "contact_desc": "TELEFONE CELULAR",
                        "contact_type": "MOBILE",
                        "preferred_contact": False,
                    },
                ],
                "documents": [
                    {
                        "document_number": "00090098501",
                        "expiring_date": "2040-12-01",
                        "person_document_type": "CPF",
                    }
                ],
                "natural_person": {
                    "birthdate": "1990-01-01",
                    "full_name": "Kakashi Hatake",
                },
                "person_ext_code": "64668374083",
                "person_type": "NATURAL",
                "reactive": False,
            },
        ],
        "ownership_percentage": 0.5,
        "quota_id": 1,
        "main_owner": "00090098501",
    }


@pytest.fixture
def event_legal_person() -> Dict[str, Any]:
    return {
        "cubees_request": [
            {
                "administrator_code": "0000000289",
                "channel_type": "EMAIL",
                "contacts": [
                    {
                        "contact": "wilmaaraujo45@hotmail.com",
                        "contact_category": "PERSONAL",
                        "contact_desc": "EMAIL PESSOAL",
                        "contact_type": "EMAIL",
                        "preferred_contact": True,
                    },
                    {
                        "contact": "11 967661797",
                        "contact_category": "PERSONAL",
                        "contact_desc": "TELEFONE CELULAR",
                        "contact_type": "MOBILE",
                        "preferred_contact": False,
                    },
                ],
                "documents": [
                    {
                        "document_number": "00090098501",
                        "expiring_date": "2040-12-01",
                        "person_document_type": "CPF",
                    }
                ],
                "natural_person": {"birthdate": None, "full_name": "WILMA ARAUJO MELO"},
                "person_ext_code": "00090098501",
                "person_type": "LEGAL",
                "reactive": False,
            }
        ],
        "ownership_percentage": 0.5,
        "quota_id": 1,
        "main_owner": "00090098501",
    }


@pytest.mark.usefixtures("create_quota_data")
class TestCustomerProcessing:
    @staticmethod
    def test_customer_processing_with_validation_error_if_cubees_request_is_missing(
        event: Dict[str, Any], context
    ) -> None:
        except_message = (
            "[{'loc': ('cubees_request',), 'msg': 'field required', 'type': "
            "'value_error.missing'}]"
        )
        del event["cubees_request"]

        with pytest.raises(UnprocessableEntity) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_customer_processing_with_validation_error_if_ownership_percentage_is_out_of_range(
        event: Dict[str, Any], context
    ) -> None:
        except_message = (
            "[{'loc': ('ownership_percentage',), 'msg': 'ensure this value is less than "
            "or equal to 1', 'type': 'value_error.number.not_le', 'ctx': {'limit_value': "
            "1}}]"
        )
        event["ownership_percentage"] = 1.1

        with pytest.raises(UnprocessableEntity) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_customer_processing_with_internal_error_if_cubees_response_has_no_code(
        requests_mock: Mocker, event: Dict[str, Any], context
    ) -> None:
        except_message = "Request ao Cubees não retornou person code: 201 - {}"

        requests_mock.register_uri(
            "POST",
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={},
        )

        with pytest.raises(InternalServerError) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_customer_processing_with_internal_error_if_cubees_response_is_not_201(
        requests_mock: Mocker, event: Dict[str, Any], context
    ) -> None:
        except_message = (
            "Requisição para criar cliente no Cubees não retornou 201: 403 - Forbidden"
        )

        requests_mock.register_uri(
            "POST",
            CubeesClient().create_customer_url,
            status_code=status.HTTP_403_FORBIDDEN,
            text="Forbidden",
        )

        with pytest.raises(InternalServerError) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_customer_processing_with_success_if_there_is_no_owner_in_db(
        requests_mock: Mocker, event: Dict[str, Any], context
    ) -> None:
        del event["cubees_request"][1]

        requests_mock.register_uri(
            "POST",
            CubeesClient().create_customer_url,
            status_code=status.HTTP_201_CREATED,
            json={"person_code": "0000403537"},
        )

        response = lambda_handler(event, context)
        data = json.loads(response["body"])

        assert (response["statusCode"], data) == (
            status.HTTP_201_CREATED,
            [{"person_code": "0000403537"}],
        )

    @staticmethod
    def test_customer_processing_with_success_if_one_owner_exists_in_db(
        requests_mock: Mocker, event: Dict[str, Any], context
    ) -> None:
        requests_mock.register_uri(
            "POST",
            CubeesClient().create_customer_url,
            [
                {"json": {"person_code": "0000403537"}, "status_code": 201},
                {"json": {"person_code": "0000403538"}, "status_code": 201},
            ],
        )

        response = lambda_handler(event, context)
        data = json.loads(response["body"])

        assert (response["statusCode"], data) == (
            status.HTTP_201_CREATED,
            [{"person_code": "0000403538"}],
        )

    @staticmethod
    def test_customer_processing_with_error_if_not_exist_type_person_in_database(
        requests_mock: Mocker, event_legal_person: Dict[str, Any], context
    ) -> None:
        event_legal_person["cubees_request"][0]["person_type"] = "LEGAL_PERSON"
        except_message = (
            "Conteudo de chave inválida em person_type, conteúdo:'LEGAL_PERSON'"
        )
        requests_mock.register_uri(
            "POST",
            CubeesClient().create_customer_url,
            [
                {"json": {"person_code": "0000403542"}, "status_code": 201},
            ],
        )
        with pytest.raises(InternalServerError) as error:
            lambda_handler(event_legal_person, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_customer_processing_with_success_if_owners_in_db_are_invalidated(
        requests_mock: Mocker, event: Dict[str, Any], context
    ) -> None:
        requests_mock.register_uri(
            "POST",
            CubeesClient().create_customer_url,
            [
                {"json": {"person_code": "0000403539"}, "status_code": 201},
                {"json": {"person_code": "0000403512"}, "status_code": 201},
            ],
        )

        response = lambda_handler(event, context)
        data = json.loads(response["body"])
        data = sorted(data, key=itemgetter("person_code"), reverse=True)

        assert (response["statusCode"], data) == (
            status.HTTP_201_CREATED,
            [{"person_code": "0000403539"}, {"person_code": "0000403512"}],
        )

    @staticmethod
    def test_customer_processing_with_success_if_customer_is_legal_person(
        requests_mock: Mocker, event_legal_person: Dict[str, Any], context
    ) -> None:
        requests_mock.register_uri(
            "POST",
            CubeesClient().create_customer_url,
            [{"json": {"person_code": "0000403543"}, "status_code": 201}],
        )

        response = lambda_handler(event_legal_person, context)
        data = json.loads(response["body"])
        data = sorted(data, key=itemgetter("person_code"), reverse=True)

        assert (response["statusCode"], data) == (
            status.HTTP_201_CREATED,
            [{"person_code": "0000403543"}],
        )
