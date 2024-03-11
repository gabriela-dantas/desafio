import json
import pytest

from datetime import date, datetime
from typing import Dict, Any

from requests_mock.mocker import Mocker
from requests.exceptions import Timeout, TooManyRedirects, HTTPError

from fastapi import status
from quota_creation_invoke.handler import lambda_handler

from common.repositories.md_cota.group import GroupRepository
from common.repositories.md_cota.quota import QuotaRepository
from common.repositories.md_cota.quota_view import QuotaViewRepository
from common.repositories.md_cota.quota_owner import QuotaOwnerRepository
from common.clients.bpm import BPMClient
from common.exceptions import (
    UnprocessableEntity,
    InternalServerError,
    Timeout as TimeoutException,
    TooManyRedirects as TooManyRedirectsException,
    EntityNotFound,
)


@pytest.fixture(scope="class")
def create_quota_data() -> None:
    # ITAÚ
    group = {
        "group_id": 1,
        "group_code": "20164",
        "group_deadline": 24,
        "group_start_date": date(2023, 1, 1),
        "group_closing_date": date(2025, 1, 1),
        "next_adjustment_date": date(2023, 9, 1),
        "current_assembly_date": date(2023, 8, 1),
        "current_assembly_number": 8,
        "administrator_id": 1,
    }

    GroupRepository().create(group)

    quota = {
        "quota_id": 1,
        "quota_code": "BZ0000019",
        "quota_number": "210",
        "check_digit": "2",
        "external_reference": "asfbgasrgfvasdfvbasdfg",
        "total_installments": 24,
        "version_id": "1",
        "contract_number": "3973076",
        "is_contemplated": False,
        "is_multiple_ownership": False,
        "administrator_fee": 14.50,
        "fund_reservation_fee": 2.00,
        "acquisition_date": date(2023, 1, 1),
        "info_date": date(2023, 1, 1),
        "quota_status_type_id": 1,
        "administrator_id": 1,
        "group_id": 1,
        "quota_origin_id": 1,
        "quota_person_type_id": 1,
    }

    QuotaRepository().create(quota)

    quotas_owner = {
        "quota_owner_id": 1,
        "ownership_percent": 1,
        "quota_id": 1,
        "person_code": "0000000010",
        "main_owner": True,
    }
    QuotaOwnerRepository().create(quotas_owner)

    quota_view_1 = {
        "quota_id": 1,
        "quota_code": "BZ0000019",
        "quota_number": "210",
        "check_digit": "2",
        "external_reference": "asfbgasrgfvasdfvbasdfg",
        "total_installments": 24,
        "version_id": "1",
        "contract_number": "3973076",
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
        "administrator_code": "0000000155",
        "administrator_desc": "ITAÚ ADM DE CONSÓRCIOS LTDA",
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

    # Santander
    group = {
        "group_id": 2,
        "group_code": "30164",
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
        "quota_id": 2,
        "quota_code": "BZ0000020",
        "quota_number": "212",
        "check_digit": "2",
        "external_reference": "bbfbgasrgfvasdfvbasdfg",
        "total_installments": 24,
        "version_id": "NA",
        "contract_number": "4973076",
        "is_contemplated": False,
        "is_multiple_ownership": False,
        "administrator_fee": 13.50,
        "fund_reservation_fee": 1.00,
        "acquisition_date": date(2023, 1, 1),
        "info_date": date(2023, 1, 1),
        "quota_status_type_id": 1,
        "administrator_id": 2,
        "group_id": 2,
        "quota_origin_id": 1,
        "quota_person_type_id": 1,
    }

    QuotaRepository().create(quota)

    quotas_owner = {
        "quota_owner_id": 2,
        "ownership_percent": 1,
        "quota_id": 2,
        "person_code": "0000000012",
        "main_owner": True,
    }
    QuotaOwnerRepository().create(quotas_owner)

    quota_view_1 = {
        "quota_id": 2,
        "quota_code": "BZ0000020",
        "quota_number": "212",
        "check_digit": "2",
        "external_reference": "bbfbgasrgfvasdfvbasdfg",
        "total_installments": 24,
        "version_id": "NA",
        "contract_number": "4973076",
        "is_contemplated": False,
        "is_multiple_ownership": False,
        "administrator_fee": 13.50,
        "fund_reservation_fee": 1.00,
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
        "group_code": "30164",
        "group_deadline": 24,
        "group_start_date": date(2023, 1, 1),
        "group_closing_date": date(2025, 1, 1),
        "next_adjustment_date": date(2023, 9, 1),
        "grp_current_assembly_date": date(2023, 8, 1),
        "grp_current_assembly_number": 8,
        "chosen_bid": 30,
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
def event() -> Dict[str, Any]:
    return {"detail": {"quota_code_list": [{"quota_code": "BZ0000019"}]}}


@pytest.fixture(scope="function")
def event_not_found() -> Dict[str, Any]:
    return {"detail": {"quota_code_list": [{"quota_code": "BZ0"}]}}


@pytest.fixture(scope="function")
def event_only_one_found() -> Dict[str, Any]:
    return {
        "detail": {
            "quota_code_list": [
                {"quota_code": "BZ0000019"},
                {"quota_code": "BZ0000000"},
            ]
        }
    }


@pytest.fixture(scope="function")
def event_santander() -> Dict[str, Any]:
    return {
        "detail": {
            "quota_code_list": [
                {
                    "quota_code": "BZ0000020",
                    "share_id": "401c3d64-0777-47d4-9f53-6f9071629550",
                }
            ]
        }
    }


@pytest.mark.usefixtures("create_quota_data")
class TestQuotaCreationInvoke:
    bpm_endpoint = (
        "https://staging.bazardoconsorcio.com.br/api/internal/md_cotas/quotas"
    )
    token = "dummy"

    @staticmethod
    def test_quota_creation_with_validation_error_if_event_hat_invalid_body(
        event: Dict[str, Any], context
    ):
        except_message = (
            "[{'loc': ('quota_code_list', 0, 'quota_code'), 'msg': 'str type expected', "
            "'type': 'type_error.str'}]"
        )
        event["detail"]["quota_code_list"][0]["quota_code"] = {"dados": 123}

        with pytest.raises(UnprocessableEntity) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_quota_creation_with_timeout_error_if_request_to_bpm_has_timeout(
        requests_mock: Mocker, event: Dict[str, Any], context
    ) -> None:
        except_message = (
            "Requisição para "
            "https://staging.bazardoconsorcio.com.br/api/internal/md_cotas/quotas gerou "
            "timeout após 10 segundos."
        )
        requests_mock.post(BPMClient().url_create_quota, exc=Timeout("Created Timeout"))

        with pytest.raises(TimeoutException) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_quota_creation_with_redirect_error_if_url_request_to_bpm_has_many_redirects(
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
    ) -> None:
        except_message = (
            "Requisição para "
            "https://staging.bazardoconsorcio.com.br/api/internal/md_cotas/quotas excedeu "
            "redirecionamentos."
        )
        requests_mock.post(
            BPMClient().url_create_quota,
            exc=TooManyRedirects("Created TooManyRedirects"),
        )

        with pytest.raises(TooManyRedirectsException) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_quota_creation_with_internal_server_error_if_url_request_to_bpm_has_http_error(
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
    ) -> None:
        except_message = (
            "Requisição para "
            "https://staging.bazardoconsorcio.com.br/api/internal/md_cotas/quotas gerou "
            "erro HTTP: ('Created HTTPError',)"
        )
        requests_mock.post(
            BPMClient().url_create_quota, exc=HTTPError("Created HTTPError")
        )

        with pytest.raises(InternalServerError) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_quota_creation_with_forbidden_if_url_request_to_bpm_has_response_not_ok(
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
    ) -> None:
        except_message = (
            "Requisição para criar cota no BPM não retornou 202: 403 - Forbidden"
        )
        requests_mock.post(
            BPMClient().url_create_quota,
            status_code=status.HTTP_403_FORBIDDEN,
            text="Forbidden",
        )

        with pytest.raises(InternalServerError) as error:
            lambda_handler(event, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_quota_creation_with_not_found_if_no_quota_not_in_db(
        event_not_found: Dict[str, Any],
        context,
    ) -> None:
        except_message = (
            "Nenhuma cota encontrada no banco do MD Cota, para envio ao BPM."
        )

        with pytest.raises(EntityNotFound) as error:
            lambda_handler(event_not_found, context)

        assert error.value.args[0] == except_message

    @staticmethod
    def test_quota_creation_with_success_if_is_sent_one_quota(
        requests_mock: Mocker,
        event: Dict[str, Any],
        context,
    ) -> None:
        requests_mock.post(
            BPMClient().url_create_quota, status_code=status.HTTP_202_ACCEPTED
        )
        response = lambda_handler(event, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_201_CREATED,
            "Invocação da lambda de criação de cota feita com sucesso.",
        )

    @staticmethod
    def test_quota_creation_with_success_if_only_one_quota_is_found(
        requests_mock: Mocker,
        event_only_one_found: Dict[str, Any],
        context,
    ) -> None:
        requests_mock.post(
            BPMClient().url_create_quota, status_code=status.HTTP_202_ACCEPTED
        )
        response = lambda_handler(event_only_one_found, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_201_CREATED,
            "Invocação da lambda de criação de cota feita com sucesso.",
        )

    @staticmethod
    def test_quota_creation_with_success_if_quota_is_santander(
        requests_mock: Mocker,
        event_santander: Dict[str, Any],
        context,
    ) -> None:
        requests_mock.post(
            BPMClient().url_create_quota, status_code=status.HTTP_202_ACCEPTED
        )
        response = lambda_handler(event_santander, context)
        body = json.loads(response["body"])

        assert (response["statusCode"], body) == (
            status.HTTP_201_CREATED,
            "Invocação da lambda de criação de cota feita com sucesso.",
        )
