import pytest

from fastapi.testclient import TestClient
from fastapi import status
from datetime import date, datetime
from typing import Dict, Any
from requests_mock.mocker import Mocker
from requests.exceptions import Timeout, TooManyRedirects, HTTPError

from common.repositories.md_cota.group import GroupRepository
from common.repositories.md_cota.quota import QuotaRepository
from common.repositories.md_cota.quota_owner import QuotaOwnerRepository
from common.repositories.md_cota.quota_history_detail import (
    QuotaHistoryDetailRepository,
)
from common.repositories.md_cota.quota_view import QuotaViewRepository
from common.clients.bpm import BPMClient


@pytest.fixture(scope="class")
def create_quota_data() -> None:
    group = {
        "group_id": 1,
        "group_code": "20164",
        "group_deadline": 24,
        "group_start_date": date(2023, 1, 1),
        "group_closing_date": date(2025, 1, 1),
        "next_adjustment_date": date(2023, 2, 1),
        "current_assembly_date": date(2023, 1, 1),
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
        "acquisition_date": date(2023, 1, 1),
        "info_date": date(2023, 1, 1),
        "quota_status_type_id": 1,
        "administrator_id": 1,
        "group_id": 1,
        "quota_origin_id": 1,
        "quota_person_type_id": 1,
    }

    QuotaRepository().create(quota)

    quota_owner = {
        "ownership_percent": 1,
        "quota_id": 1,
        "person_code": "0000404268",
        "main_owner": True,
    }

    QuotaOwnerRepository().create(quota_owner)

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

    quota_view_1 = {
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
        "next_adjustment_date": date(2023, 2, 1),
        "grp_current_assembly_date": date(2023, 1, 1),
        "grp_current_assembly_number": 1,
        "chosen_bid": 25,
        "max_bid_occurrences_perc": 49,
        "bid_calculation_date": date(2023, 1, 1),
        "administrator_code": "0000000155",
        "administrator_desc": "ITAÚ ADM DE CONSÓRCIOS LTDA",
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
        "quota_history_info_date": date(2023, 1, 1),
        "asset_type_code": "CAR",
        "asset_type_desc": "CAR",
        "vacancies": 50,
        "vacancies_info_date": date(2023, 7, 1),
    }

    QuotaViewRepository().create(quota_view_1, True)


@pytest.fixture(scope="function")
def bpm_quota_request_data() -> Dict[str, Any]:
    return {
        "data": {
            "action": "card.move",
            "to": {"id": 318196139, "name": "Opt-in"},
            "card": {"id": 633833604, "title": "BZ0000018"},
        }
    }


@pytest.mark.usefixtures("create_quota_data")
class TestBPMQuota:
    endpoint = "/bpm/quota"

    headers = {"x-api-key": "dummy"}

    @classmethod
    def test_bpm_quota_with_validation_error_if_card_has_no_title(
        cls, bpm_quota_request_data: Dict[str, Any], client: TestClient
    ) -> None:
        del bpm_quota_request_data["data"]["card"]["title"]

        response = client.post(
            url=cls.endpoint, json=bpm_quota_request_data, headers=cls.headers
        )

        assert (response.status_code, response.json()["detail"]) == (
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            [
                {
                    "loc": ["body", "data", "card", "title"],
                    "msg": "field required",
                    "type": "value_error.missing",
                }
            ],
        )

    @classmethod
    def test_bpm_quota_with_validation_error_if_to_id_is_not_opt_in(
        cls, bpm_quota_request_data: Dict[str, Any], client: TestClient
    ) -> None:
        bpm_quota_request_data["data"]["to"]["id"] = 318196131

        response = client.post(
            url=cls.endpoint, json=bpm_quota_request_data, headers=cls.headers
        )

        assert (response.status_code, response.json()["detail"]) == (
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            [
                {
                    "loc": ["body", "data", "to", "id"],
                    "msg": "ID informado é diferente do ID de OPT-IN: 318196139",
                    "type": "value_error",
                }
            ],
        )

    @classmethod
    def test_bpm_quota_with_entity_not_found_if_card_quota_does_not_exist(
        cls, bpm_quota_request_data: Dict[str, Any], client: TestClient
    ) -> None:
        bpm_quota_request_data["data"]["card"]["title"] = "BZ0000020"

        response = client.post(
            url=cls.endpoint, json=bpm_quota_request_data, headers=cls.headers
        )

        assert (response.status_code, response.json()) == (
            status.HTTP_404_NOT_FOUND,
            {
                "detail": "Cota com código BZ0000020, recebida do pipefy, não encontrada no banco do MD Cota."
            },
        )

    @classmethod
    def test_bpm_quota_with_timeout_error_if_bpm_request_has_timeout(
        cls,
        requests_mock: Mocker,
        bpm_quota_request_data: Dict[str, Any],
        client: TestClient,
    ) -> None:
        requests_mock.post(BPMClient().url_create_quota, exc=Timeout("Created Timeout"))

        response = client.post(
            url=cls.endpoint, json=bpm_quota_request_data, headers=cls.headers
        )

        assert (response.status_code, response.json()) == (
            status.HTTP_408_REQUEST_TIMEOUT,
            {
                "detail": "Requisição para https://staging.bazardoconsorcio.com.br/api/internal/md_cotas/quotas "
                "gerou timeout após 10 segundos."
            },
        )

    @classmethod
    def test_bpm_quota_with_too_may_redirects_error_if_bpm_request_fails(
        cls,
        requests_mock: Mocker,
        bpm_quota_request_data: Dict[str, Any],
        client: TestClient,
    ) -> None:
        requests_mock.post(
            BPMClient().url_create_quota,
            exc=TooManyRedirects("Created TooManyRedirects"),
        )

        response = client.post(
            url=cls.endpoint, json=bpm_quota_request_data, headers=cls.headers
        )

        assert (response.status_code, response.json()) == (
            status.HTTP_429_TOO_MANY_REQUESTS,
            {
                "detail": "Requisição para https://staging.bazardoconsorcio.com.br/api/internal/md_cotas/quotas "
                "excedeu redirecionamentos."
            },
        )

    @classmethod
    def test_bpm_quota_with_internal_error_if_bpm_request_raises_http_error(
        cls,
        requests_mock: Mocker,
        bpm_quota_request_data: Dict[str, Any],
        client: TestClient,
    ) -> None:
        requests_mock.post(
            BPMClient().url_create_quota, exc=HTTPError("Created HTTPError")
        )

        response = client.post(
            url=cls.endpoint, json=bpm_quota_request_data, headers=cls.headers
        )

        assert (response.status_code, response.json()) == (
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            {
                "detail": "Requisição para https://staging.bazardoconsorcio.com.br/api/internal/md_cotas/quotas "
                "gerou erro HTTP: ('Created HTTPError',)"
            },
        )

    @classmethod
    def test_bpm_quota_with_internal_error_if_bpm_response_is_not_ok(
        cls,
        requests_mock: Mocker,
        bpm_quota_request_data: Dict[str, Any],
        client: TestClient,
    ) -> None:
        requests_mock.post(
            BPMClient().url_create_quota,
            status_code=status.HTTP_403_FORBIDDEN,
            text="Forbidden",
        )

        response = client.post(
            url=cls.endpoint, json=bpm_quota_request_data, headers=cls.headers
        )

        assert (response.status_code, response.json()) == (
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            {
                "detail": "Requisição para criar cota no BPM não retornou 202: 403 - Forbidden"
            },
        )

    @classmethod
    def test_bpm_quota_with_success(
        cls,
        requests_mock: Mocker,
        bpm_quota_request_data: Dict[str, Any],
        client: TestClient,
    ) -> None:
        requests_mock.post(
            BPMClient().url_create_quota, status_code=status.HTTP_202_ACCEPTED
        )

        response = client.post(
            url=cls.endpoint, json=bpm_quota_request_data, headers=cls.headers
        )

        assert (response.status_code, response.json()) == (
            status.HTTP_201_CREATED,
            {"quota_code": "BZ0000018"},
        )
