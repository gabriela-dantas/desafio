import pytest
from botocore.exceptions import ClientError

from fastapi import status
from typing import Dict, Any
from fastapi.testclient import TestClient

from api.services.quotas_api import QuotasAPIService

from unittest.mock import patch


@pytest.fixture(scope="function")
def raw_quota_request_data() -> Dict[str, Any]:
    return {
        "quotas": [
            {
                "referenceId": "d54ddf8a-f79b-4346-909f-7027d818a543",
                "group": 89,
                "number": 821,
                "quotaSituation": "DESISTENTES",
                "ownerDocument": "02373584840",
                "ownerName": "EUDES APARECIDO ANDRADE",
                "ownerAddressStreet": "RUA XAVANTES",
                "ownerAddressNumber": "106",
                "ownerAddressComplement": "",
                "ownerAddressNeighborhood": "PARQUE XINGU",
                "ownerRemainingPlots": 63,
                "ownerMonthlyInsurance": 384.88,
                "ownerWithdrawInsurance": "S",
                "ownerStatusCota": "CONTATO_ACEITO",
                "ownerStatusContemplation": "Não Contemplada",
                "ownerContemplationDate": "1900-01-01T00:00:00",
                "ownerContemplationForm": None,
                "ownerAddressCity": "LINS",
                "ownerAddressState": "SP",
                "ownerAddressZipCode": "00016400",
                "ownerEmail": "eudes.andrade@bertinenergia.com.br                ",
                "ownerPhones": [
                    {"countryCode": "55", "areaCode": 14, "number": 35231525},
                    {"countryCode": "55", "areaCode": 14, "number": 35333051},
                    {"countryCode": "55", "areaCode": 14, "number": 981378141},
                ],
                "productType": "IMÓVEIS",
                "groupSituation": "A",
                "acquisitionDate": "2012-08-28",
                "lastAssemblyDate": "2021-08-17",
                "totalValue": 400000,
                "totalUpdatedValue": 687705.95,
                "exportDate": "2021-08-28 09:21:20",
                "delayDays": 0,
                "updatedDebitBalanceValue": 0,
                "outstandingBalance": 0,
                "administrationFee": 17,
                "paidAdministrationFee": 0.64,
                "parcelCurrentValue": 5209.83,
                "reserveFundPercentage": 3,
                "parcelPlanValue": 825247.14,
                "valueEndGroup": 4243.25,
                "valueEndGroupWithoutTaxes": 4992.06,
                "commonFundPaid": 2903.9,
                "percCommonFundPaid": 0.73,
                "vacantGroup": 24,
                "quantityPlots": 180,
                "quantityPaidPlots": 2,
                "quantityDelayPlots": 0,
                "safeStatus": True,
                "bidsBidPercentual": 51.76,
                "totalPlanValue": 480000,
                "totalPlanPaidValue": 5572.78,
                "debtorValue": 0,
                "quantityPlotsGroup": 180,
                "reserveFundPaid": 140.36,
                "commonAdminFoundDebtor": 816311.99,
                "proposedNumber": 809207,
                "ownerType": "INDIVIDUAL",
                "ownerBirthday": "1961-07-16",
                "deletionDate": "2012-11-06",
                "endGroupDate": "2026-09-28",
                "inputChannel": "TELEMETRIA",
                "numberReceived": "5514981378141",
            }
        ],
        "administrator": "ITAU",
        "endpoint_generator": "POST /quotas",
    }


@pytest.fixture(scope="function")
def too_many_raw_quota_request_data(
    raw_quota_request_data: Dict[str, Any]
) -> Dict[str, Any]:
    quotas = []

    while len(quotas) <= QuotasAPIService().creation_limit_size:
        quotas.extend(raw_quota_request_data["quotas"])

    raw_quota_request_data["quotas"] = quotas

    return raw_quota_request_data


class TestStagingQuota:
    endpoint = "/staging/quota"

    headers = {"x-api-key": "dummy"}

    @classmethod
    def test_staging_quota_with_forbidden_if_api_key_is_missing(
        cls, raw_quota_request_data: Dict[str, Any], client: TestClient
    ) -> None:
        response = client.post(url=cls.endpoint, json=raw_quota_request_data)

        assert (response.status_code, response.json()) == (
            status.HTTP_403_FORBIDDEN,
            {"detail": "Not authenticated"},
        )

    @classmethod
    def test_staging_quota_with_validation_error_if_quotas_is_not_list(
        cls, raw_quota_request_data: Dict[str, Any], client: TestClient
    ) -> None:
        raw_quota_request_data["quotas"] = raw_quota_request_data["quotas"][0]

        response = client.post(
            url=cls.endpoint, json=raw_quota_request_data, headers=cls.headers
        )

        assert (response.status_code, response.json()["detail"]) == (
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            [
                {
                    "loc": ["body", "quotas"],
                    "msg": "value is not a valid list",
                    "type": "type_error.list",
                }
            ],
        )

    @classmethod
    def test_staging_quota_with_validation_error_if_quota_is_not_dict(
        cls, raw_quota_request_data: Dict[str, Any], client: TestClient
    ) -> None:
        raw_quota_request_data["quotas"][0] = 1

        response = client.post(
            url=cls.endpoint, json=raw_quota_request_data, headers=cls.headers
        )

        assert (response.status_code, response.json()["detail"]) == (
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            [
                {
                    "loc": ["body", "quotas", 0],
                    "msg": "value is not a valid dict",
                    "type": "type_error.dict",
                }
            ],
        )

    @classmethod
    def test_staging_quota_with_semantic_error_if_quotas_list_exceeds_limit(
        cls, too_many_raw_quota_request_data: Dict[str, Any], client: TestClient
    ) -> None:
        response = client.post(
            url=cls.endpoint, json=too_many_raw_quota_request_data, headers=cls.headers
        )

        assert (response.status_code, response.json()) == (
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            {
                "detail": "Lista de cotas a serem criadas ultrapassou o limite de 512 unidades."
            },
        )

    @classmethod
    def test_stating_quota_with_error_if_event_put_client_error(
        cls, raw_quota_request_data: Dict[str, Any], client: TestClient
    ) -> None:
        expected_message = {
            "detail": "Comportamento inesperado ao tentar publicar o evento.An "
            "error occurred (500) when calling the put_event operation: Access Denied"
        }
        with patch("boto3.client") as mock_boto:
            mock_boto.return_value.put_events.side_effect = ClientError(
                {"Error": {"Code": "500", "Message": "Access Denied"}}, "put_event"
            )
            response = client.post(
                url=cls.endpoint, json=raw_quota_request_data, headers=cls.headers
            )

        assert (response.status_code, response.json()) == (
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            expected_message,
        )

    @classmethod
    def test_staging_quota_with_created(
        cls, raw_quota_request_data: Dict[str, Any], client: TestClient
    ) -> None:
        with patch("boto3.client") as mock_boto:
            mock_put_events_response = {"FailedEntryCount": 0, "Entries": []}
            mock_boto.return_value.put_events.return_value = mock_put_events_response

            response = client.post(
                url=cls.endpoint, json=raw_quota_request_data, headers=cls.headers
            )

            assert (response.status_code, response.json()) == (
                status.HTTP_201_CREATED,
                {"message": "Volume de 1 entidades inserido com sucesso!"},
            )
