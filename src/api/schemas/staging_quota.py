from pydantic import BaseModel
from typing import List, Dict, Any
from api.schemas.http_error import HTTPErrorSchema


class QuotasAPICreateSchema(BaseModel):
    quotas: List[Dict[str, Any]]
    administrator: str
    endpoint_generator: str

    class Config:
        schema_extra = {
            "example": {
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
        }


class QuotasAPIBatchSchema(BaseModel):
    message: str

    class Config:
        schema_extra = {
            "example": {"message": "Volume de 50 entidades inserido com sucesso!"}
        }


class QuotasBatchConflictError(HTTPErrorSchema):
    class Config:
        schema_extra = {
            "example": {
                "detail": "Erro de integridade ao tentar inserir volume de entidades tb_quotas_api"
            }
        }


class QuotasBatchInternalError(HTTPErrorSchema):
    class Config:
        schema_extra = {
            "example": {
                "detail": "Comportamento inesperado ao tentar inserir volume de entidades tb_quotas_api"
            }
        }
