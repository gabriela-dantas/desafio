import os
import boto3
import json

from typing import Dict, Union, Callable, Any, List
from fastapi import HTTPException
from botocore.exceptions import ClientError
from datetime import date, timedelta
from collections import defaultdict

from common.event_schemas.webhook_event_schema import WebhookEventSchema
from simple_common.logger import logger
from common.clients.bpm import BPMClient
from common.clients.consorciei import ConsorcieiClient
from common.clients.cubees import CubeesClient
from common.repositories.md_cota.quota_view import QuotaViewRepository
from common.exceptions import InternalServerError, EntityNotFound


class EventFlow:
    DATA = "data"
    BPM_RESPONSE = "bpm_response"
    QUOTA_CREATION_CALL = "quota_creation_call"
    CUSTOMER_CREATION_CALL = "customer_creation_call"
    CONTACT_UPDATE = "contact_update"
    COMPANY_BOND_CALL = "company_bond_call"
    LIFE_PROOF_LINK_CALL = "life_proof_link_call"
    SANTANDER_CODE = "0000000234"
    API_MAX_RETRIES = 2
    CHANNEL_TYPE = "EMAIL"
    EVENT_TYPE = "Event"
    RESPONSE_TYPE = "RequestResponse"

    def __init__(self, event: WebhookEventSchema) -> None:
        self.__event = event
        self.__logger = logger

        self.__lambda_client = boto3.client("lambda")
        self.__bpm_client = BPMClient()
        self.__consorciei_client = ConsorcieiClient()
        self.__cubees_client = CubeesClient()

        self.__quota_view_repository = QuotaViewRepository()

        self.__quota_creation_lambda = os.environ["QUOTA_CREATION_LAMBDA_NAME"]
        self.__cubees_customer_lambda = os.environ["CUBEES_CUSTOMER_LAMBDA_NAME"]
        self.__company_bond_lambda = os.environ["COMPANY_BOND_LAMBDA_NAME"]
        self.__life_proof_link_lambda = os.environ["LIFE_PROOF_LINK_LAMBDA_NAME"]

        self.__result: Dict[str, Union[int, bool]] = {
            self.BPM_RESPONSE: 0,
            self.QUOTA_CREATION_CALL: False,
            self.CUSTOMER_CREATION_CALL: False,
            self.CONTACT_UPDATE: False,
            self.COMPANY_BOND_CALL: False,
            self.LIFE_PROOF_LINK_CALL: False,
        }

        self.__bind_cnpj = ""

    @staticmethod
    def __get_document_expiration_date() -> str:
        return (date.today() + timedelta(days=365 * 10)).strftime("%Y-%m-%d")

    def __call_api(
            self, method: Callable[..., Any], request_params: Any
    ) -> Dict[str, Any]:
        errors = {}

        for attempt in range(self.API_MAX_RETRIES):
            try:
                return method(request_params)
            except HTTPException as http_error:
                errors[f"{attempt + 1}"] = http_error.detail
                self.__logger.debug(
                    f"Falha ao chamar API na tentativa {attempt + 1}: {http_error.detail}"
                )

        raise InternalServerError(
            f"Máximo de {self.API_MAX_RETRIES} "
            f"tentativas excedido ao chamar API: {errors}"
        )

    def __invoke_lambda(
            self, lambda_name: str, invocation_type: str, payload: dict
    ) -> Dict[str, Any]:
        try:
            self.__logger.debug(f"A invocar Lambda {lambda_name}...")
            json_payload = json.dumps(payload)

            response: dict = self.__lambda_client.invoke(
                FunctionName=lambda_name,
                InvocationType=invocation_type,
                Payload=json_payload,
            )

            response["Payload"] = response["Payload"].read().decode("utf-8")

            if response.get("FunctionError"):
                message = f"Lambda {lambda_name} foi invocada mas retornou erro: {response['Payload']}"
                self.__logger.error(message)
                raise InternalServerError(message)

            response = {
                "StatusCode": response["StatusCode"],
                "Payload": response["Payload"],
            }
            self.__logger.debug(
                f"Invocação da Lambda {lambda_name} concluída: {response}"
            )
            return response

        except ClientError as client_error:
            message = (
                f"Não foi possível invocar Lambda {lambda_name}: {client_error.args}"
            )
            self.__logger.error(message, exc_info=client_error)
            raise InternalServerError(message)

    def __get_quota_from_api(self) -> Dict[str, Any]:
        return self.__call_api(
            self.__consorciei_client.get_quota_details, self.__event.payload.shareId
        )

    def __get_quota_by_contract(self, contract_number: str) -> dict:
        try:
            return self.__quota_view_repository.get_quota_code_by_contract(
                contract_number, self.SANTANDER_CODE
            )
        except EntityNotFound:
            message = (
                f"Cota Santander com contrato {contract_number} não existe no MD Cota!"
            )
            self.__logger.critical(message)
            raise EntityNotFound(message)

    def __set_address_and_contact(self, data: dict, is_pj: bool) -> dict:
        address_and_contact = defaultdict(list)
        contacts_key = "contacts"

        if is_pj:
            address_category = "COMM"
            contact_category = "BUSINESS"
        else:
            address_category = "RESI"
            contact_category = "PERSONAL"

        try:
            phone = data["phone"][-11:]
            if len(phone) == 11 or int(phone[-8]) >= 6:
                phone_type = "MOBILE"
            else:
                phone_type = "LANPHONE"

            address_and_contact[contacts_key].append(
                {
                    "contact_desc": "TELEFONE",
                    "contact": data["phone"],
                    "contact_category": contact_category,
                    "contact_type": phone_type,
                    "preferred_contact": False,
                }
            )
        except (KeyError, TypeError):
            self.__logger.info("Cliente recuperado da Consorciei não possui telefone.")

        try:
            email = data["email"].replace("", "")
            address_and_contact[contacts_key].append(
                {
                    "contact_desc": "EMAIL",
                    "contact": email,
                    "contact_category": contact_category,
                    "contact_type": "EMAIL",
                    "preferred_contact": False,
                },
            )
            # Se existir telefone será o contato preferido, do contrário é o email.
            address_and_contact[contacts_key][0]["preferred_contact"] = True
        except (KeyError, TypeError, AttributeError):
            self.__logger.info("Cliente recuperado da Consorciei não possui email.")

        try:
            address = data["address"]
            addresses = {
                "address": address["address"],
                "address_2": address["complement"],
                "street_number": address["number"],
                "district": address["district"],
                "zip_code": address["zip"],
                "address_category": address_category,
                "city": address["city"],
                "state": address["state"],
            }
            address_and_contact["addresses"].append(addresses)
        except (KeyError, TypeError) as error:
            self.__logger.info(
                f"Cliente recuperado da Consorciei não possui endereço "
                f"com campos esperados.: {error}"
            )

        return dict(address_and_contact)

    @staticmethod
    def verify_amount_character(value: str) -> bool:
        return value is not None and value != ""

    def check_data(self, identifier: str, name: str) -> None:
        if not (self.verify_amount_character(identifier) and self.verify_amount_character(name)):
            message = (
                f"Consorciei enviou dado inválido. Dado enviado, "
                f"Identificador: {identifier}, nome: {name}"
            )
            self.__logger.error(message)
            raise InternalServerError(message)

    def __set_legal_person(self, data: dict) -> dict:
        self.check_data(data['cnpj'], data['name'])
        data_legal_person = {
            "person_ext_code": data["cnpj"],
            "person_type": "LEGAL",
            "administrator_code": self.SANTANDER_CODE,
            "channel_type": self.CHANNEL_TYPE,
            "legal_person": {
                "company_name": data["name"],
                "company_tax_number": data["cnpj"],
            },
            "documents": [
                {
                    "document_number": data["cnpj"],
                    "expiring_date": self.__get_document_expiration_date(),
                    "person_document_type": "CS",
                }
            ],
            "reactive": False,
        }
        return data_legal_person

    def __set_natural_person(self, data: dict) -> dict:
        self.check_data(data['cpf'], data['name'])
        customer_data = {
            "person_ext_code": data["cpf"],
            "person_type": "NATURAL",
            "administrator_code": self.SANTANDER_CODE,
            "channel_type": self.CHANNEL_TYPE,
            "natural_person": {"full_name": data["name"], "tax_code": data["cpf"]},
            "reactive": False,
        }

        if data.get("rg"):
            customer_data["documents"] = [
                {
                    "document_number": data.get("rg"),
                    "expiring_date": self.__get_document_expiration_date(),
                    "person_document_type": "RG",
                }
            ]

        return customer_data

    def __get_customer_data_for_representatives(self) -> List[dict]:
        customers = []

        representatives = self.__call_api(
            self.__consorciei_client.get_representatives,
            self.__event.payload.shareId,
        )

        try:
            for representative in representatives[self.DATA]:
                customer = self.__set_natural_person(representative)
                contact_and_address = self.__set_address_and_contact(
                    representative, False
                )
                customer.update(**contact_and_address)
                customers.append(customer)
        except (KeyError, TypeError) as error:
            message = (
                "Representantes obtidos da API Consorciei não possuem "
                f"os campos esperados para mapear cliente: {error}"
            )
            self.__logger.error(message)
            raise InternalServerError(message)

        return customers

    def __get_customer_data_for_company(self) -> dict:
        try:
            company = self.__call_api(
                self.__consorciei_client.get_company_details,
                self.__event.payload.shareId,
            )

            customer = self.__set_legal_person(company[self.DATA])
            contact_and_address = self.__set_address_and_contact(
                company[self.DATA], True
            )
            customer.update(**contact_and_address)
        except (KeyError, TypeError) as error:
            message = (
                f"Empresa recebida da API Consorciei "
                f"não possuem os campos esperados para mapear cliente: {error}"
            )
            self.__logger.error(message)
            raise InternalServerError(message)

        return customer

    def __create_customer(self, is_pj: bool, quota_id: int) -> None:
        if is_pj:
            self.__logger.debug("Dono da cota é pessoa jurídica.")
            customer = self.__get_customer_data_for_company()

            ownership_percentage = 1
            main_owner = customer["person_ext_code"]
            customers = [customer]
        else:
            self.__logger.debug("Dono da cota é pessoa física.")
            customers = self.__get_customer_data_for_representatives()

            ownership_percentage = round(1 / len(customers), 4)
            main_owner = customers[0]["person_ext_code"]

        payload = {
            "quota_id": quota_id,
            "ownership_percentage": ownership_percentage,
            "main_owner": main_owner,
            "cubees_request": customers,
        }

        self.__invoke_lambda(self.__cubees_customer_lambda, self.RESPONSE_TYPE, payload)
        self.__result[self.CUSTOMER_CREATION_CALL] = True

    def __create_quota(self, quota_code: str) -> None:
        payload = {
            "detail": {
                "quota_code_list": [
                    {"quota_code": quota_code, "share_id": self.__event.payload.shareId}
                ]
            }
        }

        self.__invoke_lambda(self.__quota_creation_lambda, self.EVENT_TYPE, payload)
        self.__result[self.QUOTA_CREATION_CALL] = True

    def __process_for_selected_proposal(self) -> None:
        self.__logger.debug("Executando etapas para PROPOSTA SELECIONADA")
        api_quota = self.__get_quota_from_api()

        try:
            contract_number: str = api_quota[self.DATA]["contract"]
            is_pj: bool = api_quota[self.DATA]["isPj"]
        except KeyError as error:
            message = f"Cota recebida da API Consorciei não possui os campos esperados: {error}"
            self.__logger.error(message)
            raise InternalServerError(message)

        db_quota = self.__get_quota_by_contract(contract_number)
        self.__create_customer(is_pj, db_quota["quota_id"])
        self.__create_quota(db_quota["quota_code"])

    def __update_customer_data(self, is_pj: bool) -> None:
        self.__logger.debug("Atualizando dados do dono da cota no Cubees..")
        if is_pj:
            self.__logger.debug("Dono da cota é pessoa jurídica.")
            customer = self.__get_customer_data_for_company()

            self.__cubees_client.create_customer(customer)
            self.__bind_cnpj = customer["person_ext_code"]
        else:
            self.__logger.debug("Dono da cota é pessoa física.")
            customers = self.__get_customer_data_for_representatives()
            for customer in customers:
                self.__cubees_client.create_customer(customer)

        self.__result[self.CONTACT_UPDATE] = True

    def __link_company_representatives(self) -> None:
        self.__logger.debug("Fazendo vínculo de representantes da empresa...")
        customers = self.__get_customer_data_for_representatives()
        payload = {
            "cnpj": self.__bind_cnpj,
            "bond_type": "PARTNER",
            "representatives": customers,
        }

        self.__invoke_lambda(self.__company_bond_lambda, self.RESPONSE_TYPE, payload)
        self.__result[self.COMPANY_BOND_CALL] = True

    def __invoke_life_proof_link(self) -> None:
        payload = {"shareId": self.__event.payload.shareId}

        self.__invoke_lambda(self.__life_proof_link_lambda, self.EVENT_TYPE, payload)
        self.__result[self.LIFE_PROOF_LINK_CALL] = True

    def __process_for_signed_by_seller(self) -> None:
        self.__logger.debug("Executando etapas para CONTRATO ASSINADO PELO VENDEDOR")
        api_quota = self.__get_quota_from_api()

        try:
            is_pj: bool = api_quota[self.DATA]["isPj"]
        except KeyError as error:
            message = f"Cota recebida da API Consorciei não possui campo isPj como esperados: {error}"
            self.__logger.error(message)
            raise InternalServerError(message)

        self.__update_customer_data(is_pj)

        if is_pj:
            self.__link_company_representatives()

        self.__invoke_life_proof_link()

    def __process_event_by_status(self) -> None:
        process_function_by_status: Dict[str, Callable] = {
            "PROPOSAL_SELECTED": self.__process_for_selected_proposal,
            "CONTRACT_SIGNED_BY_SELLER": self.__process_for_signed_by_seller,
        }

        try:
            process_function = process_function_by_status[self.__event.action]
        except KeyError:
            self.__logger.debug(
                f"Status {self.__event.action} não possui ação a ser executada."
            )
            return

        process_function()

    def start(self) -> Dict[str, Union[int, bool]]:
        request_body = {
            "share_id": self.__event.payload.shareId,
            "status": self.__event.action,
        }

        self.__logger.debug(f"Enviando atualização de status para BPM: {request_body}")
        self.__result[self.BPM_RESPONSE] = self.__bpm_client.update_santander_status(
            request_body
        )
        self.__logger.debug(
            f"Status de resposta o BPM: {self.__result['bpm_response']}"
        )

        self.__process_event_by_status()

        return self.__result.copy()
