from typing import List, Dict, Tuple

from common.exceptions import InternalServerError
from common.repositories.md_cota.quota import QuotaRepository
from cubees_customer.customer_schema import CustomerDataSchema
from common.clients.cubees import CubeesClient
from common.repositories.md_cota.quota_owner import QuotaOwnerRepository
from common.repositories.md_cota.quota_person_type import QuotaPersonTypeRepository
from common.constants import person_type
from simple_common.logger import logger


class CustomerProcessing:
    def __init__(self, customer_data: CustomerDataSchema) -> None:
        self.__customer_data = customer_data
        self.__main_owner_person_code = ""
        self.__cubees_client = CubeesClient()
        self.__quota_owner_repository = QuotaOwnerRepository()
        self.__quota_person_type_repository = QuotaPersonTypeRepository()
        self.__quota_repository = QuotaRepository()
        self.__logger = logger

    def __create_cubees_customer(self) -> set:
        created_person_codes = set()

        for i, customer in enumerate(self.__customer_data.cubees_request):
            self.__logger.debug(
                f"Chamando Cubees para criar {i+1} de "
                f"{len(self.__customer_data.cubees_request)} clientes recebidos."
            )

            person_code = self.__cubees_client.create_customer(customer)

            if customer.get("person_ext_code", "") == self.__customer_data.main_owner:
                self.__main_owner_person_code = person_code
                self.__logger.debug(
                    f"Main owner encontrado: person_ext_code: "
                    f"{customer.get('person_ext_code')}, person_code: {person_code}"
                )
            else:
                self.__logger.debug(
                    f"person_ext_code: {customer.get('person_ext_code')} não é Main owner."
                )

            created_person_codes.add(person_code)

        self.__logger.debug("Todos os clientes criados no Cubees.")
        return created_person_codes

    def __get_not_registered_owners(
        self, created_person_codes: set
    ) -> Tuple[set, float]:
        existing_owners = {"person_codes": set(), "total_ownership_percentage": 0}

        retrieved_owners = self.__quota_owner_repository.find_by_quota(
            self.__customer_data.quota_id
        )
        self.__logger.debug(
            f"Obtidos {len(retrieved_owners)}"
            f"owners para quota_id: {self.__customer_data.quota_id}"
        )

        for owner in retrieved_owners:
            existing_owners["person_codes"].add(owner.person_code)
            existing_owners["total_ownership_percentage"] += owner.ownership_percent

        target_owners = created_person_codes.difference(existing_owners["person_codes"])
        self.__logger.debug(
            f"De {len(created_person_codes)} clientes recebidos no evento, "
            f"{len(target_owners)} não estão cadastrados como owners."
        )

        return target_owners, existing_owners["total_ownership_percentage"]

    def __verify_and_create(
        self, target_owners: set, total_ownership_percentage: float
    ) -> List[Dict[str, str]]:
        created_owners = []
        total_ownership_percentage += self.__customer_data.ownership_percentage * len(
            target_owners
        )

        if total_ownership_percentage > 1.0:
            self.__logger.debug(
                f"ownership_percentage de clientes enviados somada a dos "
                f"owners cadastrados para quota_id {self.__customer_data.quota_id}"
                f"é maior do que 100. owners cadastrados serão invalidados."
            )

            self.__quota_owner_repository.invalidate_by_quota(
                self.__customer_data.quota_id, False
            )

        for person_code in target_owners:
            is_man_owner = person_code == self.__main_owner_person_code

            quota_owner = {
                "ownership_percent": self.__customer_data.ownership_percentage,
                "quota_id": self.__customer_data.quota_id,
                "person_code": person_code,
                "main_owner": is_man_owner,
            }

            self.__quota_owner_repository.create(quota_owner)
            created_owners.append({"person_code": person_code})

        return created_owners

    def create(self) -> List[Dict[str, str]]:
        created_person_codes = self.__create_cubees_customer()
        target_owners, total_ownership_percentage = self.__get_not_registered_owners(
            created_person_codes
        )
        response_create = self.__verify_and_create(
            target_owners, total_ownership_percentage
        )
        self.update_person_type()
        return response_create

    def update_person_type(self) -> None:
        customer = self.__customer_data.cubees_request[0]
        try:
            type_person = person_type[customer["person_type"]]
            type_id = self.__quota_person_type_repository.get_by_status(
                type_person
            ).quota_person_type_id
            self.__quota_repository.update_person_type_id(
                self.__customer_data.quota_id, type_id
            )
        except KeyError as error:
            message = f"Conteudo de chave inválida em person_type, conteúdo:{error}"
            self.__logger.error(message)
            raise InternalServerError(message)
