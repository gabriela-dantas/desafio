from typing import List

from common.clients.cubees import CubeesClient
from common.event_schemas.company_bond_event_schema import CompanyBondEventSchema
from simple_common.logger import logger


class LamdaCompanyBond:
    def __init__(self) -> None:
        self.__logger = logger
        self.__cubees_client = CubeesClient()

    def invoke(self, event_data: CompanyBondEventSchema) -> None:
        self.__logger.debug("Vinculando clientes aos sócios ou empresas.")

        bond_type = event_data.bond_type
        person_bonds: List[dict] = []
        for customer in event_data.representatives:
            self.__logger.debug("Cliente que será criado", customer)
            person_code = self.__cubees_client.create_customer(customer)
            person_bonds.append(
                {"related_person_code": person_code, "related_person_type": bond_type}
            )

        self.__logger.debug(
            "todos os clientes serão vinculados ao seguinte cnpj", event_data.cnpj
        )

        for person in person_bonds:
            self.__logger.debug("Criação de vínculo para:", person)
            self.__cubees_client.related_person(
                person, {"person_ext_code": event_data.cnpj}
            )

        self.__logger.debug("Clientes vinculados com sucesso.")
