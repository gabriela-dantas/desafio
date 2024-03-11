from common.clients.bpm import BPMClient
from common.clients.consorciei import ConsorcieiClient
from common.clients.cubees import CubeesClient
from common.event_schemas.life_proof_link_sender import ShareIdSchema
from common.exceptions import InternalServerError
from simple_common.logger import logger


class LambdaLifeProofLinkSender:
    def __init__(self) -> None:
        self.__logger = logger
        self.__generate_link_magic = "/customers/public/magic_links/"
        self.__consorciei_client = ConsorcieiClient()
        self.__cubees_client = CubeesClient()
        self.__bpm_client = BPMClient()

    def invoke(self, event_data: ShareIdSchema) -> None:
        share_id = event_data.shareId
        representatives = self.__consorciei_client.get_representatives(share_id)
        for signer in representatives["data"]:
            person_ext_code = {"person_ext_code": signer["cpf"]}
            data_customer = self.__cubees_client.get_customer(person_ext_code)
            try:
                token = data_customer["person_type_data"]["token"]
            except KeyError:
                message = (
                    "A chave token não foi encontrada na resposta da API do cubees"
                )
                self.__logger.error(message)
                raise InternalServerError(message)

            link_magic = self.__create_link(token)
            signer_id = signer["signerId"]
            self.__consorciei_client.send_link_proof_life(
                share_id, signer_id, {"url": link_magic}
            )

        self.__logger.debug(
            f"Envio de link de prova de vida para o Share_id:{share_id}, executado com sucesso."
        )

    def __create_link(self, token: str) -> str:
        return f"{self.__bpm_client.endpoint_url}{self.__generate_link_magic}{token}"
