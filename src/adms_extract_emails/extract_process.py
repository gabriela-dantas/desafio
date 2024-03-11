import os
import time
import boto3
import email
import io
import pandas as pd
import numpy as np
import base64
import json

from email.message import Message
from typing import Dict, Any, List
from urllib.parse import unquote_plus
from botocore.exceptions import ClientError
from unidecode import unidecode
from pandas import DataFrame
from datetime import datetime

from common.exceptions import UnprocessableEntity, InternalServerError
from simple_common.logger import logger
from common.repositories.abstract_repository import AbstractRepository
from common.repositories.groups_gmac import GroupsGMACRepository
from common.repositories.quotas_gmac_pre import QuotasGMACPreRepository
from common.repositories.customers_gmac_pre import CustomersGMACPreRepository
from common.repositories.quotas_santander_pre import QuotasSantanderPreRepository
from common.repositories.groups_santander_pre import GroupsSantanderPreRepository
from common.repositories.bids_santander_pre import BidsSantanderPreRepository
from common.repositories.prize_draw_santander_pre import PrizeDrawSantanderPreRepository
from common.repositories.quotas_volks_pre import QuotasVolksPreRepository
from common.repositories.groups_volks_pre import GroupsVolksPreRepository
from common.repositories.bids_volks_pre import BidsVolksPreRepository
from common.repositories.assets_volks_pre import AssetsVolksPreRepository
from common.repositories.customers_volks_pre import CustomersVolksPrePreRepository
from common.repositories.contemplados_itau import ContempladosItauRepository
from adms_extract_emails.constantes import EXCEL_TYPE, CSV_TYPE

from common.database.session import db_session


class ExtractProcess:
    def __init__(self, event: Dict[str, Any]) -> None:
        self.__logger = logger
        self.__s3_client = boto3.client("s3")
        self.__put_event_bridge = boto3.client("events")

        self.__source_bucket = os.environ["SOURCE_BUCKET"]
        self.__bus_name = os.environ["BUS_NAME"]
        self.__extract_s3_key = ""
        self.__adm_name = ""
        self.__file_type = ""
        self.__processed_s3_key = "email-processed"
        self.__file_name = ""
        self.__excel_type = f"application/{EXCEL_TYPE}"
        self.__csv_type = f"text/{CSV_TYPE}"
        self.__data_info: datetime

        self.__get_extract_s3_key(event)

        self.__extracts_by_adm: Dict[str, Dict[str, List[AbstractRepository]]] = {
            "gmac": {
                "Base": [QuotasGMACPreRepository(), CustomersGMACPreRepository()],
                "bens": [GroupsGMACRepository()],
            },
            "santander": {
                "Valores": [GroupsSantanderPreRepository()],
                "Lances": [BidsSantanderPreRepository()],
                "Sorteios": [PrizeDrawSantanderPreRepository()],
            },
            "itau": {"contemplados": [ContempladosItauRepository()]},
        }

    def __get_extract_s3_key(self, event: Dict[str, Any]) -> None:
        self.__logger.debug("Obtendo chave do S3 do evento...")

        try:
            ses_records = event["Records"][0]
            message_id = ses_records["ses"]["mail"]["messageId"]
            adm_name = ses_records["ses"]["mail"]["destination"][0]
            self.__adm_name = adm_name.split("@")[0]

            self.__extract_s3_key = unquote_plus(f"{self.__adm_name}/{message_id}")
            self.__logger.debug(f"Obtida chave do S3: {self.__extract_s3_key}")

        except (KeyError, IndexError, TypeError) as error:
            message = f"Falha ao tentar obter chave do S3 no evento recebido: {error}"
            self.__logger.error(message, exc_info=error)
            raise UnprocessableEntity(message)

    def __set_date_info(self) -> None:
        file_date = self.__file_name.split(".")[0][-8:]
        date_format_list = ["%d%m%Y", "%Y%m%d"]
        for item in date_format_list:
            try:
                self.__data_info = datetime.strptime(file_date, item)
                return
            except ValueError:
                continue
        message = (
            f"Nome {self.__file_name} não contém data nos formatos de data mapeados."
        )
        self.__logger.error(message)
        raise UnprocessableEntity(message)

    @classmethod
    def __get_standardized_columns(cls, columns: List[str]) -> Dict[str, str]:
        standardized_columns = {}
        db_columns = {"vl_percpago": "vl_perc_pago"}

        for column in columns:
            new_column = column.lower().strip().replace(" ", "_")
            new_column = unidecode(new_column)
            try:
                new_column = db_columns[new_column]
            except KeyError:
                pass

            standardized_columns[column] = new_column

        return standardized_columns

    def __convert_float_columns_to_datetime(self, df: DataFrame) -> None:
        df_columns = set(df.columns)
        expected_columns = {
            "dt_contempla",
            "dt_cancelamento_calculada_vc",
            "dt_desistencia",
            "dt_cancel_cota",
            "dt_prim_assembl_grupo",
            "dt_ult_assembleia",
        }

        self.__logger.debug(
            f"Colunas consideradas para converter "
            f"de float para datetime: {expected_columns}"
        )
        target_columns = df_columns.intersection(expected_columns)
        target_columns = list(target_columns)
        self.__logger.debug(
            f"Colunas obtidas do Dataframe para conversão: {target_columns}"
        )

        df[target_columns] = df[target_columns].apply(
            pd.to_datetime, errors="coerce", unit="D", origin="1899-12-30"
        )
        self.__logger.debug("Todas as colunas convertidas para datetime.")

    def __get_mine_from_s3(self) -> Message:
        try:
            self.__logger.debug("Obtendo arquivo do S3...")
            response = self.__s3_client.get_object(
                Bucket=self.__source_bucket, Key=self.__extract_s3_key
            )
        except ClientError as client_error:
            message = f"Falha ao tentar obter arquivo do S3: {client_error.args}"
            self.__logger.error(message, exc_info=client_error)
            raise InternalServerError(message)

        self.__logger.debug("Lendo body de resposta...")
        mime_data = response["Body"].read()

        self.__logger.debug("Convertendo bytes para objeto de mensagem de email...")
        return email.message_from_bytes(mime_data)

    def __get_data_from_mine(self, message: Message) -> bytes:
        self.__logger.debug(
            f"Obtendo dados do arquivo {self.__file_name} "
            f"a partir do objeto de mensagem..."
        )

        for part in message.walk():
            if part.get_content_type() in [self.__excel_type, self.__csv_type]:
                self.__logger.debug(
                    f"part.get_content_type(): {part.get_content_type()}"
                )
                self.__file_name = part.get_filename()
                self.__file_type = part.get_content_type()
                self.__set_date_info()
                payload = part.get_payload()

                self.__logger.debug(f"Obtidos dados do arquivo: {self.__file_name}")
                return base64.b64decode(payload)

    def __format_numbers(self, values_list: List, df: DataFrame) -> DataFrame:
        for item in values_list:
            df[item] = df[item].astype(str).apply(self.__formatar_numero)
        return df

    def __format_dates(self, values_list: List, df: DataFrame) -> DataFrame:
        for item in values_list:
            df[item] = pd.to_datetime(df[item], dayfirst=True, format="%d/%m/%Y")
        return df

    def __treat_dataframe(self, df):
        new_columns = self.__get_standardized_columns(df.columns.values.tolist())

        self.__logger.debug(f"Feito mapeamento de colunas: {new_columns}")

        df.rename(columns=new_columns, inplace=True)
        df["data_info"] = self.__data_info

        self.__convert_float_columns_to_datetime(df)

        self.__logger.debug("Tratando valores nulos...")
        df = df.replace(np.nan, "", regex=True)
        df.replace({pd.NaT: None}, inplace=True)
        df = df.fillna("")

        self.__logger.debug(
            "Convertendo colunas para string e substituindo valores por regex..."
        )

        df = df.astype(str).replace(r"^R\$", "", regex=True)

        df = (
            df.astype(str)
            .replace(r"\.0+$", "", regex=True)
            .replace(r"^\s*$", None, regex=True)
        )

        return df

    @staticmethod
    def __formatar_numero(numero):
        return numero.replace(".", "").replace(",", ".")

    def __read_excel_data(self, file_content: bytes) -> Dict[str, List[dict]]:
        self.__logger.debug("Lendo dados do arquivo como Excel Sheets...")
        entities_by_extract: Dict[str, List[dict]] = {}

        excel_data = io.BytesIO(file_content)
        if self.__file_type == self.__csv_type:
            df = pd.read_csv(excel_data, sep=";")
            df = self.__treat_dataframe(df)

            columns_list_values = [
                "pe_lance",
                "vl_credito",
                "vl_credito_atualizado",
                "vl_fgts",
                "vl_lance",
                "vl_lance_embutido",
                "vl_lance_pago",
                "vl_recurso_proprio",
            ]

            columns_list_date = ["dt_adesao", "dt_contemplacao"]

            self.__format_numbers(columns_list_values, df)

            self.__format_dates(columns_list_date, df)

            extract_name = f"{self.__file_name}".replace(" ", "_")
            extract_name = unidecode(extract_name)

            entities_by_extract[extract_name] = df.to_dict(orient="records")

            self.__logger.debug(f"Arquivo {extract_name} convertido para dict.")

        else:
            df_sheets = pd.ExcelFile(excel_data)

            for sheet in df_sheets.sheet_names:
                self.__logger.debug(
                    f"Lendo sheet {sheet} do arquivo "
                    f"{self.__file_name} e convertendo para dict..."
                )
                df: DataFrame = df_sheets.parse(sheet)
                df = self.__treat_dataframe(df)

                fields_to_format = ["parcela_pj", "valor_bem", "parcela_pf"]

                for field in fields_to_format:
                    if field in df.columns:
                        df[field] = df[field].astype(str).apply(self.__formatar_numero)

                extract_name = f"{self.__file_name}{sheet}".replace(" ", "_")
                extract_name = unidecode(extract_name)

                entities_by_extract[extract_name] = df.to_dict(orient="records")

                self.__logger.debug(f"Sheet {sheet} convertida para dict.")

        self.__logger.debug("Leitura e conversão de todas as Sheets finalizadas.")
        return entities_by_extract

    def __upload_processed_extract(self, file_content: bytes) -> None:
        file_key = f"{self.__adm_name}/{self.__processed_s3_key}/{self.__file_name}"

        try:
            self.__logger.debug(
                f"Salvando extrato processado no bucket "
                f"{self.__source_bucket}: Key: {file_key}..."
            )
            self.__s3_client.put_object(
                Bucket=self.__source_bucket,
                Key=file_key,
                Body=file_content,
                ContentType=self.__excel_type,
            )
        except ClientError as client_error:
            message = f"Falha ao tentar salvar extrato no S3: {client_error.args}"
            self.__logger.error(message, exc_info=client_error)
            raise InternalServerError(message)

    def __filter_rows_by_not_null_columns(
        self, entities: List[dict], repository: AbstractRepository
    ) -> List[dict]:
        self.__logger.debug("Filtrando linhas com colunas obrigatórias nulas...")

        columns = repository.get_model_columns()
        not_null_columns = []

        # obter colunas que não podem ser nulas, desconsiderando chave primária, pois sempre são autoincrement.
        for column in columns:
            if (column.nullable is False) and (column.primary_key is False):
                not_null_columns.append(column.name)

        self.__logger.debug(
            f"Tabela: {repository.get_model_name()}, "
            f"colunas obrigatórias: {not_null_columns}"
        )

        def check_columns(entity: dict) -> bool:
            not_null = True

            for required_column in not_null_columns:
                not_null = not_null and (entity.get(required_column) is not None)

            return not_null

        filtered_entities = list(filter(check_columns, entities))
        self.__logger.debug(
            f"Obtidas {len(filtered_entities)} "
            f"das {len(entities)} entidades, após o filtro."
        )
        return filtered_entities

    def __insert_entities(
        self,
        extract_name: str,
        entities: List[dict],
        repositories_by_extract: Dict[str, List[AbstractRepository]],
    ) -> None:
        self.__logger.debug(f"Obtendo repositórios para extrato {extract_name}...")

        for key, repositories in repositories_by_extract.items():
            if key in extract_name:
                for repository in repositories:
                    start = time.time()
                    self.__logger.debug(
                        f"A salvar {len(entities)} "
                        f"entidades {repository.get_model_name()}"
                    )

                    filtered_entities = self.__filter_rows_by_not_null_columns(
                        entities, repository
                    )
                    repository.create_many(filtered_entities, commit_at_the_end=False)

                    end = time.time() - start
                    self.__logger.debug(
                        f"{len(filtered_entities)} entidades "
                        f"{repository.get_model_name()} salvas em {end} segundos."
                    )

                return

        message = f"Extrato {extract_name} não possui repositório mapeado."
        self.__logger.error(message)
        raise InternalServerError(message)

    def __save_entities(
        self, entities_by_extract: Dict[str, List[dict]], file_content: bytes
    ) -> None:
        self.__logger.debug("Iniciando armazenamento de entidades...")

        try:
            repositories_by_extract = self.__extracts_by_adm[self.__adm_name]
        except KeyError as key_error:
            message = f"ADM {self.__adm_name} não possui extratos mapeados para armazenamento."
            self.__logger.error(message, exc_info=key_error)
            raise InternalServerError(message)

        for extract_name, entities in entities_by_extract.items():
            self.__insert_entities(extract_name, entities, repositories_by_extract)

        # Commit somente após salvar entidades de todas as Sheets do extratoe e fazer upload no S3.
        self.__upload_processed_extract(file_content)
        db_session.commit()

    def __put_event(self):
        event_detail = ""
        if self.__adm_name == "itau":
            event_detail = "from-email-to-md-cota-bids-itau"
        self.__logger.debug("Iniciando criação do evento")
        event_bus_name = self.__bus_name
        event_source = "lambda"
        event_detail_type = event_detail
        event_detail = {"workflow": "md-cota_itau_bid_ingestion"}
        if event_detail != "":
            try:
                self.__logger.debug("Criando evento...")
                event = [
                    {
                        "Source": event_source,
                        "DetailType": event_detail_type,
                        "Detail": json.dumps(event_detail),
                        "EventBusName": event_bus_name,
                    }
                ]
                self.__logger.debug(event)
                response = self.__put_event_bridge.put_events(Entries=event)

                if response["FailedEntryCount"] == 0:
                    self.__logger.debug("Evento criado com sucesso.")

                else:
                    self.__logger.error(
                        f'Falha ao tentar criar o evento: {response["Entries"][0]["ErrorCode"]}'
                    )

            except Exception as e:
                self.__logger.error(f"Error sending event: {str(e)}")
                return {"statusCode": 500, "body": json.dumps(f"Error: {str(e)}")}
        else:
            self.__logger.debug("Evento não criado. Adm não é Itaú!")

    def start(self) -> None:
        start = time.time()

        message = self.__get_mine_from_s3()
        file_content = self.__get_data_from_mine(message)
        entities_by_extract = self.__read_excel_data(file_content)
        self.__save_entities(entities_by_extract, file_content)
        self.__put_event()

        end = time.time() - start
        self.__logger.debug(f"Execução concluída em {end} segundos.")
