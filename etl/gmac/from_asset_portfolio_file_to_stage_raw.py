import pandas as pd
import sys
import json
import numpy as np
import boto3
import msoffcrypto

from io import BytesIO
from unidecode import unidecode
from typing import List, Tuple
from datetime import datetime

from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from bazartools.common.logger import get_logger
from bazartools.common.database.glueConnection import GlueConnection


logger = get_logger()

args = getResolvedOptions(
        sys.argv,
        [
            "bucket_name",
            "file_key",
            "event_bus_name",
            "secret_name_file_password"
        ],
    )

# Execução local
# args = {
#    "bucket_name": "cota-adms-sandbox",
#    "file_key": "gmac/asset-portfolio/received/partner-report-2023-11-10.xlsx",
#    "event_bus_name": "md",
#    "secret_name_file_password": "GMAC-File-Password"
# }


def get_file_data() -> Tuple[pd.DataFrame, str, str]:
    logger.info("Obtendo evento.")
    logger.info(f"Argumentos obtidos: {args}")

    bucket_name = args["bucket_name"]
    file_key = args["file_key"]
    secret_name = args["secret_name_file_password"]
    logger.info(f"Obtendo arquivo {file_key} do bucket {bucket_name}")

    secrets_manager_client = boto3.client("secretsmanager")
    response = secrets_manager_client.get_secret_value(SecretId=secret_name)
    password = json.loads(response["SecretString"])['password']

    try:
        s3_client = boto3.client("s3")
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=file_key,
        )
        data = response['Body'].read()
        logger.info("Arquivo obtido.")

        msoffcrypto_obj = msoffcrypto.OfficeFile(BytesIO(data))
        msoffcrypto_obj.load_key(password=password)
        unprotected_excel_content = BytesIO()
        msoffcrypto_obj.decrypt(unprotected_excel_content)

        df = pd.read_excel(unprotected_excel_content, header=1)
        return df, bucket_name, file_key
    except ClientError as error:
        logger.exception(
            f"Erro ao buscar arquivo {file_key} no bucket {bucket_name}: {error}"
        )
        raise error


def process_columns(df: pd.DataFrame) -> List[dict]:
    logger.info("Mapeando colunas do Dataframe para o banco e convertendo tipos.")
    renamed_columns = {}

    for column in df.columns:
        new_column = unidecode(str(column)).lower().replace(" ", "_")
        renamed_columns[column] = new_column

    df.rename(columns=renamed_columns, inplace=True)

    df['data_nascimento'] = pd.to_datetime(df['data_nascimento'], format='%d/%m/%Y')
    df['data_nascimento'] = df['data_nascimento'].dt.date

    for column in ["data_adesao", "data_venda", "data_situacao", "data_contemplacao", "data_entrega_bem"]:
        df[column] = pd.to_datetime(df[column], format='%d/%m/%Y')
    df['data_info'] = datetime.now()

    df.replace({np.nan: None}, inplace=True)

    logger.info(f"Colunas mapeadas: {df.columns}")
    return df.to_dict(orient="records")


def insert(rows: List[dict]) -> None:
    logger.info(f"Preparando inserção de {len(rows)} linhas.")
    values = ''
    colum_names = ''
    for column in rows[0].keys():
        colum_names += f'{column}, '
        values += f'%({column})s, '
    colum_names = colum_names[:-2]
    values = values[:-2]

    logger.debug("Obtendo conexão para inserção no DB.")
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()

    with md_quota_connection.cursor() as cursor:
        try:
            sql_query = f'INSERT INTO stage_raw.tb_quotas_gmac_pos ({colum_names}) VALUES ({values})'
            logger.debug(f"Executando query:\n{sql_query}")
            cursor.executemany(sql_query, rows)
            md_quota_connection.commit()
            logger.info("Dados inseridos com sucesso.")
        except Exception as error:
            logger.error(f"Erro na inserção na tabela de tb_quotas_gmac_pos no stage_raw, error:{error}")
            raise error
        finally:
            cursor.close()
            md_quota_connection.close()
            logger.info("Conexão com o banco finalizada.")


def move_processed_file(bucket_name: str, file_key: str) -> None:
    logger.info("MOvendo arquivo para diretório de processado no S3.")
    s3_client = boto3.client("s3")
    target_key = file_key.replace("received/", "processed/")
    copy_source = {'Bucket': bucket_name, 'Key': file_key}

    try:
        s3_client.copy_object(
            Bucket=bucket_name, CopySource=copy_source, Key=target_key)
        s3_client.delete_object(Bucket=bucket_name, Key=file_key)
        logger.info("Arquivo movido com sucesso.")
    except ClientError as error:
        logger.exception(
            f"Erro ao copiar objeto do bucket {bucket_name}/{file_key} "
            f"para bucket {bucket_name}{target_key}: {error}"
        )
        raise error


def put_event():
    etl_name = "quota_ingestion_gmac_pos"
    logger.info(f"Iniciando criação do evento para ETL {etl_name}")
    event_bus_name = args["event_bus_name"]

    entry = {
        "Source": "glue",
        "DetailType": etl_name,
        "EventBusName": event_bus_name,
        "Detail": '{"success": "OK"}'
    }
    try:
        response = boto3.client("events").put_events(Entries=[entry])
        logger.info(
            f"Resposta da publicação: {response['ResponseMetadata']['HTTPStatusCode']}"
        )
    except ClientError as client_error:
        message = f"Comportamento inesperado ao tentar publicar o evento.{client_error}"
        logger.error(message)
        raise client_error


def start():
    df, bucket_name, file_key = get_file_data()
    rows = process_columns(df)
    insert(rows)
    move_processed_file(bucket_name, file_key)
    put_event()


if __name__ == "__main__":
    start()
