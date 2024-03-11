from bazartools.common.logger import get_logger
from bazartools.common.database.glueConnection import GlueConnection
import pandas as pd
from pandas import DataFrame
import numpy as np
from unidecode import unidecode
from typing import Dict, List
from awsglue.utils import getResolvedOptions
from datetime import datetime
import sys
import boto3
from botocore.exceptions import ClientError
import json

BATCH_SIZE = 2000

GLUE_DEFAULT_CODE = 2

today = datetime.today()

logger = get_logger()


def put_event(event_bus_name, event_detail_type):
    logger.info("Iniciando criação do evento")
    event_source = "glue"
    event_detail_type = event_detail_type
    event_detail = {
        "start": True,
    }
    entry = {
        "Source": event_source,
        "DetailType": event_detail_type,
        "Detail": json.dumps(event_detail),
        "EventBusName": event_bus_name,
    }
    try:
        logger.info("Criando evento...")
        response = boto3.client("events").put_events(Entries=[entry])
        logger.info(
            f"Resposta da publicação: {response['ResponseMetadata']['HTTPStatusCode']}"
        )
    except ClientError as client_error:
        message = f"Comportamento inesperado ao tentar publicar o evento.{client_error}"
        logger.error(message)
        raise client_error


def set_date_info(file_name) -> datetime:
    file_date = file_name.split(".")[0][-8:]
    try:
        data_info = datetime.strptime(file_date, "%Y%m%d")
        return data_info
    except ValueError:
        message = (
            f"Nome {file_name} não contém data no formato %Y%m%d nos últimos 8 digitos."
        )
        logger.error(message)
        data_info = datetime.utcnow()
        return data_info


def get_standardized_columns(columns: List[str]) -> Dict[str, str]:
    standardized_columns = {}

    for column in columns:
        logger.info(column.lower().strip().replace(" ", "_"))
        new_column = column.lower().strip().replace(" ", "_")
        new_column = unidecode(new_column)
        logger.info(f"new column {new_column}")

        standardized_columns[column] = new_column

    return standardized_columns


def try_parsing_date(value):
    valid_date_formats = (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%y-%m-%d",
        "%d/%m/%y",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%y-%m-%d %H:%M:%S",
        "%d/%m/%y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%d/%m/%Y %H:%M:%S.%f",
        "%y-%m-%d %H:%M:%S.%f",
        "%d/%m/%y %H:%M:%S.%f",
    )
    if value is None:
        return value

    for fmt in valid_date_formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    raise ValueError(f"Nenhum formato de data válido encontrado para {value}")


def insert_new_quota_stage_raw(new_quota, md_quota_cursor):
    try:
        logger.info("Inserindo dados na tabela tb_quotas_santander_pre")
        query_insert_new_asset = f"""
            INSERT
            INTO
            stage_raw.tb_quotas_santander_pre
            (
            cd_grupo,
            cd_cota,
            nr_contrato,
            vl_devolver,
            vl_bem_atual,
            cd_produto,
            pc_fc_pago,
            dt_canc,
            dt_venda,
            pz_restante_grupo,
            qt_parcela_a_pagar,
            nm_situ_entrega_bem,
            pc_fr_pago,
            pc_tx_adm,
            pc_tx_pago,
            pz_contratado,
            qt_parcela_paga,
            pc_fundo_reserva,
            pz_decorrido_grupo,
            data_info,
            is_processed ,
            created_at,
            cd_versao_cota,
            cd_tipo_pessoa,
            dt_entrega_bem,
            dt_contemplacao,
            vl_lance_embutido,
            vl_bem_corrigido,
            vl_total_contrato,
            vl_bem_entregue,
            vl_bem_a_entregar,
            pc_seguro,
            vl_lance_proprio,
            qt_pc_atraso,
            pz_comercializacao,
            qt_pc_lance
            )
            VALUES(
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
            )
            """
        md_quota_cursor.execute(
            query_insert_new_asset,
            (
                new_quota["cd_grupo"],
                new_quota["cd_cota"],
                new_quota["nr_contrato"],
                new_quota["vl_devolver"],
                new_quota["vl_bem_atual"],
                new_quota["cd_produto"],
                new_quota["pc_fc_pago"],
                new_quota["dt_canc"],
                new_quota["dt_venda"],
                new_quota["pz_restante_grupo"],
                new_quota["qt_parcela_a_pagar"],
                new_quota["nm_situ_entrega_bem"],
                new_quota["pc_fr_pago"],
                new_quota["pc_tx_adm"],
                new_quota["pc_tx_pago"],
                new_quota["pz_contratado"],
                new_quota["qt_parcela_paga"],
                new_quota["pc_fundo_reserva"],
                new_quota["pz_decorrido_grupo"],
                new_quota["data_info"],
                False,
                "now()",
                new_quota["cd_versao_cota"],
                new_quota["cd_tipo_pessoa"],
                new_quota["dt_entrega_bem"],
                new_quota["dt_contemplacao"],
                new_quota["vl_lance_embutido"],
                new_quota["vl_bem_corrigido"],
                new_quota["vl_total_contrato"],
                new_quota["vl_bem_entregue"],
                new_quota["vl_bem_a_entregar"],
                new_quota["pc_seguro"],
                new_quota["vl_lance_proprio"],
                new_quota["qt_pc_atraso"],
                new_quota["pz_comercializacao"],
                new_quota["qt_pc_lance"],
            ),
        )
        logger.info("Dados inseridos com sucesso!")
    except Exception as error:
        logger.error(
            f"Falha ao tentar inserir dados na tabela tb_quotas_santander_pre: {error}"
        )
        raise error


def formatar_numero(numero):
    return numero.replace(".", "").replace(",", ".")


def format_numbers(values_list: List, df: DataFrame) -> DataFrame:
    for item in values_list:
        if item in df.columns:
            df[item] = df[item].astype(str).apply(formatar_numero)
            df[item] = pd.to_numeric(df[item], errors='coerce')
    return df


def treat_dataframe(df):
    new_columns = get_standardized_columns(df.columns.values.tolist())

    logger.info(f"Feito mapeamento de colunas: {new_columns}")

    df.rename(columns=new_columns, inplace=True)

    df.dropna(how='all', inplace=True)

    columns_list_date = ["dt_venda", "dt_canc", "dt_entrega_bem", "dt_contemplacao"]
    logger.info(f"Antes tratamento número: {df['vl_lance_embutido']}")
    format_dates(columns_list_date, df)

    columns_list_values = [
        "vl_bem_atual",
        "vl_devolver",
        "pc_fc_pago",
        "pc_fr_pago",
        "pc_tx_adm",
        "pc_tx_pago",
        "pc_fundo_reserva",
        "vl_lance_embutido",
        "vl_bem_corrigido",
        "vl_total_contrato",
        "vl_bem_entregue",
        "vl_bem_a_entregar",
        "pc_seguro",
        "vl_lance_proprio",
    ]

    format_numbers(columns_list_values, df)

    logger.info(f"Depois tratamento número: {df['vl_lance_embutido']}")
    logger.info("Tratando valores nulos...")
    df = df.replace({np.nan: None}, regex=True)
    df.replace({pd.NaT: None}, inplace=True)
    logger.info(f"Depois tratamento nulo: {df['vl_lance_embutido']}")

    return df


def upload_processed_extract(file_name, bucket_name) -> None:
    file_key = f"santander/csv-processed/{file_name.split('/')[2]}"

    try:
        logger.info(
            f"Salvando extrato processado no bucket "
            f"{bucket_name}: Key: {file_key}..."
        )
        copy_source = {"Bucket": bucket_name, "Key": file_name}

        boto3.client("s3").copy_object(
            Bucket=bucket_name, CopySource=copy_source, Key=file_key
        )

        # Exclui o objeto original
        boto3.client("s3").delete_object(Bucket=bucket_name, Key=file_name)
    except ClientError as client_error:
        message = f"Falha ao tentar salvar extrato no S3: {client_error.args}"
        logger.error(message, exc_info=client_error)


def format_dates(values_list: List, df: DataFrame) -> DataFrame:
    for item in values_list:
        df[item] = pd.to_datetime(df[item])
    return df


def read_excel_data(s3_path, file_name, md_quota_cursor):
    logger.info("Lendo dados do arquivo como Excel Sheets...")
    data_info = set_date_info(file_name)

    df = pd.read_csv(s3_path, sep=";", low_memory=False)
    df = treat_dataframe(df)

    extract_name = f"{file_name}".replace(" ", "_")

    for index, row in df.iterrows():
        new_quota = {
            "cd_grupo": row["cd_grupo"],
            "cd_cota": row["cd_cota"],
            "nr_contrato": row["nr_contrato"],
            "vl_devolver": row["vl_devolver"],
            "vl_bem_atual": row["vl_bem_atual"],
            "cd_produto": row["cd_produto"],
            "pc_fc_pago": row["pc_fc_pago"],
            "dt_canc": row["dt_canc"],
            "dt_venda": row["dt_venda"],
            "pz_restante_grupo": row["pz_restante_grupo"],
            "qt_parcela_a_pagar": row["qt_parcela_a_pagar"],
            "nm_situ_entrega_bem": row["nm_situ_entrega_bem"],
            "pc_fr_pago": row["pc_fr_pago"],
            "pc_tx_adm": row["pc_tx_adm"],
            "pc_tx_pago": row["pc_tx_pago"],
            "pz_contratado": row["pz_contratado"],
            "qt_parcela_paga": row["qt_parcela_paga"],
            "pc_fundo_reserva": row["pc_fundo_reserva"],
            "pz_decorrido_grupo": row["pz_decorrido_grupo"],
            "data_info": data_info,
            "cd_versao_cota": row["cd_versao_cota"],
            "cd_tipo_pessoa": row["cd_tipo_pessoa"],
            "dt_entrega_bem": row["dt_entrega_bem"],
            "dt_contemplacao": row["dt_contemplacao"],
            "vl_lance_embutido": row["vl_lance_embutido"],
            "vl_bem_corrigido": row["vl_bem_corrigido"],
            "vl_total_contrato": row["vl_total_contrato"],
            "vl_bem_entregue": row["vl_bem_entregue"],
            "vl_bem_a_entregar": row["vl_bem_a_entregar"],
            "pc_seguro": row["pc_seguro"],
            "vl_lance_proprio": row["vl_lance_proprio"],
            "qt_pc_atraso": row["qt_pc_atraso"],
            "pz_comercializacao": row["pz_comercializacao"],
            "qt_pc_lance": row["qt_pc_lance"]
        }
        insert_new_quota_stage_raw(new_quota, md_quota_cursor)


def from_email_to_stage_raw_santander():
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()
    md_quota_cursor = md_quota_connection.cursor()
    # args = getResolvedOptions(
    #     sys.argv, ["path_s3", "bucket_s3", "file_name", "event_bus_name"]
    # )
    # event_bus_name = args["event_bus_name"]
    # s3_path = args["path_s3"]
    # bucket_name = args["bucket_s3"]
    # file_name = args["file_name"]
    s3_path = 's3://cota-adms-sandbox/santander/csv/CotasSantander_20231110.csv'
    file_name = 'santander/csv/CotasSantander_20231110.csv'
    bucket_name = 'cota-adms-sandbox'

    try:
        read_excel_data(s3_path, file_name, md_quota_cursor)
        upload_processed_extract(file_name, bucket_name)
        # put_event(event_bus_name, event_detail_type)
        md_quota_connection.commit()
        logger.error("Transaction successfully commited:")

    except Exception as error:
        # Rollback the transaction in case of an error
        md_quota_connection.rollback()
        logger.error("Transaction rolled back due to an error:", error)
        raise error

    finally:
        # Close the cursor and connection
        md_quota_cursor.close()
        md_quota_connection.close()
        logger.info("Connection closed.")


if __name__ == "__main__":
    from_email_to_stage_raw_santander()
