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


def get_table_dict(cursor, rows):
    column_names = [desc[0] for desc in cursor.description]

    # Create a list of dictionaries using list comprehension
    return [dict(zip(column_names, row)) for row in rows]


def get_list_by_id(id_item: str, data_list: list, field_name: str) -> list:
    items = []
    for item_list in data_list:
        if item_list[field_name] == id_item:
            items.append(item_list)
    return items


def cd_grupo_right_justified(group: str) -> str:
    if len(str(group)) == 5:
        code_group = str(group)
    else:
        code_group = str(group).rjust(5, "0")
    return code_group


def get_dict_by_id(id_item: str, data_list: list, field_name: str):
    for item_list in data_list:
        if item_list[field_name] == id_item:
            return item_list
    return None


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
    db_columns = {
        "vl_percentual_pago": "vl_perc_pago",
        "digito": "cd_digito",
        "cpf_cnpj": "cd_cpf_cnpj",
        "bem_basico": "ds_bem_basico",
        "nome_do_cliente": "nm_pessoa",
        "e_mail": "ds_endereco_eml_p",
        "telefone": "ds_numero_tel_cel",
        "regional": "ds_regional",
        "tipo_de_pessoa": "tp_pes_pes",
        "dt_cancelamento": "dt_cancel_cota",
        "pe_taxa_adm": "vl_taxa_adm",
        "pe_taxa_fundo_reserva": "vl_tx_fundo_reserva",
        "dt_primeira_assembleia_grupo": "dt_prim_assembl_grupo",
        "dt_ultima_assembleia_grupo": "dt_ult_assembleia",
        "no_assembleia_atual": "nr_assembleia_vigente",
        "prazo_grupo": "nr_prazo_grupo",
        "pz_cota": "nr_prazo_cota",
        "no_primeira_assembleia_cota": "nr_prim_assembl_particip",
        "plano_venda": "nr_plano",
        "vl_bem_atualizado": "vl_bem_basico_atu",
        "vl_percentual_mensal": "vl_percmes",
        "vl_percentual_atraso": "vl_percatr",
        "grupo": "pkni_grupo",
        "vl_bem_menor_valor": "valor_do_bem1",
        "vl_bem_maior_valor": "valor_do_bem2",
        "produto": "atsv_descrbem",
        "cd_grupo": "cd_grupo",
        "dt_assembleia": "atdt_captacao",
        "pe_lance": "atnd_percamortz",
        "vl_lance": "atnd_lancebruto",
    }

    for column in columns:
        logger.info(column.lower().strip().replace(" ", "_"))
        new_column = column.lower().strip().replace(" ", "_")
        new_column = unidecode(new_column)
        logger.info(f"new column {new_column}")
        try:
            new_column = db_columns[new_column]
        except KeyError:
            pass

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


def insert_new_group_stage_raw(new_group, md_quota_cursor):
    try:
        logger.info("Inserindo dados na tabela tb_grupos_volks_pre")
        query_insert_new_group = f"""
            INSERT
            INTO
            stage_raw.tb_grupos_volks_pre
            (
            grupo,
            primeira_ass,
            prazo,
            ult_ass,
            participantes,
            nro_ass_atual,
            vagas,
            data_encerramento,
            data_info,
            is_processed,
            created_at        
            )
            VALUES(
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
            )
            """
        md_quota_cursor.execute(
            query_insert_new_group,
            (
                new_group["grupo"],
                new_group["primeira_ass"],
                new_group["prazo"],
                new_group["ult_ass"],
                new_group["participantes"],
                new_group["nro_ass_atual"],
                new_group["vagas"],
                new_group["data_encerramento"],
                new_group["data_info"],
                False,
                "now()",
            ),
        )
        logger.info("Dados inseridos com sucesso!")
    except Exception as error:
        logger.error(
            f"Falha ao tentar inserir dados na tabela tb_grupos_volks_pre: {error}"
        )
        raise error


def insert_new_quota_stage_raw(new_quota, md_quota_cursor):
    try:
        logger.info("Inserindo dados na tabela tb_quotas_volks_pre")
        query_insert_new_quota = f"""
            INSERT
            INTO
            stage_raw.tb_quotas_volks_pre
            (
            cd_grupo,
            cd_cota,
            cd_digito,
            ds_bem_basico,
            tp_pes_pes,
            dt_cancel_cota,
            vl_taxa_adm,
            vl_tx_fundo_reserva,
            dt_prim_assembl_grupo,
            dt_ult_assembleia,
            nr_assembleia_vigente,
            nr_prazo_grupo,
            nr_prazo_cota,
            nr_prim_assembl_particip,
            nr_plano,
            vl_bem_basico_atu,
            vl_percatr,
            vl_percmes,
            vl_perc_pago,
            data_info,
            contrato,
            bem_objeto,
            fabricacao,
            vl_credito,
            pcl_a_pagar,
            vl_pcl_atual,
            pe_seguro_vida,
            marca,
            qt_participantes,
            pe_seg_quebra_garantia,
            pe_sd_devedor,
            vl_sd_devedor,
            is_processed,
            created_at      
            )
            VALUES(
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s
            )
            """
        md_quota_cursor.execute(
            query_insert_new_quota,
            (
                new_quota["cd_grupo"],
                new_quota["cd_cota"],
                new_quota["cd_digito"],
                new_quota["ds_bem_basico"],
                new_quota["tp_pes_pes"],
                new_quota["dt_cancel_cota"],
                new_quota["vl_taxa_adm"],
                new_quota["vl_tx_fundo_reserva"],
                new_quota["dt_prim_assembl_grupo"],
                new_quota["dt_ult_assembleia"],
                new_quota["nr_assembleia_vigente"],
                new_quota["nr_prazo_grupo"],
                new_quota["nr_prazo_cota"],
                new_quota["nr_prim_assembl_particip"],
                new_quota["nr_plano"],
                new_quota["vl_bem_basico_atu"],
                new_quota["vl_percatr"],
                new_quota["vl_percmes"],
                new_quota["vl_perc_pago"],
                new_quota["data_info"],
                new_quota["contrato"],
                new_quota["bem_objeto"],
                new_quota["fabricacao"],
                new_quota["vl_credito"],
                new_quota["pcl_a_pagar"],
                new_quota["vl_pcl_atual"],
                new_quota["pe_seguro_vida"],
                new_quota["marca"],
                new_quota["qt_participantes"],
                new_quota["pe_seg_quebra_garantia"],
                new_quota["pe_sd_devedor"],
                new_quota["vl_sd_devedor"],
                False,
                "now()",
            ),
        )
        logger.info("Dados inseridos com sucesso!")
    except Exception as error:
        logger.error(
            f"Falha ao tentar inserir dados na tabela tb_quotas_volks_pre: {error}"
        )
        raise error


def insert_new_customer_stage_raw(new_customer, md_quota_cursor):
    try:
        logger.info("Inserindo dados na tabela tb_clientes_volks_pre")
        query_insert_new_customer = f"""
            INSERT
            INTO
            stage_raw.tb_clientes_volks_pre
            (
            cd_grupo,
            cd_cota,
            cd_digito,
            cd_cpf_cnpj,
            nm_pessoa,
            ds_numero_tel_cel,
            ds_endereco_eml_p,
            dt_nascimento,
            data_info,
            is_processed,
            created_at        
            )
            VALUES(
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
            )
            """
        md_quota_cursor.execute(
            query_insert_new_customer,
            (
                new_customer["cd_grupo"],
                new_customer["cd_cota"],
                new_customer["cd_digito"],
                new_customer["cd_cpf_cnpj"],
                new_customer["nm_pessoa"],
                new_customer["ds_numero_tel_cel"],
                new_customer["ds_endereco_eml_p"],
                new_customer["dt_nascimento"],
                new_customer["data_info"],
                False,
                "now()",
            ),
        )
        logger.info("Dados inseridos com sucesso!")
    except Exception as error:
        logger.error(
            f"Falha ao tentar inserir dados na tabela tb_clientes_volks_pre: {error}"
        )
        raise error


def formatar_numero(numero):
    return numero.replace(".", "").replace(",", ".")


def format_numbers(values_list: List, df: DataFrame) -> DataFrame:
    for item in values_list:
        if item in df.columns:
            df[item] = df[item].astype(str).apply(formatar_numero)
    return df


def upload_processed_extract(file_name, bucket_name) -> None:
    file_key = f"volkswagen/processed/{file_name.split('/')[1]}"

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


def read_excel_data(s3_path, file_name, md_quota_cursor):
    logger.info("Lendo dados do arquivo como Excel Sheets...")

    data_info = set_date_info(file_name)

    df_sheets = pd.ExcelFile(s3_path)

    for sheet in df_sheets.sheet_names:
        logger.info(
            f"Lendo sheet {sheet} do arquivo " f"{file_name} e convertendo para dict..."
        )
        df: DataFrame = df_sheets.parse(sheet_name=sheet, header=5)

        logger.info(f"Dataframe {df.columns.values.tolist()}")
        new_columns = get_standardized_columns(df.columns.values.tolist())

        logger.info(f"Feito mapeamento de colunas: {new_columns}")

        df.rename(columns=new_columns, inplace=True)
        df["data_info"] = data_info
        df.drop(columns=["unnamed:_4"], inplace=True)
        df.drop(columns=["unnamed:_5"], inplace=True)
        df.drop(columns=["unnamed:_8"], inplace=True)
        df.drop(columns=["unnamed:_9"], inplace=True)

        logger.info(f"Novo Dataframe {df.columns.values.tolist()}")

        logger.info("Tratando valores nulos...")
        df = df.replace(np.nan, "", regex=True)
        df.replace({pd.NaT: None}, inplace=True)

        df = df.fillna("")

        logger.info(
            "Convertendo colunas para string e substituindo valores por regex..."
        )
        df = df.astype(str).replace(r"^R\$", "", regex=True)

        df = (
            df.astype(str)
            .replace(r"\.0+$", "", regex=True)
            .replace(r"^\s*$", None, regex=True)
        )

        columns_list_values = [
            "vl_credito",
            "vl_pcl_atual",
            "vl_sd_devedor",
            "atnd_percamortz",
            "pe_seguro_vida",
            "pe_sd_devedor",
        ]

        format_numbers(columns_list_values, df)
        logger.info(f"sheet: {sheet}")
        logger.info("Conversão finalizada colunas para string e regex...")

        if sheet == "Relatorio SPBAZAR":
            event_detail_type = "from_sftp_to_stage_raw_volks-quotas"
            logger.info("leu arquivo")
            groups_appended = []

            for index, row in df.iterrows():
                logger.info(f"linha dataframe {row}")
                new_quota = {
                    "cd_grupo": row["cd_grupo"][-5:],
                    "cd_cota": row["cd_cota"],
                    "cd_digito": row["cd_digito"],
                    "ds_bem_basico": row["ds_bem_basico"],
                    "tp_pes_pes": row["tp_pes_pes"],
                    "dt_cancel_cota": datetime.strptime(
                        row["dt_cancel_cota"], "%d/%m/%Y"
                    ),
                    "vl_taxa_adm": row["vl_taxa_adm"].replace(",", "."),
                    "vl_tx_fundo_reserva": row["vl_tx_fundo_reserva"].replace(",", "."),
                    "dt_prim_assembl_grupo": datetime.strptime(
                        row["dt_prim_assembl_grupo"], "%d/%m/%Y"
                    ),
                    "dt_ult_assembleia": datetime.strptime(
                        row["dt_ult_assembleia"], "%d/%m/%Y"
                    ),
                    "nr_assembleia_vigente": row["nr_assembleia_vigente"],
                    "nr_prazo_grupo": row["nr_prazo_grupo"],
                    "nr_prazo_cota": row["nr_prazo_cota"],
                    "nr_prim_assembl_particip": row["nr_prim_assembl_particip"],
                    "nr_plano": row["nr_plano"],
                    "vl_bem_basico_atu": row["vl_bem_basico_atu"],
                    "vl_percatr": row["vl_percatr"].replace(",", "."),
                    "vl_percmes": row["vl_percmes"].replace(",", "."),
                    "vl_perc_pago": row["vl_perc_pago"].replace(",", "."),
                    "contrato": row["contrato"],
                    "data_info": row["data_info"],
                    "bem_objeto": row["bem_objeto"],
                    "fabricacao": row["fabricacao"],
                    "vl_credito": row["vl_credito"],
                    "pcl_a_pagar": row["pcl_a_pagar"],
                    "vl_pcl_atual": row["vl_pcl_atual"],
                    "pe_seguro_vida": row["pe_seguro_vida"].replace(",", ".")
                    if row["pe_seguro_vida"] != "None"
                    else 0,
                    "marca": row["marca"],
                    "qt_participantes": row["qt_participantes"],
                    "pe_seg_quebra_garantia": row["pe_seg_quebra_garantia"].split(" ")[
                        0
                    ],
                    "pe_sd_devedor": row["pe_sd_devedor"],
                    "vl_sd_devedor": row["vl_sd_devedor"],
                }
                new_customer = {
                    "cd_grupo": row["cd_grupo"][-5:],
                    "cd_cota": row["cd_cota"],
                    "cd_digito": row["cd_digito"],
                    "cd_cpf_cnpj": row["cd_cpf_cnpj"],
                    "nm_pessoa": row["nm_pessoa"],
                    "ds_numero_tel_cel": row["ds_numero_tel_cel"],
                    "ds_endereco_eml_p": row["ds_endereco_eml_p"],
                    "data_info": row["data_info"],
                    "dt_nascimento": try_parsing_date(row["dt_nascimento"]),
                }
                if row["cd_grupo"] in groups_appended:
                    pass
                else:
                    new_group = {
                        "grupo": row["cd_grupo"],
                        "primeira_ass": datetime.strptime(
                            row["dt_prim_assembl_grupo"], "%d/%m/%Y"
                        ),
                        "prazo": row["nr_prazo_grupo"],
                        "ult_ass": datetime.strptime(
                            row["dt_ult_assembleia"], "%d/%m/%Y"
                        ),
                        "participantes": row["qt_participantes"],
                        "nro_ass_atual": row["nr_assembleia_vigente"],
                        "vagas": row["qtd_cotas_vagas"],
                        "data_encerramento": datetime.strptime(
                            row["dt_ult_assembleia"], "%d/%m/%Y"
                        ),
                        "data_info": row["data_info"],
                    }
                    groups_appended.append(row["cd_grupo"])
                    insert_new_group_stage_raw(new_group, md_quota_cursor)
                insert_new_quota_stage_raw(new_quota, md_quota_cursor)
                insert_new_customer_stage_raw(new_customer, md_quota_cursor)

            return event_detail_type


def from_sftp_to_stage_raw_quotas():
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()
    md_quota_cursor = md_quota_connection.cursor()
    args = getResolvedOptions(
        sys.argv, ["path_s3", "bucket_s3", "file_name", "event_bus_name"]
    )
    event_bus_name = args["event_bus_name"]
    s3_path = args["path_s3"]
    bucket_name = args["bucket_s3"]
    file_name = args["file_name"]

    try:
        event_detail_type = read_excel_data(s3_path, file_name, md_quota_cursor)
        upload_processed_extract(file_name, bucket_name)
        put_event(event_bus_name, event_detail_type)
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
    from_sftp_to_stage_raw_quotas()