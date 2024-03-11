from bazartools.common.logger import get_logger
from bazartools.common.database.glueConnection import GlueConnection
from bazartools.common.database.quotaCodeBuilder import build_quota_code
from psycopg2 import OperationalError
from bazartools.common.eventTrigger import EventTrigger
from awsglue.utils import getResolvedOptions
from datetime import datetime
from dateutil import relativedelta
import sys
import boto3
from botocore.exceptions import ClientError
import json

BATCH_SIZE = 2000

GLUE_DEFAULT_CODE = 2

today = datetime.today()

logger = get_logger()


def switch_status(key: str, value: int) -> int:
    status = {
        "1": 1,
        "3": 4,
        "EXCLUIDOS": 2,
        "2": 3,
    }
    return status.get(key, value)


def switch_multiple_owner(key: str) -> bool:
    multiple_owner = {"N": False, "S": True}
    return multiple_owner.get(key, False)


def switch_asset_type(key: str, value: int) -> int:
    asset_type = {
        "1": 2,
        "2": 2,
        "3": 2,
        "4": 2,
        "5": 2,
        "6": 1,
        "7": 1,
        "8": 1,
        "9": 1,
        "10": 1,
        "11": 3,
    }
    return asset_type.get(key, value)


def switch_quota_history_field(key: str, value: int) -> int:
    quota_history_field = {
        "old_quota_number": 1,
        "old_digit": 2,
        "quota_plan": 3,
        "installments_paid_number": 4,
        "overdue_installments_number": 5,
        "overdue_percentage": 6,
        "per_amount_paid": 7,
        "per_mutual_fund_paid": 8,
        "per_reserve_fund_paid": 9,
        "per_adm_paid": 10,
        "per_subscription_paid": 11,
        "per_mutual_fund_to_pay": 12,
        "per_reserve_fund_to_pay": 13,
        "per_adm_to_pay": 14,
        "per_subscription_to_pay": 15,
        "per_insurance_to_pay": 16,
        "per_install_diff_to_pay": 17,
        "per_total_amount_to_pay": 18,
        "amnt_mutual_fund_to_pay": 19,
        "amnt_reserve_fund_to_pay": 20,
        "amnt_adm_to_pay": 21,
        "amnt_subscription_to_pay": 22,
        "amnt_insurance_to_pay": 23,
        "amnt_fine_to_pay": 24,
        "amnt_interest_to_pay": 25,
        "amnt_others_to_pay": 26,
        "amnt_install_diff_to_pay": 27,
        "amnt_to_pay": 28,
        "quitter_assembly_number": 29,
        "cancelled_assembly_number": 30,
        "adjustment_date": 31,
        "current_assembly_date": 32,
        "current_assembly_number": 33,
        "asset_adm_code": 34,
        "asset_description": 35,
        "asset_value": 36,
        "asset_type_id": 37,
    }
    return quota_history_field.get(key, value)


def get_table_dict(cursor, rows):
    column_names = [desc[0] for desc in cursor.description]

    # Create a list of dictionaries using list comprehension
    return [dict(zip(column_names, row)) for row in rows]


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


def find_stage_raw_quotas(md_quota_cursor):
    try:
        # executando queries de busca de informações no banco
        query_select_quotas_stage_raw = (
            f"""
                SELECT 
                * 
                FROM 
                stage_raw.tb_quotas_porto_pre tqpp
                WHERE 
                tqpp.is_processed is false
            """)

        # executando query de busca de dados de quotas Porto do stage_raw
        md_quota_cursor.execute(query_select_quotas_stage_raw)

    except Exception as error:
        logger.error(f'Erro durante tentativa de leitura do banco: {error}')
        raise error


def put_event(event_bus_name, batch_pricing_quotas):
    logger.info("Iniciando criação do evento")
    event_source = "glue"
    event_detail_type = "porto_quota_ingestion_batch_pricing"
    event_detail = {
        "quota_code_list": batch_pricing_quotas,
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


def put_event_truncate_ofertas_bazar(event_bus_name: str):
    event_detail = {"event_origin": "porto_quota_ingestion"}
    event_detail_type = 'md-oferta-truncate-ofertas-bazar'
    entry = {
        "Source": "glue",
        "DetailType": event_detail_type,
        "Detail": json.dumps(event_detail),
        "EventBusName": event_bus_name,
    }
    try:
        response = boto3.client("events").put_events(Entries=[entry])
        logger.debug(f"Resposta da publicação: {response}. evento:{entry}")
    except ClientError as client_error:
        logger.error(f"Comportamento inesperado ao tentar publicar o evento.{client_error}")
        raise client_error


def find_data_source_id(md_quota_cursor) -> int:
    try:
        query_select_data_source = (
            f"""
                SELECT 
                pds.data_source_id 
                FROM 
                md_cota.pl_data_source pds 
                WHERE 
                pds.data_source_desc = 'API'
                AND
                pds.is_deleted is false
            """)
        logger.info('Buscando data_source_id no banco.')
        md_quota_cursor.execute(query_select_data_source)
        query_result_data_source = md_quota_cursor.fetchall()
        data_source_dict = get_table_dict(md_quota_cursor, query_result_data_source)
        data_source_id = data_source_dict[0]["data_source_id"]
        logger.info(f'data_source_id recuperado no banco: {data_source_id}')
        return data_source_id

    except Exception as error:
        logger.error(f"Ocorreu um erro durante tentativa de gravação dos dados no stage_raw: {error}")
        raise error


def find_adm_id(md_quota_cursor) -> int:
    try:
        query_select_adm = (
            f"""
                SELECT
                pa.administrator_id
                FROM
                md_cota.pl_administrator pa
                WHERE 
                pa.administrator_desc = 'PORTO SEGURO ADM. CONS. LTDA'
                AND
                pa.is_deleted is false
            """)
        logger.info('Buscando id_adm no banco.')
        logger.info(f"query leitura adm md-cota: {query_select_adm}")
        # buscando adm_id no banco
        md_quota_cursor.execute(query_select_adm)
        query_result_adm = md_quota_cursor.fetchall()
        adm_dict = get_table_dict(md_quota_cursor, query_result_adm)
        id_adm = adm_dict[0]["administrator_id"]
        logger.info(f'id_adm recuperado no banco: {id_adm}')
        return id_adm

    except Exception as error:
        logger.error(f"Ocorreu um erro durante tentativa de gravação dos dados no stage_raw: {error}")
        raise error


def find_md_quota_quotas(md_quota_cursor, adm_id) -> list:
    try:
        query_select_quotas_md_cota = (
            f"""
                SELECT 
                *
                FROM 
                md_cota.pl_quota pq
                WHERE 
                pq.administrator_id = %s
                AND
                pq.is_deleted is FALSE
            """)
        logger.info('Buscando cotas Porto no MD-COTA.')
        logger.info(f"query leitura quotas md-cota: {query_select_quotas_md_cota}")
        # buscando cotas do md-cota
        md_quota_cursor.execute(query_select_quotas_md_cota, [adm_id])
        query_result_quotas = md_quota_cursor.fetchall()
        quotas_md_cota_dict = get_table_dict(md_quota_cursor, query_result_quotas)
        logger.info('Cotas Porto no MD-COTA recuperadas com sucesso.')
        return quotas_md_cota_dict

    except Exception as error:
        logger.error(f"Ocorreu um erro durante tentativa de gravação dos dados no stage_raw: {error}")
        raise error


def find_md_quota_groups(md_quota_cursor, adm_id) -> list:
    try:
        query_select_groups_md_cota =(
            f"""
                SELECT 
                pg.group_id,
                pg.group_code 
                FROM
                md_cota.pl_group pg 
                where 
                pg.administrator_id = %s
                AND
                pg.is_deleted is false
            """)
        logger.info('Buscando grupos Porto no MD-COTA.')
        logger.info(f"query leitura grupos md-cota: {query_select_groups_md_cota}")
        # buscando grupos do md-cota
        md_quota_cursor.execute(query_select_groups_md_cota, [adm_id])
        query_result_groups = md_quota_cursor.fetchall()
        groups_md_cota_dict = get_table_dict(md_quota_cursor, query_result_groups)
        logger.info('Grupos Porto no MD-COTA recuperadas com sucesso.')
        return groups_md_cota_dict

    except Exception as error:
        logger.error(f"Ocorreu um erro durante tentativa de gravação dos dados no stage_raw: {error}")
        raise error


def insert_md_quota_group(md_quota_cursor, group, adm_id, row_group_code, group_end_date, md_quota_connection) -> dict:
    try:
        query_insert_group = (
            f"""
                INSERT
                INTO
                md_cota.pl_group
                (
                group_code,
                group_deadline,
                administrator_id,
                group_closing_date,
                created_at,
                modified_at,
                created_by,
                modified_by
                )
                VALUES
                (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
                )
                RETURNING 
                GROUP_ID
            """)
        logger.info('Inserindo novo grupo no md-cota.')
        md_quota_cursor.execute(
            query_insert_group,
            (
                row_group_code,
                group["end_of_group_months"],
                adm_id,
                group_end_date,
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )
        group_inserted = md_quota_cursor.fetchall()
        group_id_md_quota = get_table_dict(
            md_quota_cursor, group_inserted
        )[0]["group_id"]
        logger.info(f"group_id inserido {group_id_md_quota}")
        group_inserted = {
                "group_id": group_id_md_quota,
                "group_code": row_group_code,
            }
        return group_inserted

    except Exception as error:
        # Rollback the transaction in case of an error
        md_quota_connection.rollback()
        logger.error(f"Ocorreu um erro durante tentativa de atualização dos dados no md-cota: {error}")
        raise error


def update_md_quota_group(md_quota_cursor, md_quota_group, stage_group, group_end_date, md_quota_connection):
    try:
        group_id_md_quota = md_quota_group["group_id"]
        query_update_group = (
            """
                UPDATE
                md_cota.pl_group
                SET
                group_closing_date = %s,
                group_deadline = %s,
                modified_at = %s,
                modified_by = %s
                WHERE
                group_id = %s
            """)
        logger.info('Atualizando grupo no md-cota...')
        md_quota_cursor.execute(
            query_update_group,
            (
                group_end_date,
                stage_group["end_of_group_months"],
                "now()",
                GLUE_DEFAULT_CODE,
                group_id_md_quota,
            ),
        )
        logger.info('Grupo atualizado com sucesso!')

    except Exception as error:
        # Rollback the transaction in case of an error
        md_quota_connection.rollback()
        logger.error(f"Ocorreu um erro durante tentativa de atualização dos dados no md-cota: {error}")
        raise error


def get_status(share_situation) -> int:
    status_type = switch_status(share_situation, 5)
    return status_type


def get_person_type(person_type) -> int:
    if person_type == 1:
        quota_person_type = 2
    else:
        quota_person_type = 1

    return quota_person_type


def get_asset_type(asset) -> int:
    asset_type = switch_asset_type(asset, 7)
    return asset_type


def insert_new_quota(md_quota_cursor, md_quota_connection, new_quota, status_type, adm_id, group_id_md_quota):
    try:
        multiple_owner = switch_multiple_owner(new_quota["e_ou"])

        quota_person_type = get_person_type(new_quota["is_pj"])

        contract_number = new_quota["uuid"].split("-")[0]
        quota_code = build_quota_code(md_quota_connection)
        query_insert_new_quota = (
            """
                INSERT
                INTO
                md_cota.pl_quota
                (
                quota_code,
                quota_number,
                check_digit,
                external_reference,
                total_installments,
                is_contemplated,
                is_multiple_ownership,
                administrator_fee,
                fund_reservation_fee,
                info_date,
                quota_status_type_id,
                administrator_id,
                group_id,
                contract_number,
                quota_person_type_id,
                quota_origin_id,
                created_by,
                modified_by
                )
                VALUES
                (
                %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s
                )
                RETURNING
                *
            """)
        md_quota_cursor.execute(
            query_insert_new_quota,
            (
                quota_code,
                new_quota["share_number"],
                new_quota["share_version"],
                new_quota["uuid"],
                new_quota["end_of_share_months"],
                "false",
                multiple_owner,
                new_quota["total_adm_fee_percentage"],
                new_quota["reserve_fund_percentage"],
                new_quota["data_info"],
                status_type,
                adm_id,
                group_id_md_quota,
                contract_number,
                quota_person_type,
                1,
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )
        quota_inserted = md_quota_cursor.fetchall()
        quota = get_table_dict(md_quota_cursor, quota_inserted)[
            0
        ]
        return quota

    except Exception as error:
        logger.error(f'Erro durante tentativa de inserção de nova cota: {error}')
        raise error


def update_quota_status_and_person_type(md_quota_cursor, stage_row, status_type, quota_person_type, quota_id):
    try:
        contract_number = stage_row["uuid"].split("-")[0]
        query_update_quota = (
            """
                UPDATE
                md_cota.pl_quota
                SET
                quota_status_type_id = %s,
                contract_number = %s,
                quota_person_type_id = %s,
                modified_at = %s,
                modified_by = %s
                WHERE
                quota_id = %s
            """)
        md_quota_cursor.execute(
            query_update_quota,
            (
                status_type,
                contract_number,
                quota_person_type,
                "now()",
                GLUE_DEFAULT_CODE,
                quota_id,
            ),
        )
    except Exception as error:
        logger.error(f"Erro durante tentativa de atualizar cota: {error}")
        raise error


def update_quota_person_type(md_quota_cursor, quota_person_type, quota_id):
    try:
        query_update_quota = (
            """
                UPDATE
                md_cota.pl_quota
                SET
                quota_person_type_id = %s,
                modified_at = %s,
                modified_by = %s
                WHERE
                quota_id = %s
            """)
        md_quota_cursor.execute(
            query_update_quota,
            (
                quota_person_type,
                "now()",
                GLUE_DEFAULT_CODE,
                quota_id,
            ),
        )
    except Exception as error:
        logger.error(f"Erro durante tentativa de atualizar cota: {error}")
        raise error


def insert_new_quota_status(md_quota_cursor, status_type, quota_id):
    try:
        query_insert_quota_status = (
            """
                INSERT
                INTO
                md_cota.pl_quota_status
                (
                quota_id,
                quota_status_type_id,
                created_by,
                modified_by
                )
                VALUES
                (
                %s,
                %s,
                %s,
                %s
                )
            """)

        md_quota_cursor.execute(
            query_insert_quota_status,
            (quota_id, status_type, GLUE_DEFAULT_CODE, GLUE_DEFAULT_CODE),
        )
    except Exception as error:
        logger.error(f"Erro durante tentativa de inserir novo quota_status: {error}")
        raise error


def update_quota_status(md_quota_cursor, quota_id):
    try:
        query_update_quota_status = (
            """
                UPDATE
                md_cota.pl_quota_status
                SET
                valid_to = %s,
                modified_at = %s,
                modified_by = %s
                WHERE
                quota_id = %s
                AND
                valid_to is null
            """)
        md_quota_cursor.execute(
            query_update_quota_status,
            ("now()", "now()", GLUE_DEFAULT_CODE, quota_id),
        )
    except Exception as error:
        logger.error(f"Erro durante tentativa de atualização de status da cota: {error}")
        raise error


def insert_new_quota_field_update_date(md_quota_cursor, quota_id, history_field_id, data_source_id,
                                       data_info):
    try:
        query_quota_field_update_date_insert = (
            """
                INSERT
                INTO
                md_cota.pl_quota_field_update_date
                (
                quota_id,
                quota_history_field_id,
                data_source_id,
                update_date,
                created_by,
                modified_by
                )
                VALUES
                (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s)
            """)
        md_quota_cursor.execute(
            query_quota_field_update_date_insert,
            (
                quota_id,
                history_field_id,
                data_source_id,
                data_info,
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )
    except Exception as error:
        logger.error(f"Erro durante tentativa de inserção de quota_field_update_date: {error}")
        raise error


def update_quota_field_update_date(md_quota_cursor, quota_id, history_field_id, data_source_id, data_info):
    try:
        query_update_data_field_update_date = (
            f"""
                update 
                md_cota.pl_quota_field_update_date 
                set 
                data_source_id = %s,
                update_date = %s,
                modified_at = %s,
                modified_by = %s
                where 
                quota_id = %s
                and 
                quota_history_field_id = %s
            """
        )
        md_quota_cursor.execute(
            query_update_data_field_update_date,
            (
                data_source_id,
                data_info,
                'now()',
                GLUE_DEFAULT_CODE,
                quota_id,
                history_field_id,
            ),
        )

    except Exception as error:
        logger.error(f"Erro durante tentativa de atualização do quota_field_update_date: {error}")
        raise error


def search_quota_history_detail(md_quota_cursor, quota_id):
    try:
        query_search_quota_history_detail = (
            """
                SELECT
                *
                FROM
                md_cota.pl_quota_history_detail pqhd
                WHERE
                pqhd.quota_id = %s
                AND
                pqhd.valid_to is null
                AND
                pqhd.is_deleted is false
            """)

        md_quota_cursor.execute(
            query_search_quota_history_detail, [quota_id]
        )
        query_result_quota_history_detail = md_quota_cursor.fetchall()
        quota_history_detail = get_table_dict(
            md_quota_cursor, query_result_quota_history_detail
        )[0]
        return quota_history_detail

    except Exception as error:
        logger.error(f"Erro durante tentativa de busca de quota_history_detail da cota: {error}")
        raise error


def insert_new_quota_history_detail(md_quota_cursor, quota_id, quota_history_detail,
                                    asset_type, current_assembly, data_source_id, new_quota):
    quota_history_detail_insert = (
        """
            INSERT
            INTO
            md_cota.pl_quota_history_detail
            (
            quota_id,
            old_quota_number,
            old_digit,
            installments_paid_number,
            per_mutual_fund_paid,
            current_assembly_number,
            asset_value,
            asset_type_id,
            info_date,
            valid_from,
            created_by,
            modified_by
            )
            VALUES
            (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s)
        """)

    md_quota_cursor.execute(
        quota_history_detail_insert,
        (
            quota_id,
            quota_history_detail["share_number"],
            quota_history_detail["share_version"],
            quota_history_detail["late_parcel"],
            quota_history_detail["common_fund_percentage"],
            current_assembly,
            quota_history_detail["good_value"],
            asset_type,
            quota_history_detail["data_info"],
            "now()",
            GLUE_DEFAULT_CODE,
            GLUE_DEFAULT_CODE,
        ),
    )

    fields_inserted_quota_history = [
        "installments_paid_number",
        "old_quota_number",
        "old_digit",
        "per_mutual_fund_paid",
        "asset_value",
        "asset_type_id",
        "current_assembly_number",
    ]

    for field in fields_inserted_quota_history:
        history_field_id = switch_quota_history_field(field, 0)
        if new_quota:
            insert_new_quota_field_update_date(md_quota_cursor, quota_id, history_field_id, data_source_id,
                                               quota_history_detail["data_info"])
        else:
            update_quota_field_update_date(md_quota_cursor, quota_id, history_field_id, data_source_id, quota_history_detail["data_info"])


def update_quota_history_detail(md_quota_cursor, quota_history_detail):
    try:
        query_update_quota_history_detail = (
            """
                UPDATE
                md_cota.pl_quota_history_detail
                SET
                valid_to = now()
                WHERE
                quota_history_detail_id = %s
                AND
                valid_to is null
            """)
        md_quota_cursor.execute(
            query_update_quota_history_detail,
            ([quota_history_detail["quota_history_detail_id"]]),
        )

    except Exception as error:
        logger.error(f"Erro durante tentativa de atualização de quota_history_detail da cota: {error}")
        raise error


def update_stage_raw_row(md_quota_cursor, stage_row):
    try:
        query_update_stage_raw = (
            f"""
                        UPDATE stage_raw.tb_quotas_porto_pre
                        SET is_processed = true
                        WHERE 
                        id_quotas_porto = {stage_row['id_quotas_porto']};
                    """)
        logger.info(f"query de atualização: {query_update_stage_raw}")
        md_quota_cursor.execute(query_update_stage_raw)

    except Exception as error:
        logger.error(f"Erro durante atualização da linha processada no stage_raw: {error}")
        raise error


def new_quota_md_quota_flow(md_quota_cursor, adm_id, stage_row, group_id_md_quota, md_quota_connection, current_assembly, data_source_id) -> str:
    try:
        status_type = get_status(stage_row["share_situation"])
        new_quota = insert_new_quota(md_quota_cursor, md_quota_connection, stage_row, status_type, adm_id, group_id_md_quota)
        quota_id = new_quota["quota_id"]
        quota_code = new_quota["quota_code"]
        insert_new_quota_status(md_quota_cursor, status_type, quota_id)
        asset_type = get_asset_type(stage_row["good_type"])

        insert_new_quota_history_detail(md_quota_cursor, quota_id, stage_row, asset_type,
                                        current_assembly, data_source_id, True)
        update_stage_raw_row(md_quota_cursor, stage_row)

        return new_quota

    except Exception as error:
        # Rollback the transaction in case of an error
        md_quota_connection.rollback()
        logger.error(f"Ocorreu um erro durante tentativa de atualização/inserção dos dados no md-cota: {error}")
        raise error


def update_quota_md_quota_flow(md_quota_cursor, data_source_id, current_assembly, row_md_quota, stage_row):
    quota_id = row_md_quota["quota_id"]

    status_type = switch_status(stage_row["share_situation"], 5)

    quota_person_type = get_person_type(stage_row["is_pj"])

    if row_md_quota["info_date"] < stage_row["data_info"]:
        if row_md_quota["quota_status_type_id"] != status_type:
            update_quota_status_and_person_type(md_quota_cursor, stage_row, status_type, quota_person_type, quota_id)

            update_quota_status(md_quota_cursor, quota_id)

            insert_new_quota_status(md_quota_cursor, status_type, quota_id)

        else:
            update_quota_person_type(md_quota_cursor, quota_person_type, quota_id)

    quota_history_detail = search_quota_history_detail(md_quota_cursor, quota_id)

    if quota_history_detail["info_date"] < stage_row["data_info"]:

        update_quota_history_detail(md_quota_cursor, quota_history_detail)
        asset_type = get_asset_type(stage_row["good_type"])

        insert_new_quota_history_detail(md_quota_cursor, quota_id, stage_row,
                                        asset_type, current_assembly, data_source_id, False)

    update_stage_raw_row(md_quota_cursor, stage_row)


def process_all(md_quota_cursor, md_quota_operational_cursor, md_quota_connection, data_source_id, id_adm,
                md_quota_quotas, md_quota_groups, batch_pricing_quotas):
    try:
        batch_counter = 0

        while True:
            batch_counter += 1
            rows = md_quota_cursor.fetchmany(size=BATCH_SIZE)  # Fetch XXX rows at a time
            column_names = [desc[0] for desc in md_quota_cursor.description]
            if not rows:
                break
            logger.info(
                f"Fetch {BATCH_SIZE} rows at a time - Batch number {batch_counter}"
            )
            for row in rows:
                row_dict = dict(zip(column_names, row))
                # verificando se a cota já existe no md-cota
                row_md_quota = get_dict_by_id(
                    row_dict["uuid"], md_quota_quotas, "external_reference"
                )
                row_group_code = cd_grupo_right_justified(row_dict["grupo"])
                # buscando grupo no md-cota
                row_md_cota_group = get_dict_by_id(
                    row_group_code, md_quota_groups, "group_code"
                )
                info_date = row_dict["data_info"]
                assembly_since_statement = relativedelta.relativedelta(
                    today, info_date
                ).months
                assembly_to_end = (
                        row_dict["end_group_m"] - assembly_since_statement
                )
                current_assembly = (
                        row_dict["end_of_group_months"] - assembly_to_end
                )
                group_end_date = today + relativedelta.relativedelta(
                    months=assembly_to_end
                )

                if row_md_quota is None:
                    # caso não encontre insere o registro do grupo no banco
                    if row_md_cota_group is None:
                        group_inserted = insert_md_quota_group(md_quota_operational_cursor,
                                                               row_dict, id_adm, row_group_code, group_end_date, md_quota_connection)
                        md_quota_groups.append(group_inserted)
                        group_id_md_quota = group_inserted["group_id"]

                    else:
                        update_md_quota_group(md_quota_operational_cursor, row_md_cota_group, row_dict,
                                              group_end_date, md_quota_connection)
                        group_id_md_quota = row_md_cota_group["group_id"]

                    quota = new_quota_md_quota_flow(md_quota_operational_cursor, id_adm, row_dict,
                                                    group_id_md_quota, md_quota_connection, current_assembly, data_source_id)
                    md_quota_quotas.append(quota)

                else:
                    update_quota_md_quota_flow(md_quota_operational_cursor, data_source_id,
                                               current_assembly, row_md_quota, row_dict)
                    quota_to_batch_pricing = {
                        "quota_code": row_md_quota["quota_code"],
                        "auction": True,
                        "send_to_adapter": False,
                    }
                    batch_pricing_quotas.append(quota_to_batch_pricing)

    except OperationalError as error:
        # Rollback the transaction in case of an error
        md_quota_connection.rollback()
        logger.error("Transaction rolled back due to an oretaional error:", error)
        raise error

    except Exception as error:
        # Rollback the transaction in case of an error
        md_quota_connection.rollback()
        logger.error("Transaction rolled back due to an error:", error)
        raise error


def porto_quota_ingestion():

    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()
    md_quota_cursor = md_quota_connection.cursor()
    md_quota_operational_cursor = md_quota_connection.cursor()
    # args = getResolvedOptions(
    #     sys.argv, ["WORKFLOW_NAME", "JOB_NAME", "time_window", "event_bus_name"]
    # )
    # event_bus_name = args["event_bus_name"]
    # workflow_name = args['WORKFLOW_NAME']
    # job_name = args['JOB_NAME']
    # event_trigger = EventTrigger(workflow_name, job_name)
    # event = event_trigger.get_event_details()
    # logger.info(f'event: {event}')
    try:
        data_source_id = find_data_source_id(md_quota_cursor)
        id_adm = find_adm_id(md_quota_cursor)
        md_quota_quotas = find_md_quota_quotas(md_quota_cursor, id_adm)
        md_quota_groups = find_md_quota_groups(md_quota_cursor, id_adm)
        find_stage_raw_quotas(md_quota_cursor)

        batch_pricing_quotas = []

        process_all(md_quota_cursor, md_quota_operational_cursor, md_quota_connection, data_source_id, id_adm,
                    md_quota_quotas, md_quota_groups, batch_pricing_quotas)
        # put_event(event_bus_name, batch_pricing_quotas)
        # put_event_truncate_ofertas_bazar(event_bus_name)
        md_quota_connection.commit()

    except Exception as error:
        raise error

    finally:
        # Close the cursor and connection
        md_quota_cursor.close()
        md_quota_connection.close()
        logger.info("Connection closed.")


if __name__ == "__main__":
    porto_quota_ingestion()
