import json

import boto3
from bazartools.common.logger import get_logger
from bazartools.common.database.glueConnection import GlueConnection
from bazartools.common.database.quotaCodeBuilder import build_quota_code
from bazartools.common.eventTrigger import EventTrigger
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from psycopg2 import extras
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED
from datetime import datetime
from dateutil.relativedelta import relativedelta
import sys

BATCH_SIZE = 500
GLUE_DEFAULT_CODE = 2
QUOTA_ORIGIN_ADM = 1
logger = get_logger()
administrator_code = "0000000155"


def get_table_dict(cursor, rows):
    column_names = [desc[0] for desc in cursor.description]

    # Create a list of dictionaries using list comprehension
    return [dict(zip(column_names, row)) for row in rows]


def cd_group_right_justified(group: str) -> str:
    code_group = group if len(str(group)) == 5 else str(group).rjust(5, "0")
    return code_group


def get_dict_by_id(id_item: str, data_list: list, field_name: str):
    filtered_items = filter(lambda item: item[field_name] == id_item, data_list)
    return next(filtered_items, None)


def switch_status(key: str, value: int) -> str:
    status = {
        "ATIVOS": 1,
        "DESISTENTES": 4,
        "EXCLUIDOS": 2,
        "EM ATRASO": 3,
    }
    return status.get(key, value)


def switch_asset_type(key: str, value: int) -> str:
    asset_type = {
        "VEÍCULOS PESADOS": 3,
        "VEÍCULOS LEVES": 2,
        "IMÓVEIS": 1,
        "MOTOCICLETAS": 4,
    }
    return asset_type.get(key, value)


def switch_quota_history_field(key: str, value: int) -> str:
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


def get_quota_origin(endpoint) -> int:
    if endpoint == "POST /pricingQuery":
        return 1
    else:
        return 2


def fetch_select_stage_raw_quotas(
    md_quota_cursor, endpoint, administrator, quotas_stage_raw
):
    try:
        logger.info("Buscando dados para processar no stage_raw...")
        if quotas_stage_raw is not None:
            query_select_stage_raw = """
            SELECT *
            FROM
                stage_raw.tb_quotas_api tqa
            WHERE
                tqa.is_processed IS FALSE
            AND
                tqa.endpoint_generator = %s
            AND
                tqa.administrator = %s
            AND
                tqa.id_quotas_itau in %s
            """
            md_quota_cursor.execute(
                query_select_stage_raw,
                (
                    endpoint,
                    administrator,
                    tuple(quotas_stage_raw),
                ),
            )
            logger.info("Dados para processamento recuperados com sucesso no stage_raw")
        else:
            query_select_stage_raw = """
            SELECT *
            FROM
                stage_raw.tb_quotas_api tqa
            WHERE
                tqa.is_processed IS FALSE
            AND
                tqa.endpoint_generator = %s
            AND
                tqa.administrator = %s
            """
            logger.info(f"query leitura stage_raw: {query_select_stage_raw}")
            md_quota_cursor.execute(
                query_select_stage_raw,
                (
                    endpoint,
                    administrator,
                ),
            )
            logger.info("Dados para processamento recuperados com sucesso no stage_raw")

    except Exception as error:
        logger.error(
            f"Não foi possível executar"
            f" o select na tabela tb_quotas_api: error:{error}"
        )
        raise error


def select_group_md_quota(md_quota_select_cursor):
    logger.info("Buscando informações de grupos no md-cota...")
    try:
        query_select_group_md_quota = """
        SELECT
            pg.group_id,
            pg.group_code
        FROM
            md_cota.pl_group pg
        LEFT JOIN
            md_cota.pl_administrator pa ON pa.administrator_id = pg.administrator_id
        WHERE
            pa.administrator_code = %s
        AND
            pg.is_deleted is false
        """
        md_quota_select_cursor.execute(
            query_select_group_md_quota, [administrator_code]
        )
        logger.info("Informações de grupos recuperadas com sucesso no md-cota!")
        query_result_groups = md_quota_select_cursor.fetchall()
        groups_dict = get_table_dict(md_quota_select_cursor, query_result_groups)
        return groups_dict
    except Exception as error:
        logger.error(
            f"Não foi possível executar" f" o select na tabela pl_group: error:{error}"
        )
        raise error


def select_administrator_id(md_quota_select_cursor):
    try:
        logger.info(f"Buscando id da adm no md-cota...")
        query_select_adm = """
        SELECT
            pa.administrator_id
        FROM
            md_cota.pl_administrator pa
        WHERE 
            pa.administrator_code = %s
        AND
            pa.is_deleted is false
        """
        logger.info(f"query leitura adm md-cota: {query_select_adm}")
        md_quota_select_cursor.execute(query_select_adm, [administrator_code])
        query_result_adm = md_quota_select_cursor.fetchall()
        logger.info(f"Id da adm recuperado com sucesso md-cota!")
        adm_dict = get_table_dict(md_quota_select_cursor, query_result_adm)
        return adm_dict[0]["administrator_id"] if adm_dict is not None else None
    except Exception as error:
        logger.error(
            f"Não foi possível executar"
            f" o select na tabela pl_administrator: error:{error}"
        )
        raise error


def select_quota_md_quota(md_quota_select_cursor, id_adm):
    try:
        logger.info("Buscando informações de cotas no md-cota...")
        query_select_quota_md_quota = """
        SELECT
            pq.*
        FROM
            md_cota.pl_quota pq
        LEFT JOIN
            md_cota.pl_administrator pa ON pa.administrator_id = pq.administrator_id 
        WHERE
            pq.is_deleted IS FALSE
        AND 
            pa.administrator_code = '0000000155'
        AND
            pq.is_deleted is false
        """
        md_quota_select_cursor.execute(query_select_quota_md_quota, [id_adm])
        query_result_quotas = md_quota_select_cursor.fetchall()
        logger.info("Informações de cotas recuperadas com sucesso no md-cota!")
        return get_table_dict(md_quota_select_cursor, query_result_quotas)
    except Exception as error:
        logger.error(
            f"Não foi possível executar" f" o select na tabela pl_quota: error:{error}"
        )
        raise error


def select_data_source_id(md_quota_select_cursor):
    try:
        logger.info(f"Buscando id da fonte de dados no md-cota...")
        query_select_data_source = """
        SELECT 
            pds.data_source_id 
        FROM 
            md_cota.pl_data_source pds 
        WHERE 
            pds.data_source_desc = 'API'
        AND
            pds.is_deleted is false
        """
        md_quota_select_cursor.execute(query_select_data_source)
        query_result_data_source = md_quota_select_cursor.fetchall()
        data_source_dict = get_table_dict(
            md_quota_select_cursor, query_result_data_source
        )
        logger.info(f"Id da fonte de dados recuperado com sucesso md-cota!")
        return data_source_dict[0]["data_source_id"]
    except Exception as error:
        logger.info(
            f"Erro ao fazer o select na tabela pl_data_source_desc, error:{error}"
        )
        raise error


def update_md_quota_group(
    row_md_quota_group, group_end_date, group_deadline, md_quota_update_cursor, group_id
):
    try:
        logger.info(f"Atualizando grupo no md-cota: {row_md_quota_group}")

        query_update_group = """
        UPDATE md_cota.pl_group
        SET
            group_closing_date = %s,
            group_deadline = %s,
            modified_at = %s,
            modified_by = %s
        WHERE
            group_id = %s
        """
        md_quota_update_cursor.execute(
            query_update_group,
            (
                group_end_date,
                group_deadline,
                "now()",
                GLUE_DEFAULT_CODE,
                group_id,
            ),
        )
        logger.info("Grupo atualizado com sucesso!")
    except Exception as error:
        logger.error(f"Erro ao executar a query de atualização: {str(error)}")
        raise error


def insert_group_into_database(
    md_cota_insert_cursor, row_group_code, group_deadline, id_adm, group_end_date
):
    try:
        logger.info(f"Inserindo novo grupo no md-cota: {row_group_code}")
        query_insert_group = """
            INSERT INTO md_cota.pl_group
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
                %s,%s,%s,%s,%s,%s,%s,%s
            )
            RETURNING GROUP_ID
        """
        md_cota_insert_cursor.execute(
            query_insert_group,
            (
                row_group_code,
                group_deadline,
                id_adm,
                group_end_date,
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )

        group_inserted = md_cota_insert_cursor.fetchall()
        logger.info("Grupo inserido com sucesso!")
        group_id = get_table_dict(md_cota_insert_cursor, group_inserted)[0]["group_id"]
        logger.info(f"group_id inserido {group_id}")

        return {"group_id": group_id, "group_code": row_group_code}, group_id

    except Exception as error:
        logger.error(
            f"Erro ao executar a query de inserção de grupo no md-cota: {str(error)}"
        )
        raise error


def insert_group_vacancy_into_database(md_cota_insert_cursor, new_group_vacancy):
    try:
        logger.info(
            f"Inserindo número de vagas para o group_id: {new_group_vacancy['group_id']}"
        )
        query_insert_group_vacancy = """
            INSERT INTO md_cota.pl_group_vacancies 
            (
                vacancies,
                info_date,
                group_id,
                valid_from,
                created_at,
                modified_at,
                created_by,
                modified_by
            )
            VALUES
            (
                %s, %s, %s, %s, 
                %s, %s, %s, %s
            )
        """
        md_cota_insert_cursor.execute(
            query_insert_group_vacancy,
            (
                new_group_vacancy["vacancies"],
                new_group_vacancy["info_date"],
                new_group_vacancy["group_id"],
                new_group_vacancy["valid_from"],
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )
        logger.info("Dados de vagas inseridos com sucesso!")

    except Exception as error:
        logger.error(
            f"Erro ao executar a query de inserção de vagas do grupo: {str(error)}"
        )
        raise error


def read_group_vacancies(group_id, md_quota_select_cursor):
    try:
        logger.info(f"Lendo dados de vagas para o group_id: {group_id}")
        query_select_group_vacancy = """
            SELECT
                *
            FROM
                md_cota.pl_group_vacancies
            WHERE
                is_deleted IS FALSE
            AND 
                valid_to is NULL
            AND
                group_id = %s
            """
        md_quota_select_cursor.execute(query_select_group_vacancy, [group_id])
        query_result_group_vacancies = md_quota_select_cursor.fetchall()
        logger.info("Dados de vagas recuperados com sucesso!")
        return get_table_dict(md_quota_select_cursor, query_result_group_vacancies)
    except Exception as error:
        logger.error(
            f"Não foi possível executar"
            f" o select na tabela pl_group_vacancies: error:{error}"
        )
        raise error


def update_group_vacancy(group_vacancy_id, md_quota_update_cursor):
    try:
        logger.info(f"Invalidando registro antigo de vagas...")
        query_update_group_vacancy = """
        UPDATE md_cota.pl_group_vacancies
        SET
            valid_to = %s,
            modified_at = %s,
            modified_by = %s
        WHERE
            group_vacancies_id = %s
        """
        md_quota_update_cursor.execute(
            query_update_group_vacancy,
            (
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                group_vacancy_id,
            ),
        )
        logger.info("Dados de vagas atualizado com sucesso!")
    except Exception as error:
        logger.error(
            f"Erro ao executar a query de atualização de dados vagas de grupo: {str(error)}"
        )
        raise error


def insert_quota_into_database(
    md_quota_insert_cursor, md_quota_connection, quota_to_insert
):
    try:
        logger.info(f"Inserindo nova cota no md-cota...")
        quota_code = build_quota_code(md_quota_connection)

        query_insert_quota = """
            INSERT INTO md_cota.pl_quota
            (
                quota_code,
                external_reference,
                quota_number,
                total_installments,
                is_contemplated,
                is_multiple_ownership,
                administrator_fee,
                fund_reservation_fee,
                info_date,
                quota_status_type_id,
                administrator_id,
                group_id,
                contemplation_date,
                quota_person_type_id,
                contract_number,
                quota_origin_id,
                cancel_date,
                acquisition_date,
                created_at,
                modified_at,
                created_by,
                modified_by
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING *
        """
        md_quota_insert_cursor.execute(
            query_insert_quota,
            (
                quota_code,
                quota_to_insert["external_reference"],
                quota_to_insert["quota_number"],
                quota_to_insert["total_installments"],
                quota_to_insert["is_contemplated"],
                quota_to_insert["is_multiple_ownership"],
                quota_to_insert["administrator_fee"],
                quota_to_insert["fund_reservation_fee"],
                quota_to_insert["info_date"],
                quota_to_insert["quota_status_type_id"],
                quota_to_insert["administrator_id"],
                quota_to_insert["group_id"],
                quota_to_insert["contemplation_date"],
                quota_to_insert["quota_person_type_id"],
                quota_to_insert["contract_number"],
                quota_to_insert["quota_origin_id"],
                quota_to_insert["cancel_date"],
                quota_to_insert["acquisition_date"],
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )

        new_quota = get_table_dict(
            md_quota_insert_cursor, md_quota_insert_cursor.fetchall()
        )[0]
        logger.info("Nova cota inserida com sucesso!")
        return new_quota

    except Exception as error:
        logger.error(f"Erro ao executar a query de inserção da quota: {str(error)}")
        raise error


def insert_quota_status_into_database(md_quota_insert_cursor, quota_id, status_type):
    try:
        logger.info(f"Inserindo novo status para quota_id: {quota_id}")
        query_insert_quota_status = """
            INSERT INTO md_cota.pl_quota_status
            (
                quota_id,
                quota_status_type_id,
                created_by,
                modified_by
            )
            VALUES
            (
                %s, %s, %s, %s
            )
        """
        md_quota_insert_cursor.execute(
            query_insert_quota_status,
            (quota_id, status_type, GLUE_DEFAULT_CODE, GLUE_DEFAULT_CODE),
        )
        logger.info("Status inserido com sucesso!")

    except Exception as error:
        logger.error(
            f"Erro ao executar a query de inserção do status da quota: {str(error)}"
        )
        raise error


def insert_quota_history_detail_into_database(
    md_quota_insert_cursor, quota_history_detail
):
    try:
        logger.info(
            f"Inserindo novo histórico de cota para quota_id: {quota_history_detail['quota_id']}"
        )
        quota_history_detail_insert = """
            INSERT INTO md_cota.pl_quota_history_detail
            (
                quota_id,
                installments_paid_number,
                old_quota_number,
                overdue_installments_number,
                asset_value,
                asset_type_id,
                per_adm_paid,
                per_mutual_fund_paid,
                per_adm_to_pay,
                amnt_to_pay,
                info_date,
                valid_from,
                valid_to,
                created_at,
                modified_at,
                created_by,
                modified_by
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        md_quota_insert_cursor.execute(
            quota_history_detail_insert,
            (
                quota_history_detail["quota_id"],
                quota_history_detail["installments_paid_number"],
                quota_history_detail["old_quota_number"],
                quota_history_detail["overdue_installments_number"],
                quota_history_detail["asset_value"],
                quota_history_detail["asset_type_id"],
                quota_history_detail["per_adm_paid"],
                quota_history_detail["per_mutual_fund_paid"],
                quota_history_detail["per_adm_to_pay"],
                quota_history_detail["amnt_to_pay"],
                quota_history_detail["info_date"],
                quota_history_detail["valid_from"],
                quota_history_detail["valid_to"],
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )
        logger.info("Histórico da cota inserido com sucesso!")

        fields_inserted_quota_history = [
            "installments_paid_number",
            "old_quota_number",
            "overdue_installments_number",
            "asset_value",
            "asset_type_id",
            "per_adm_paid",
            "per_mutual_fund_paid",
            "per_adm_to_pay",
            "amnt_to_pay",
        ]
        return fields_inserted_quota_history
    except Exception as error:
        logger.error(
            f"Erro ao executar a query de inserção no histórico da quota: {error}"
        )
        raise error


def insert_quota_field_update_date_into_database(
    md_quota_insert_cursor, quota_id, field, data_source_id, row_dict
):
    try:
        history_field_id = switch_quota_history_field(field, 0)
        logger.info(
            f"Inserindo nova data de atualização do histórico do quota_id {quota_id}, "
            f"para o campo com quota_history_field_id {history_field_id}"
        )

        query_quota_field_update_date_insert = """
            INSERT INTO md_cota.pl_quota_field_update_date
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
                %s, %s, %s, %s, %s, %s
            )
        """
        md_quota_insert_cursor.execute(
            query_quota_field_update_date_insert,
            (
                quota_id,
                history_field_id,
                data_source_id,
                row_dict["created_at"],
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )
        logger.info("Data de atualização inserida com sucesso!")

    except Exception as error:
        logger.error(
            f"Erro ao executar a query de inserção na tabela de datas de atualização do campo da quota: {str(error)}"
        )
        raise error


def update_quota(md_quota_update_cursor, quota_to_update):
    try:
        logger.info(f"Atualizando quota_id {quota_to_update['quota_id']} no md-cota...")
        query_update_quota = """
            UPDATE md_cota.pl_quota
            SET
                quota_status_type_id = %s,
                cancel_date = %s,
                acquisition_date = %s,
                modified_at = %s,
                modified_by = %s,
                contemplation_date = %s,
                quota_person_type_id = %s,
                contract_number = %s,
                quota_number = %s
            WHERE
                quota_id = %s
        """
        md_quota_update_cursor.execute(
            query_update_quota,
            (
                quota_to_update["quota_status_type_id"],
                quota_to_update["cancel_date"],
                quota_to_update["acquisition_date"],
                "now()",
                GLUE_DEFAULT_CODE,
                quota_to_update["contemplation_date"],
                quota_to_update["quota_person_type_id"],
                quota_to_update["contract_number"],
                quota_to_update["quota_number"],
                quota_to_update["quota_id"],
            ),
        )
        logger.info("Cota atualizada com sucesso!")
    except Exception as error:
        logger.error(f"Erro ao atualizar a cota: {str(error)}")
        raise error


def update_quota_reference_id(md_quota_update_cursor, quota_to_update):
    try:
        logger.info(
            f"Atualizando external_reference da quota_id {quota_to_update['quota_id']}..."
        )
        query_update_quota = """
            UPDATE md_cota.pl_quota
            SET
                quota_status_type_id = %s,
                cancel_date = %s,
                acquisition_date = %s,
                modified_at = %s,
                modified_by = %s,
                contemplation_date = %s,
                quota_person_type_id = %s,
                contract_number = %s,
                quota_number = %s,
                external_reference = %s
            WHERE
                quota_id = %s
        """
        md_quota_update_cursor.execute(
            query_update_quota,
            (
                quota_to_update["quota_status_type_id"],
                quota_to_update["cancel_date"],
                quota_to_update["acquisition_date"],
                "now()",
                GLUE_DEFAULT_CODE,
                quota_to_update["contemplation_date"],
                quota_to_update["quota_person_type_id"],
                quota_to_update["contract_number"],
                quota_to_update["quota_number"],
                quota_to_update["external_reference"],
                quota_to_update["quota_id"],
            ),
        )
        logger.info("External_reference atualizado com sucesso!")
    except Exception as error:
        logger.error(f"Erro ao atualizar a cota: {str(error)}")
        raise error


def delete_quota(md_quota_update_cursor, quota_id):
    try:
        logger.info(f"Deletando quota_id {quota_id}...")
        query_update_quota = """
            UPDATE md_cota.pl_quota
            SET
                id_deleted = %s,
                modified_at = %s,
                modified_by = %s
            WHERE
                quota_id = %s
        """
        md_quota_update_cursor.execute(
            query_update_quota,
            (
                True,
                "now()",
                GLUE_DEFAULT_CODE,
                quota_id,
            ),
        )
        logger.info("Cota deletada com sucesso!")
    except Exception as error:
        logger.error(f"Erro ao atualizar a cota: {str(error)}")
        raise error


def update_quota_status(
    md_quota_update_cursor, md_quta_insert_cursor, quota_id, status_type
):
    try:
        logger.info(
            f"Atualizando registro antigo do status da cota com quota_id {quota_id}"
        )
        query_update_quota_status = """
        UPDATE md_cota.pl_quota_status
        SET
            valid_to = %s,
            modified_at = %s,
            modified_by = %s
        WHERE
            quota_id = %s
            AND
            valid_to IS NULL
        """
        md_quota_update_cursor.execute(
            query_update_quota_status,
            (
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                quota_id,
            ),
        )
        logger.info("Status atualizado com sucesso!")
        insert_quota_status_into_database(md_quta_insert_cursor, quota_id, status_type)

    except Exception as error:
        logger.error(f"Erro ao atualizar o status da cota: {str(error)}")
        raise error


def update_quota_history_detail(
    md_quota_select_cursor,
    md_quota_update_cursor,
    md_quota_insert_cursor,
    quota_detail_update,
    data_source_id,
):
    try:
        logger.info(
            f"Buscando histórico da cota com quota_id {quota_detail_update['quota_id']}"
        )
        query_search_quota_history_detail = """
        SELECT *
        FROM md_cota.pl_quota_history_detail pqhd
        WHERE
            pqhd.quota_id = %s
            AND
            pqhd.valid_to IS NULL
            AND
            pqhd.is_deleted IS FALSE
        """
        md_quota_select_cursor.execute(
            query_search_quota_history_detail, [quota_detail_update["quota_id"]]
        )
        query_result_quota_history_detail = md_quota_select_cursor.fetchall()
        quota_history_detail = get_table_dict(
            md_quota_select_cursor, query_result_quota_history_detail
        )
        logger.info("Histórico da cota recuperado com sucesso!")
        if len(quota_history_detail) > 0:
            if quota_history_detail[0]["info_date"] < quota_detail_update["info_date"]:
                update_quota_history_detail_valid_to(
                    md_quota_update_cursor, quota_history_detail[0]["quota_history_detail_id"]
                )

                fields_inserted_quota_history = insert_quota_history_detail_into_database(
                    md_quota_insert_cursor, quota_detail_update
                )

                for field in fields_inserted_quota_history:
                    history_field_id = switch_quota_history_field(field, 0)
                    update_quota_field_update_date(
                        md_quota_insert_cursor,
                        quota_detail_update["info_date"],
                        quota_detail_update["quota_id"],
                        history_field_id,
                        data_source_id,
                    )

    except Exception as error:
        logger.error(f"Erro ao atualizar o histórico da cota: {str(error)}")
        raise error


def update_quota_history_detail_valid_to(
    md_cota_update_cursor, quota_history_detail_id
):
    try:
        logger.info(
            f"Atualizando histórico da cota com quota_history_detail_id {quota_history_detail_id}"
        )
        query_update_quota_history_detail = """
        UPDATE md_cota.pl_quota_history_detail
        SET
            valid_to = now()
        WHERE
            quota_history_detail_id = %s
            AND
            valid_to IS NULL
        """
        md_cota_update_cursor.execute(
            query_update_quota_history_detail,
            ([quota_history_detail_id]),
        )
        logger.info("Histórico da cota atualizado com sucesso!")

    except Exception as error:
        logger.error(f"Erro ao atualizar o histórico da cota: {str(error)}")
        raise error


def update_quota_field_update_date(
    md_quota_insert_cursor, data_info, quota_id, history_field_id, data_source_id
):
    try:
        logger.info(
            f"Atualizando data de atualização do histórico do quota_id {quota_id}, "
            f"para o campo com quota_history_field_id {history_field_id}"
        )
        query_quota_field_update_date_insert = """
            UPDATE 
                md_cota.pl_quota_field_update_date
            SET
                data_source_id = %s,
                update_date = %s,
                modified_at = %s,
                modified_by = %s
            WHERE
                quota_history_field_id = %s
            AND
                quota_id = %s
        """
        md_quota_insert_cursor.execute(
            query_quota_field_update_date_insert,
            (
                data_source_id,
                data_info,
                "now()",
                GLUE_DEFAULT_CODE,
                history_field_id,
                quota_id,
            ),
        )
        logger.info("Data de atualização do campo atualizada com sucesso!")
    except Exception as error:
        logger.error(
            f"Erro ao atualizar a data de atualização do campo da cota: {str(error)}"
        )
        raise error


def update_stage_raw(md_quota_update_cursor, id_quotas_itau):
    try:
        logger.info(f"Atualizando cota já processada no stage_raw...")
        query_update_stage_raw = f"""
            UPDATE stage_raw.tb_quotas_api tqa
            SET is_processed = true
            WHERE tqa.id_quotas_itau = {id_quotas_itau};
        """
        logger.info(f"query de atualização: {query_update_stage_raw}")
        md_quota_update_cursor.execute(query_update_stage_raw)
        logger.info("Cota processada atualizada com sucesso!")

    except Exception as error:
        logger.error(f"Erro ao atualizar o estágio bruto: {str(error)}")
        raise error


def generate_payload_customer(request_body):
    logger.info("Iniciando construção do payload de chamada para o cubees...")
    person_ext_code = (
        request_body["ownerDocument"].replace(".", "").replace("-", "").replace("/", "")
    )
    # Obter a data e hora atual
    date_today = datetime.now()

    # Adicionar 10 anos à data atual usando relativedelta
    expiring_date = (date_today + relativedelta(years=+10)).strftime("%Y-%m-%d")

    person_type = "NATURAL" if len(person_ext_code) == 11 else "LEGAL"
    contact_category = "PERSONAL" if len(person_ext_code) == 11 else "BUSINESS"
    address_category = "RESI" if len(person_ext_code) == 11 else "COMM"
    person_document_type = "CPF" if len(person_ext_code) == 11 else "CS"
    contacts = [
        {
            "contact_desc": "EMAIL PESSOAL",
            "contact": request_body["ownerEmail"],
            "contact_category": contact_category,
            "contact_type": "EMAIL",
            "preferred_contact": True,
        }
    ]
    for item in request_body["ownerPhones"]:
        contact = {
            "contact_desc": "TELEFONE",
            "contact": str(item["countryCode"])
            + str(item["areaCode"])
            + str(item["number"]),
            "contact_category": contact_category,
            "contact_type": "MOBILE",
            "preferred_contact": False,
        }
        contacts.append(contact)

    payload = {
        "person_ext_code": person_ext_code,
        "person_type": person_type,
        "administrator_code": administrator_code,
        "channel_type": "EMAIL",
        "addresses": [
            {
                "address": request_body["ownerAddressStreet"],
                "address_2": request_body["ownerAddressComplement"],
                "street_number": request_body["ownerAddressNumber"],
                "district": request_body["ownerAddressNeighborhood"],
                "zip_code": request_body["ownerAddressZipCode"],
                "address_category": address_category,
                "city": (
                    str(request_body["ownerAddressCity"])[0]
                    + str(request_body["ownerAddressCity"])[1:].lower()
                ),
                "state": request_body["ownerAddressState"],
            }
        ],
        "contacts": contacts,
        "documents": [
            {
                "document_number": person_ext_code,
                "expiring_date": expiring_date,
                "person_document_type": person_document_type,
            }
        ],
        "reactive": False,
    }

    if len(person_ext_code) == 11:
        payload["natural_person"] = {
            "full_name": request_body["ownerName"],
            "birthdate": request_body["ownerBirthday"],
        }

    else:
        payload["legal_person"] = {
            "company_name": request_body["ownerName"],
            "company_fantasy_name": request_body["ownerName"],
            "founding_date": request_body["ownerBirthday"],
        }

    logger.info("Payload gerado com sucesso!")

    return payload


def lambda_invoke(
    payload_customer, quota_id_md_quota, person_ext_code, lambda_customer_cubees
):
    customer_data = [payload_customer]
    payload_lambda = {
        "quota_id": quota_id_md_quota,
        "ownership_percentage": 1,
        "cubees_request": customer_data,
        "main_owner": person_ext_code,
    }
    request_lambda = json.dumps(payload_lambda)
    lambda_client = boto3.client("lambda")
    lambda_client.invoke(
        FunctionName=lambda_customer_cubees,
        InvocationType="Event",
        Payload=request_lambda,
    )


def put_event(event_bus_name, online_pricing_quotas):
    logger.info("Iniciando criação do evento")
    event_source = "glue"
    event_detail_type = "from_api_to_model_itau_md_cota"
    event_detail = {
        "quota_code_list": online_pricing_quotas,
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


def process_group_vacancy(
    group_vacancy,
    row_dict,
    md_quota_update_cursor,
    md_quota_insert_cursor,
    group_vacancy_to_insert,
):
    if len(group_vacancy) > 0:
        if group_vacancy[0]["info_date"] < row_dict["created_at"]:
            group_vacancy_id = group_vacancy[0]["group_vacancies_id"]
            update_group_vacancy(group_vacancy_id, md_quota_update_cursor)
            insert_group_vacancy_into_database(
                md_quota_insert_cursor, group_vacancy_to_insert
            )
    else:
        insert_group_vacancy_into_database(
            md_quota_insert_cursor, group_vacancy_to_insert
        )


def process_row(
    row_dict,
    quotas_dict,
    groups_dict,
    md_quota_update_cursor,
    md_quota_insert_cursor,
    id_adm,
    md_quota_connection,
    data_source_id,
    md_quota_select_cursor,
    quotas_online_pricing,
    quotas_auction_winner,
    lambda_customer_cubees,
    lambda_auction_winner,
):
    request_body = json.loads(row_dict["request_body"])
    row_md_quota = get_dict_by_id(
        request_body["referenceId"], quotas_dict, "external_reference"
    )
    logger.info(f"cota md_cota: {row_md_quota}")
    if "groupCode" in request_body:
        row_group_code = cd_group_right_justified(request_body["groupCode"])
    else:
        row_group_code = cd_group_right_justified(request_body["group"])
    # buscando grupo no md-cota
    logger.info(f"group_code: {row_group_code}")
    row_md_quota_group = get_dict_by_id(str(row_group_code), groups_dict, "group_code")
    logger.info(f"grupo md_cota: {row_md_quota_group}")
    quota_origin = get_quota_origin(row_dict["endpoint_generator"])
    if "proposedNumber" in request_body:
        md_quota_quota_contract = get_dict_by_id(
            str(request_body["proposedNumber"]),
            quotas_dict,
            "contract_number",
        )
    else:
        md_quota_quota_contract = None
    if quota_origin == 1:
        auction = True
        send_to_adapter = True
    else:
        auction = False
        send_to_adapter = False

    if request_body["deletionDate"] >= request_body["acquisitionDate"]:
        cancel_date = request_body["deletionDate"]
    else:
        cancel_date = None

    if (
        "ownerContemplationDate" in request_body
        and request_body["ownerContemplationDate"] is not None
        and request_body["ownerContemplationDate"] >= request_body["acquisitionDate"]
    ):
        contemplation_date = request_body["ownerContemplationDate"]
    else:
        contemplation_date = None
    if "ownerDocument" in request_body:
        person_ext_code = (
            request_body["ownerDocument"]
            .replace(".", "")
            .replace("-", "")
            .replace("/", "")
        )
        if len(person_ext_code) == 11:
            quota_person_type_id = 1
        else:
            quota_person_type_id = 2

    else:
        quota_person_type_id = None

    asset_type = switch_asset_type(request_body["productType"], 7)
    if row_md_quota is None:
        group_vacancy_to_insert = {
            "vacancies": request_body["vacantGroup"],
            "info_date": row_dict["created_at"],
            "valid_from": "now()",
        }
        if md_quota_quota_contract is not None:
            delete_quota(md_quota_insert_cursor, md_quota_quota_contract["quota_id"])
            quotas_dict.remove(md_quota_quota_contract)

        if row_md_quota_group is not None:
            group_id = row_md_quota_group["group_id"]

            update_md_quota_group(
                row_md_quota_group,
                request_body["endGroupDate"],
                request_body["quantityPlotsGroup"],
                md_quota_update_cursor,
                group_id,
            )
            group = {"group_id": group_id, "group_code": row_group_code}
            group_vacancy_to_insert["group_id"] = (group_id,)
            groups_dict.append(group)
            group_vacancy = read_group_vacancies(group_id, md_quota_select_cursor)
            process_group_vacancy(
                group_vacancy,
                row_dict,
                md_quota_update_cursor,
                md_quota_insert_cursor,
                group_vacancy_to_insert,
            )

        else:
            group, group_id = insert_group_into_database(
                md_quota_insert_cursor,
                row_group_code,
                request_body["quantityPlotsGroup"],
                id_adm,
                request_body["endGroupDate"],
            )
            groups_dict.append(group)
            group_vacancy_to_insert["group_id"] = group_id
            insert_group_vacancy_into_database(
                md_quota_insert_cursor, group_vacancy_to_insert
            )

        status_type = switch_status(request_body["quotaSituation"], 5)

        quota_to_insert = {
            "external_reference": request_body["referenceId"],
            "total_installments": request_body["quantityPlots"],
            "is_contemplated": False,
            "is_multiple_ownership": False,
            "administrator_fee": request_body["administrationFee"],
            "fund_reservation_fee": request_body["reserveFundPercentage"],
            "info_date": row_dict["created_at"],
            "quota_status_type_id": status_type,
            "administrator_id": id_adm,
            "group_id": group_id,
            "quota_origin_id": quota_origin,
            "acquisition_date": request_body["acquisitionDate"],
            "cancel_date": cancel_date,
            "contemplation_date": contemplation_date,
            "contract_number": request_body["proposedNumber"]
            if "proposedNumber" in request_body
            else None,
            "quota_number": request_body["number"]
            if "number" in request_body
            else None,
            "quota_person_type_id": quota_person_type_id,
        }
        new_quota = insert_quota_into_database(
            md_quota_insert_cursor, md_quota_connection, quota_to_insert
        )
        quota_id = new_quota["quota_id"]
        quota_code = new_quota["quota_code"]
        logger.info(f"quota_id inserido {quota_id}")
        quotas_dict.append(new_quota)
        quota_to_md_offer = {
            "quota_code": quota_code,
            "auction": auction,
            "send_to_adapter": send_to_adapter,
        }
        quotas_online_pricing.append(quota_to_md_offer)
        insert_quota_status_into_database(md_quota_insert_cursor, quota_id, status_type)

        quota_history_detail_to_insert = {
            "quota_id": quota_id,
            "old_quota_number": request_body["number"]
            if "number" in request_body
            else None,
            "installments_paid_number": request_body["quantityPaidPlots"],
            "overdue_installments_number": request_body["quantityDelayPlots"],
            "per_adm_paid": request_body["paidAdministrationFee"],
            "per_adm_to_pay": request_body["administrationFee"]
            - request_body["paidAdministrationFee"],
            "per_mutual_fund_paid": request_body["percCommonFundPaid"],
            "asset_value": request_body["totalUpdatedValue"],
            "asset_type_id": asset_type,
            "info_date": row_dict["created_at"],
            "amnt_to_pay": request_body["valueEndGroup"],
            "valid_from": "now()",
            "valid_to": None,
        }

        fields_inserted_quota_history = insert_quota_history_detail_into_database(
            md_quota_insert_cursor, quota_history_detail_to_insert
        )
        for field in fields_inserted_quota_history:
            insert_quota_field_update_date_into_database(
                md_quota_insert_cursor, quota_id, field, data_source_id, row_dict
            )

        if "ownerDocument" in request_body:
            person_ext_code = (
                request_body["ownerDocument"]
                .replace(".", "")
                .replace("-", "")
                .replace("/", "")
            )
            payload_customer = generate_payload_customer(request_body)
            lambda_invoke(
                payload_customer, quota_id, person_ext_code, lambda_customer_cubees
            )

    else:
        quota_id_md_quota = row_md_quota["quota_id"]
        group_id_md_quota = row_md_quota_group["group_id"]
        group_vacancy_to_insert = {
            "vacancies": request_body["vacantGroup"],
            "info_date": row_dict["created_at"],
            "valid_from": "now()",
            "group_id": group_id_md_quota,
        }
        group_vacancy = read_group_vacancies(group_id_md_quota, md_quota_select_cursor)
        process_group_vacancy(
            group_vacancy,
            row_dict,
            md_quota_update_cursor,
            md_quota_insert_cursor,
            group_vacancy_to_insert,
        )
        quota_to_md_offer = {
            "quota_code": row_md_quota["quota_code"],
            "auction": auction,
            "send_to_adapter": send_to_adapter,
        }
        quota_to_auction_winner = {
            "quota_code": row_md_quota["quota_code"],
        }
        if quota_origin == 2:
            quotas_auction_winner.append(quota_to_auction_winner)
            request_auction_winner = {"quota_code_list": quotas_auction_winner}
            lambda_client_auction_winner = boto3.client("lambda")
            lambda_client_auction_winner.invoke(
                FunctionName=lambda_auction_winner,
                InvocationType="Event",
                Payload=json.dumps(request_auction_winner),
            )
        quotas_online_pricing.append(quota_to_md_offer)
        status_type = switch_status(request_body["quotaSituation"], 5)
        if row_md_quota["contract_number"] is not None:
            if row_md_quota["info_date"] < row_dict["created_at"]:
                logger.info("atualizando cota md-cota")
                quota_update = {
                    "quota_id": quota_id_md_quota,
                    "quota_status_type_id": status_type,
                    "acquisition_date": request_body["acquisitionDate"],
                    "cancel_date": cancel_date,
                    "contemplation_date": contemplation_date,
                    "contract_number": request_body["proposedNumber"]
                    if "proposedNumber" in request_body
                    else None,
                    "quota_number": request_body["number"]
                    if "number" in request_body
                    else None,
                    "quota_person_type_id": quota_person_type_id,
                }
                update_quota(md_quota_update_cursor, quota_update)
                if row_md_quota["quota_status_type_id"] != status_type:
                    update_quota_status(
                        md_quota_update_cursor,
                        md_quota_insert_cursor,
                        quota_id_md_quota,
                        status_type,
                    )
                quota_history_detail_to_insert = {
                    "quota_id": quota_id_md_quota,
                    "old_quota_number": request_body["number"]
                    if "number" in request_body
                    else None,
                    "installments_paid_number": request_body["quantityPaidPlots"],
                    "overdue_installments_number": request_body["quantityDelayPlots"],
                    "per_adm_paid": request_body["paidAdministrationFee"],
                    "per_adm_to_pay": request_body["administrationFee"]
                    - request_body["paidAdministrationFee"],
                    "per_mutual_fund_paid": request_body["percCommonFundPaid"],
                    "asset_value": request_body["totalUpdatedValue"],
                    "asset_type_id": asset_type,
                    "info_date": row_dict["created_at"],
                    "amnt_to_pay": request_body["valueEndGroup"],
                    "valid_from": "now()",
                    "valid_to": None,
                }
                update_quota_history_detail(
                    md_quota_select_cursor,
                    md_quota_update_cursor,
                    md_quota_insert_cursor,
                    quota_history_detail_to_insert,
                    data_source_id,
                )

        else:
            if md_quota_quota_contract is not None and (
                md_quota_quota_contract["quota_id"] != quota_id_md_quota
            ):
                delete_quota(md_quota_update_cursor, quota_id_md_quota)
                quotas_dict.remove(md_quota_quota_contract)
            quota_update = {
                "quota_id": quota_id_md_quota,
                "quota_status_type_id": status_type,
                "contract_number": request_body["proposedNumber"]
                if "proposedNumber" in request_body
                else None,
                "quota_number": request_body["number"]
                if "number" in request_body
                else None,
                "contemplation_date": contemplation_date,
                "quota_person_type_id": quota_person_type_id,
                "acquisition_date": request_body["acquisitionDate"],
                "cancel_date": cancel_date,
                "external_reference": request_body["referenceId"],
            }
            update_quota_reference_id(md_quota_update_cursor, quota_update)
            quota_to_md_offer = {
                "quota_code": row_md_quota["quota_code"],
                "auction": auction,
                "send_to_adapter": send_to_adapter,
            }
            quotas_online_pricing.append(quota_to_md_offer)
            if row_md_quota["quota_status_type_id"] != status_type:
                update_quota_status(
                    md_quota_update_cursor,
                    md_quota_insert_cursor,
                    quota_id_md_quota,
                    status_type,
                )
            quota_history_detail_to_insert = {
                "quota_id": quota_id_md_quota,
                "old_quota_number": request_body["number"]
                if "number" in request_body
                else None,
                "installments_paid_number": request_body["quantityPaidPlots"],
                "overdue_installments_number": request_body["quantityDelayPlots"],
                "per_adm_paid": request_body["paidAdministrationFee"],
                "per_adm_to_pay": request_body["administrationFee"]
                - request_body["paidAdministrationFee"],
                "per_mutual_fund_paid": request_body["percCommonFundPaid"],
                "asset_value": request_body["totalUpdatedValue"],
                "asset_type_id": asset_type,
                "info_date": row_dict["created_at"],
                "amnt_to_pay": request_body["valueEndGroup"],
                "valid_from": "now()",
                "valid_to": None,
            }
            update_quota_history_detail(
                md_quota_select_cursor,
                md_quota_update_cursor,
                md_quota_insert_cursor,
                quota_history_detail_to_insert,
                data_source_id,
            )

        if "ownerDocument" in request_body:
            person_ext_code = (
                request_body["ownerDocument"]
                .replace(".", "")
                .replace("-", "")
                .replace("/", "")
            )
            payload_customer = generate_payload_customer(request_body)
            lambda_invoke(
                payload_customer,
                quota_id_md_quota,
                person_ext_code,
                lambda_customer_cubees,
            )

    update_stage_raw(md_quota_update_cursor, row_dict["id_quotas_itau"])


def process_pricing_query(
    groups_dict,
    id_adm,
    data_source_id,
    quotas_dict,
    md_quota_cursor,
    md_quota_update_cursor,
    md_quota_insert_cursor,
    md_quota_connection,
    md_quota_select_cursor,
    quotas_online_pricing,
    quotas_auction_winner,
    lambda_customer_cubees,
    lambda_auction_winner,
):
    batch_counter = 0
    while True:
        try:
            batch_counter += 1
            rows = md_quota_cursor.fetchmany(
                size=BATCH_SIZE
            )  # Fetch XXX rows at a time
            if not rows:
                break
            logger.info(
                f"Fetch {BATCH_SIZE} rows at a time - Batch number {batch_counter}"
            )
            for row in rows:
                row_dict = dict(row)
                logger.info(f"cota stage_raw: {row_dict}")
                process_row(
                    row_dict,
                    quotas_dict,
                    groups_dict,
                    md_quota_update_cursor,
                    md_quota_insert_cursor,
                    id_adm,
                    md_quota_connection,
                    data_source_id,
                    md_quota_select_cursor,
                    quotas_online_pricing,
                    quotas_auction_winner,
                    lambda_customer_cubees,
                    lambda_auction_winner,
                )
                md_quota_connection.commit()
        except Exception as error:
            logger.error(f"Transação revertida devido a um erro:{error}")
            raise error


def process_quotas(
    groups_dict,
    id_adm,
    data_source_id,
    quotas_dict,
    md_quota_cursor,
    md_quota_update_cursor,
    md_quota_insert_cursor,
    md_quota_connection,
    md_quota_select_cursor,
    quotas_online_pricing,
    quotas_auction_winner,
    lambda_customer_cubees,
    lambda_auction_winner,
):
    batch_counter = 0
    while True:
        try:
            batch_counter += 1
            rows = md_quota_cursor.fetchmany(
                size=BATCH_SIZE
            )  # Fetch XXX rows at a time
            if not rows:
                break
            logger.info(
                f"Fetch {BATCH_SIZE} rows at a time - Batch number {batch_counter}"
            )
            for row in rows:
                row_dict = dict(row)
                logger.info(f"cota stage_raw: {row_dict}")
                process_row(
                    row_dict,
                    quotas_dict,
                    groups_dict,
                    md_quota_update_cursor,
                    md_quota_insert_cursor,
                    id_adm,
                    md_quota_connection,
                    data_source_id,
                    md_quota_select_cursor,
                    quotas_online_pricing,
                    quotas_auction_winner,
                    lambda_customer_cubees,
                    lambda_auction_winner,
                )
                md_quota_connection.commit()
        except Exception as error:
            logger.error(f"Transação revertida devido a um erro:{error}")
            raise error


def itau_quota_ingestion() -> None:
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()
    md_quota_connection.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
    md_quota_insert_cursor = md_quota_connection.cursor()
    md_quota_update_cursor = md_quota_connection.cursor()
    md_quota_select_cursor = md_quota_connection.cursor()
    md_quota_cursor = md_quota_connection.cursor(cursor_factory=extras.RealDictCursor)
    args = getResolvedOptions(
        sys.argv,
        [
            "WORKFLOW_NAME",
            "JOB_NAME",
            "md_cota_cubees_customer_lambda",
            "md_oferta_auction_winner",
            "event_bus_name",
        ],
    )
    lambda_auction_winner = args["md_oferta_auction_winner"]
    workflow_name = args["WORKFLOW_NAME"]
    event_bus_name = args["event_bus_name"]
    job_name = args["JOB_NAME"]
    lambda_customer_cubees = args["md_cota_cubees_customer_lambda"]
    event_trigger = EventTrigger(workflow_name, job_name)
    event = event_trigger.get_event_details()
    logger.info(f"event: {event}")
    ids = None
    if event is not None:
        ids = event["detail"]["quota_id_list"]
    endpoint_pricing_query = "POST /pricingQuery"
    endpoint_quotas = "POST /quotas"
    quotas_online_pricing = []
    quotas_auction_winner = []

    try:
        id_adm = select_administrator_id(md_quota_select_cursor)
        groups_dict = select_group_md_quota(md_quota_select_cursor)
        data_source_id = select_data_source_id(md_quota_select_cursor)
        quotas_dict = select_quota_md_quota(md_quota_select_cursor, id_adm)
        fetch_select_stage_raw_quotas(
            md_quota_cursor, endpoint_pricing_query, "ITAU", ids
        )
        process_pricing_query(
            groups_dict,
            id_adm,
            data_source_id,
            quotas_dict,
            md_quota_cursor,
            md_quota_update_cursor,
            md_quota_insert_cursor,
            md_quota_connection,
            md_quota_select_cursor,
            quotas_online_pricing,
            quotas_auction_winner,
            lambda_customer_cubees,
            lambda_auction_winner,
        )
        fetch_select_stage_raw_quotas(md_quota_cursor, endpoint_quotas, "ITAU", ids)
        process_quotas(
            groups_dict,
            id_adm,
            data_source_id,
            quotas_dict,
            md_quota_cursor,
            md_quota_update_cursor,
            md_quota_insert_cursor,
            md_quota_connection,
            md_quota_select_cursor,
            quotas_online_pricing,
            quotas_auction_winner,
            lambda_customer_cubees,
            lambda_auction_winner,
        )

        put_event(event_bus_name, quotas_online_pricing)

        md_quota_connection.commit()
        logger.info(
            "Dados processados com sucesso. Todas as informações atualizadas foram inseridas no banco."
        )
    except Exception as error:
        md_quota_connection.rollback()
        logger.error("Rollback da transação efetuado devido ao erro:", error)
        raise error

    finally:
        # Close the cursor and connection
        md_quota_cursor.close()
        md_quota_connection.close()
        logger.info("Conexão com o banco finalizada.")


if __name__ == "__main__":
    itau_quota_ingestion()
