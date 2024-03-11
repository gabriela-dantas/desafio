from bazartools.common.logger import get_logger
from bazartools.common.database.glueConnection import GlueConnection
from bazartools.common.eventTrigger import EventTrigger
from awsglue.utils import getResolvedOptions
import sys

import json

BATCH_SIZE = 500
GLUE_DEFAULT_CODE = 2
logger = get_logger()


def get_table_dict(cursor, rows):
    column_names = [desc[0] for desc in cursor.description]

    # Create a list of dictionaries using list comprehension
    return [dict(zip(column_names, row)) for row in rows]


def cd_group_right_justified(group: str) -> str:
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


def switch_status(key: str, value: int):
    status = {
        "1": 1,
        "3": 4,
        "EXCLUIDOS": 2,
        "2": 3,
    }
    return status.get(key, value)


def switch_quota_history_detail():
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
    return quota_history_field


def fetch_select_quotas_md_quota(md_quota_cursor):
    try:
        query_select_quotas_md_quota = """SELECT 
            *
            FROM 
            md_cota.pl_quota pq 
            LEFT JOIN md_cota.pl_administrator pa ON pa.administrator_id = pq.administrator_id
            WHERE 
            pa.administrator_desc = 'PORTO SEGURO ADM. CONS. LTDA'
            AND
            pq.is_deleted is false
            """
        logger.info(f"query leitura quotas md-cota: {query_select_quotas_md_quota}")
        md_quota_cursor.execute(query_select_quotas_md_quota)
        query_result_quotas = md_quota_cursor.fetchall()
        return get_table_dict(md_quota_cursor, query_result_quotas)
    except Exception as error:
        logger.error(f"Erro ao buscar dados na pl_quota: error:{error}")
        raise error


def get_quotas_to_update(md_quotas_cursor):
    try:
        query_quotas_to_update = """
            SELECT
                *
            FROM
                stage_raw.tb_quotas_api tqa
            WHERE
                tqa.endpoint_generator = 'POST /contract/update-contract'
                AND tqa.is_processed IS FALSE
                AND tqa.administrator = 'PORTO'
        """

        logger.info(
            f"Query para leitura de quotas reativadas em stage_raw: {query_quotas_to_update}"
        )

        md_quotas_cursor.execute(query_quotas_to_update)
    except Exception as error:
        logger.error(f"Error ao executar a consulta de quotas reativadas: {error}")


def get_data_source_id(md_quota_cursor):
    try:
        query_select_data_source = """
            SELECT pds.data_source_id 
            FROM md_cota.pl_data_source pds 
            WHERE pds.data_source_desc = 'API'
            AND
            pds.is_deleted is false
        """

        md_quota_cursor.execute(query_select_data_source)
        query_result_data_source = md_quota_cursor.fetchall()
        data_source_dict = get_table_dict(md_quota_cursor, query_result_data_source)
        return data_source_dict[0]["data_source_id"]
    except Exception as error:
        logger.info(f"Erro ao executar a consulta do data source: {error}")
        raise error


def update_quotas_api_status(md_quota_update_cursor, row_dict):
    try:
        logger.info(f"Cota não encontrada para atualização {row_dict['request_body']}")

        query_update_stage_raw = """
            UPDATE stage_raw.tb_quotas_api
            SET is_processed = TRUE
            WHERE id_quotas_itau = %s;
        """

        md_quota_update_cursor.execute(
            query_update_stage_raw, [row_dict["id_quotas_itau"]]
        )
        logger.info(f"Query de atualização: {query_update_stage_raw}")

    except Exception as error:
        logger.error(f"Erro ao executar a atualização: {error}")
        raise error


def update_quota_status(md_quota_update_cursor, quota_id):
    try:
        query_update_quota_status = """
            UPDATE md_cota.pl_quota_status
            SET
                valid_to = %s,
                modified_at = %s,
                modified_by = %s
            WHERE
                quota_id = %s
                AND valid_to IS NULL
        """

        md_quota_update_cursor.execute(
            query_update_quota_status,
            ("now()", "now()", GLUE_DEFAULT_CODE, quota_id),
        )

    except Exception as error:
        logger.error(f"Erro ao executar a atualização do status de cota: {error}")
        raise error


def insert_quota_status(md_quota_insert_cursor, quota_id, status_type):
    try:
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
                %s,%s,%s,%s
            )
        """
        md_quota_insert_cursor.execute(
            query_insert_quota_status,
            (quota_id, status_type, GLUE_DEFAULT_CODE, GLUE_DEFAULT_CODE),
        )

    except Exception as error:
        logger.error(f"Erro ao executar a inserção do status de cota: {error}")
        raise error


def update_quota(md_quota_update_cursor, quota_id, status_type, new_share_number):
    try:
        query_update_quota = """
            UPDATE md_cota.pl_quota
            SET
                quota_status_type_id = %s,
                quota_number = %s,
                modified_at = %s,
                modified_by = %s
            WHERE
                quota_id = %s
        """

        md_quota_update_cursor.execute(
            query_update_quota,
            (status_type, new_share_number, "now()", GLUE_DEFAULT_CODE, quota_id),
        )
    except Exception as error:
        logger.error(f"Erro ao executar a atualização da cota: {error}")
        raise error


def get_quota_history_detail(md_quota_select_cursor, quota_id):
    try:
        query_search_quota_history_detail = """
            SELECT
                *
            FROM
                md_cota.pl_quota_history_detail pqhd
            WHERE
                pqhd.quota_id = %s
                AND pqhd.valid_to IS NULL
                AND pqhd.is_deleted IS FALSE
        """

        md_quota_select_cursor.execute(query_search_quota_history_detail, [quota_id])
        query_result_quota_history_detail = md_quota_select_cursor.fetchall()
        return get_table_dict(
            md_quota_select_cursor, query_result_quota_history_detail
        )[0]

    except Exception as error:
        # Trate a exceção de acordo com sua lógica de manipulação de erros
        logger.error(
            f"Erro ao executar a busca do histórico detalhado da cota: {error}"
        )
        raise error


def update_quota_history_detail(md_quota_update_cursor, quota_history_detail_id):
    try:
        query_update_quota_history_detail = """
            UPDATE md_cota.pl_quota_history_detail
            SET valid_to = NOW()
            WHERE quota_history_detail_id = %s
            AND valid_to IS NULL
        """

        logger.info(
            f"quota history detail id: {quota_history_detail_id['quota_history_detail_id']}"
        )
        md_quota_update_cursor.execute(
            query_update_quota_history_detail,
            [quota_history_detail_id["quota_history_detail_id"]],
        )

    except Exception as error:
        logger.error(
            f"Erro ao executar a atualização do histórico detalhado da cota: {error}"
        )
        raise error


def insert_pl_quota_history_detail(
    md_quota_insert_cursor, request_body, quota_history_detail, quota_id, row_dict
):
    try:
        quota_history_detail_insert = """
            INSERT INTO md_cota.pl_quota_history_detail
            (
                quota_id, old_quota_number, old_digit, quota_plan, installments_paid_number,
                overdue_installments_number, overdue_percentage, per_amount_paid,
                per_mutual_fund_paid, per_reserve_fund_paid, per_adm_paid,
                per_subscription_paid, per_mutual_fund_to_pay, per_reserve_fund_to_pay,
                per_adm_to_pay, per_subscription_to_pay, per_insurance_to_pay,
                per_install_diff_to_pay, per_total_amount_to_pay, amnt_mutual_fund_to_pay,
                amnt_reserve_fund_to_pay, amnt_adm_to_pay, amnt_subscription_to_pay,
                amnt_insurance_to_pay, amnt_fine_to_pay, amnt_interest_to_pay,
                amnt_others_to_pay, amnt_install_diff_to_pay, amnt_to_pay,
                quitter_assembly_number, cancelled_assembly_number, adjustment_date,
                current_assembly_date, current_assembly_number, asset_adm_code,
                asset_description, asset_value, asset_type_id, info_date, valid_from,
                created_by, modified_by
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        md_quota_insert_cursor.execute(
            quota_history_detail_insert,
            (
                quota_id,
                request_body["newShareNumber"],
                quota_history_detail["old_digit"],
                quota_history_detail["quota_plan"],
                quota_history_detail["installments_paid_number"],
                quota_history_detail["overdue_installments_number"],
                quota_history_detail["overdue_percentage"],
                quota_history_detail["per_amount_paid"],
                quota_history_detail["per_mutual_fund_paid"],
                quota_history_detail["per_reserve_fund_paid"],
                quota_history_detail["per_adm_paid"],
                quota_history_detail["per_subscription_paid"],
                quota_history_detail["per_mutual_fund_to_pay"],
                quota_history_detail["per_reserve_fund_to_pay"],
                quota_history_detail["per_adm_to_pay"],
                quota_history_detail["per_subscription_to_pay"],
                quota_history_detail["per_insurance_to_pay"],
                quota_history_detail["per_install_diff_to_pay"],
                quota_history_detail["per_total_amount_to_pay"],
                quota_history_detail["amnt_mutual_fund_to_pay"],
                quota_history_detail["amnt_reserve_fund_to_pay"],
                quota_history_detail["amnt_adm_to_pay"],
                quota_history_detail["amnt_subscription_to_pay"],
                quota_history_detail["amnt_insurance_to_pay"],
                quota_history_detail["amnt_fine_to_pay"],
                quota_history_detail["amnt_interest_to_pay"],
                quota_history_detail["amnt_others_to_pay"],
                quota_history_detail["amnt_install_diff_to_pay"],
                quota_history_detail["amnt_to_pay"],
                quota_history_detail["quitter_assembly_number"],
                quota_history_detail["cancelled_assembly_number"],
                quota_history_detail["adjustment_date"],
                quota_history_detail["current_assembly_date"],
                quota_history_detail["current_assembly_number"],
                quota_history_detail["asset_adm_code"],
                quota_history_detail["asset_description"],
                request_body["goodValue"],
                quota_history_detail["asset_type_id"],
                row_dict["created_at"],
                "now()",
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )

    except Exception as error:
        logger.error(f"Erro ao fazer insert in pl_quota_history_detail: {error}")
        raise error


def insert_quota_field_update_data(
    md_quota_insert_cursor, quota_id, history_field_id, data_source_id, row_dict
):
    try:
        query_quota_field_update_date_insert = """
            INSERT INTO md_cota.pl_quota_field_update_date
            (
                quota_id, quota_history_field_id, data_source_id, update_date,
                created_by, modified_by
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
    except Exception as error:
        logger.error(f"Erro ao executar a query de inserção: {error}")
        raise error


def execute_update_stage_raw_query(md_quota_update_cursor, row_dict):
    try:
        query_update_stage_raw = f"""
            UPDATE stage_raw.tb_quotas_api
            SET is_processed = true
            WHERE id_quotas_itau = {row_dict['id_quotas_itau']};
        """
        logger.info(f"Query de atualização: {query_update_stage_raw}")
        md_quota_update_cursor.execute(query_update_stage_raw)
    except Exception as error:
        logger.error(f"Erro ao executar a query de atualização: {error}")
        raise error


def process_row(
    row_dict,
    quota_md_quotas_dict,
    md_quota_update_cursor,
    md_quota_insert_cursor,
    md_quota_select_cursor,
    data_source_id,
):
    request_body = json.loads(row_dict["request_body"])
    row_md_quota = get_dict_by_id(
        str(request_body["contract"]),
        quota_md_quotas_dict,
        "contract_number",
    )
    if row_md_quota is None:
        update_quotas_api_status(md_quota_update_cursor, row_dict)
    else:
        quota_id = row_md_quota["quota_id"]
        status_type = switch_status("1", 5)
        if status_type != row_md_quota["quota_status_type_id"]:
            update_quota_status(md_quota_update_cursor, quota_id)
            insert_quota_status(md_quota_insert_cursor, quota_id, status_type)
        update_quota(
            md_quota_update_cursor,
            quota_id,
            status_type,
            request_body["newShareNumber"],
        )
        quota_history_detail = get_quota_history_detail(
            md_quota_select_cursor, quota_id
        )
        update_quota_history_detail(md_quota_update_cursor, quota_history_detail)
        quota_history_detail_to_insert = {}
        quota_history_field = switch_quota_history_detail()
        for keyword in quota_history_field:
            quota_history_detail_to_insert[keyword] = quota_history_detail[keyword]
        insert_pl_quota_history_detail(
            md_quota_insert_cursor,
            request_body,
            quota_history_detail,
            quota_id,
            row_dict,
        )

        fields_inserted_quota_history = [
            "old_quota_number",
            "asset_value",
        ]
        for field in fields_inserted_quota_history:
            history_field_id = quota_history_field.get(field, 0)
            insert_quota_field_update_data(
                md_quota_insert_cursor,
                quota_id,
                history_field_id,
                data_source_id,
                row_dict,
            )
        execute_update_stage_raw_query(md_quota_update_cursor, row_dict)


def process_all(
    md_quota_cursor,
    quota_md_quotas_dict,
    md_quota_update_cursor,
    md_quota_insert_cursor,
    md_quota_select_cursor,
    data_source_id,
    md_quota_connection,
):
    batch_counter = 0
    while True:
        try:
            batch_counter += 1
            rows = md_quota_cursor.fetchmany(
                size=BATCH_SIZE
            )  # Fetch XXX rows at a time
            column_names = [desc[0] for desc in md_quota_cursor.description]
            if not rows:
                break
            logger.info(
                f"Fetch {BATCH_SIZE} rows at a time - Batch number {batch_counter}"
            )
            for row in rows:
                row_dict = dict(zip(column_names, row))
                process_row(
                    row_dict,
                    quota_md_quotas_dict,
                    md_quota_update_cursor,
                    md_quota_insert_cursor,
                    md_quota_select_cursor,
                    data_source_id,
                )

            md_quota_connection.commit()
        except Exception as error:
            logger.error(error)


def porto_quota_reactivate():
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()
    md_quota_insert_cursor = md_quota_connection.cursor()
    md_quota_select_cursor = md_quota_connection.cursor()
    md_quota_update_cursor = md_quota_connection.cursor()
    md_quota_cursor = md_quota_connection.cursor()
    args = getResolvedOptions(sys.argv, ["WORKFLOW_NAME", "JOB_NAME"])
    workflow_name = args["WORKFLOW_NAME"]
    job_name = args["JOB_NAME"]
    event_trigger = EventTrigger(workflow_name, job_name)
    event = event_trigger.get_event_details()
    logger.info(f"event: {event}")
    try:
        data_source_id = get_data_source_id(md_quota_cursor)
        quota_md_quotas_dict = fetch_select_quotas_md_quota(md_quota_cursor)
        get_quotas_to_update(md_quota_cursor)
        process_all(
            md_quota_cursor,
            quota_md_quotas_dict,
            md_quota_update_cursor,
            md_quota_insert_cursor,
            md_quota_select_cursor,
            data_source_id,
            md_quota_connection,
        )
        logger.info("Dados processados com sucesso.")
    except Exception as error:
        logger.error(f"Processamento de dados não concluído. Error:{error}")
        raise error
    finally:
        md_quota_connection.close()
        logger.info("Conexão encerrada.")


if __name__ == "__main__":
    porto_quota_reactivate()
