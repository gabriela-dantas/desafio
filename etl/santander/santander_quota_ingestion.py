from bazartools.common.logger import get_logger
from bazartools.common.database.glueConnection import GlueConnection
from bazartools.common.database.quotaCodeBuilder import build_quota_code

# from bazartools.common.eventTrigger import EventTrigger
# from awsglue.utils import getResolvedOptions
from psycopg2 import extras, ProgrammingError
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED
import sys
from datetime import datetime
from dateutil import relativedelta

BATCH_SIZE = 500
GLUE_DEFAULT_CODE = 2
QUOTA_ORIGIN_ADM = 1
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


def switch_status(key: str, value: int) -> str:
    status = {
        "Bem Pendente de Entrega": 1,
        "Bem a Contemplar": 4,
        "Cota Cancelada": 2,
    }
    return status.get(key, value)


def switch_asset_type(key: str, value: int) -> str:
    asset_type = {
        "AUTO": 2,
        "IMOVEL": 1,
        "SERVIC": 4,
        "CAMIN": 3,
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


def fetch_select_stage_raw_quotas(md_quota_cursor):
    try:
        query_select_stage_raw = """
        SELECT *
        FROM
            stage_raw.tb_quotas_santander_pre tqsp
        WHERE
            tqsp.is_processed IS FALSE
        """
        logger.info(f"query leitura stage_raw: {query_select_stage_raw}")
        md_quota_cursor.execute(query_select_stage_raw)
    except Exception as error:
        logger.error(
            f"Não foi possível executar"
            f" o select na tabela quotas_santander: error:{error}"
        )
        raise error


def select_group_md_quota(md_quota_select_cursor):
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
            pa.administrator_desc = 'SANTANDER ADM. CONS. LTDA'
        AND
            pg.is_deleted is false
        """
        logger.info(f"query leitura grupos md-cota: {query_select_group_md_quota}")
        md_quota_select_cursor.execute(query_select_group_md_quota)
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
        query_select_adm = """
        SELECT
            pa.administrator_id
        FROM
            md_cota.pl_administrator pa
        WHERE 
            pa.administrator_desc = 'SANTANDER ADM. CONS. LTDA'
        AND
            pa.is_deleted is false
        """
        logger.info(f"query leitura adm md-cota: {query_select_adm}")
        md_quota_select_cursor.execute(query_select_adm)
        query_result_adm = md_quota_select_cursor.fetchall()
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
            pa.administrator_desc = 'SANTANDER ADM. CONS. LTDA'
        AND
            pq.is_deleted is false
        """
        md_quota_select_cursor.execute(query_select_quota_md_quota, [id_adm])
        query_result_quotas = md_quota_select_cursor.fetchall()
        return get_table_dict(md_quota_select_cursor, query_result_quotas)
    except Exception as error:
        logger.error(
            f"Não foi possível executar" f" o select na tabela pl_quota: error:{error}"
        )
        raise error


def select_data_source_id(md_quota_select_cursor):
    try:
        query_select_data_source = """
        SELECT 
            pds.data_source_id 
        FROM 
            md_cota.pl_data_source pds 
        WHERE 
            pds.data_source_desc = 'FILE'
        AND
            pds.is_deleted is false
        """
        md_quota_select_cursor.execute(query_select_data_source)
        query_result_data_source = md_quota_select_cursor.fetchall()
        data_source_dict = get_table_dict(
            md_quota_select_cursor, query_result_data_source
        )
        return data_source_dict[0]["data_source_id"]
    except Exception as error:
        logger.info(
            f"Erro ao fazer o select na tabela pl_data_source_desc, error:{error}"
        )
        raise error


def update_md_quota_group(
    row_md_quota_group, group_end_date, row_dict, md_quota_update_cursor, group_id
):
    try:
        logger.info(f"group md-cota: {row_md_quota_group}")

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
                row_dict["pz_contratado"],
                "now()",
                GLUE_DEFAULT_CODE,
                group_id,
            ),
        )
    except Exception as error:
        logger.error(f"Erro ao executar a query de atualização: {str(error)}")


def insert_group_into_database(
    md_cota_insert_cursor, row_group_code, row_dict, id_adm, group_end_date
):
    try:
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
                row_dict["pz_contratado"],
                id_adm,
                group_end_date,
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )

        group_inserted = md_cota_insert_cursor.fetchall()
        group_id = get_table_dict(md_cota_insert_cursor, group_inserted)[0]["group_id"]
        logger.info(f"group_id inserido {group_id}")

        return {"group_id": group_id, "group_code": row_group_code}, group_id

    except Exception as error:
        logger.error(f"Erro ao executar a query de inserção: {str(error)}")
        raise error


def insert_quota_into_database(
    md_quota_insert_cursor,
    md_quota_connection,
    row_dict,
    info_date,
    status_type,
    id_adm,
    group_id,
):
    try:
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
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING *
        """
        md_quota_insert_cursor.execute(
            query_insert_quota,
            (
                quota_code,
                row_dict["nr_contrato"],
                row_dict["cd_cota"],
                row_dict["pz_contratado"],
                False,
                False,
                row_dict["pc_tx_adm"],
                row_dict["pc_fundo_reserva"],
                info_date,
                status_type,
                id_adm,
                group_id,
                row_dict["nr_contrato"],
                QUOTA_ORIGIN_ADM,
                row_dict["dt_canc"],
                row_dict["dt_venda"],
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )

        new_quota = get_table_dict(
            md_quota_insert_cursor, md_quota_insert_cursor.fetchall()
        )[0]
        return new_quota

    except Exception as error:
        logger.error(f"Erro ao executar a query de inserção da quota: {str(error)}")
        raise error


def insert_quota_status_into_database(md_quota_insert_cursor, quota_id, status_type):
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
                %s, %s, %s, %s
            )
        """
        md_quota_insert_cursor.execute(
            query_insert_quota_status,
            (quota_id, status_type, GLUE_DEFAULT_CODE, GLUE_DEFAULT_CODE),
        )

    except Exception as error:
        logger.error(
            f"Erro ao executar a query de inserção do status da quota: {str(error)}"
        )
        raise error


def insert_quota_history_detail_into_database(
    md_quota_insert_cursor, quota_id, row_dict, info_date, asset_type
):
    try:
        quota_history_detail_insert = """
            INSERT INTO md_cota.pl_quota_history_detail
            (
                quota_id,
                installments_paid_number,
                old_quota_number,
                per_mutual_fund_paid,
                asset_value,
                asset_type_id,
                per_adm_paid,
                per_reserve_fund_paid,
                info_date,
                valid_from,
                created_by,
                modified_by
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        md_quota_insert_cursor.execute(
            quota_history_detail_insert,
            (
                quota_id,
                row_dict["qt_parcela_paga"],
                row_dict["cd_cota"],
                row_dict["pc_fc_pago"],
                row_dict["vl_bem_atual"],
                asset_type,
                row_dict["pc_tx_pago"],
                row_dict["pc_fr_pago"],
                info_date,
                "now()",
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )

        fields_inserted_quota_history = [
            "installments_paid_number",
            "old_quota_number",
            "per_mutual_fund_paid",
            "asset_value",
            "asset_type_id",
            "per_adm_paid",
            "per_reserve_fund_paid",
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
                row_dict["data_info"],
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )

        query_update_stage_raw = f"""
            UPDATE stage_raw.tb_quotas_santander_pre tqsp
            SET is_processed = true
            WHERE tqsp.id_quotas_santander = {row_dict['id_quotas_santander']};
        """
        logger.info(f"query de atualização: {query_update_stage_raw}")
        md_quota_insert_cursor.execute(query_update_stage_raw)

    except Exception as error:
        logger.error(
            f"Erro ao executar a query de inserção na tabela de datas de atualização do campo da quota: {str(error)}"
        )
        raise error


def update_quota(
    md_cota_update_cursor, quota_id, status_type, cancel_date, acquisition_date
):
    try:
        query_update_quota = """
            UPDATE md_cota.pl_quota
            SET
                quota_status_type_id = %s,
                cancel_date = %s,
                acquisition_date = %s,
                modified_at = %s,
                modified_by = %s
            WHERE
                quota_id = %s
        """
        md_cota_update_cursor.execute(
            query_update_quota,
            (
                status_type,
                cancel_date,
                acquisition_date,
                "now()",
                GLUE_DEFAULT_CODE,
                quota_id,
            ),
        )
    except Exception as error:
        logger.error(f"Erro ao atualizar a cota: {str(error)}")
        raise error


def update_quota_status(
    md_quota_update_cursor, md_quta_insert_cursor, quota_id, status_type
):
    try:
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
        insert_quota_status_into_database(md_quta_insert_cursor, quota_id, status_type)

    except Exception as error:
        logger.error(f"Erro ao atualizar o status da cota: {str(error)}")
        raise error


def update_quota_history_detail(
    md_quota_select_cursor,
    md_quota_update_cursor,
    md_quota_insert_cursor,
    row_dict,
    quota_id,
    info_date,
    data_source_id,
):
    try:
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
        md_quota_select_cursor.execute(query_search_quota_history_detail, [quota_id])
        query_result_quota_history_detail = md_quota_select_cursor.fetchall()
        quota_history_detail = get_table_dict(
            md_quota_select_cursor, query_result_quota_history_detail
        )[0]

        if quota_history_detail["info_date"] < row_dict["data_info"]:
            update_quota_history_detail_valid_to(
                md_quota_update_cursor, quota_history_detail["quota_history_detail_id"]
            )

            asset_type = switch_asset_type(row_dict["cd_produto"], 7)
            fields_inserted_quota_history = insert_quota_history_detail_into_database(
                md_quota_insert_cursor, quota_id, row_dict, info_date, asset_type
            )

            for field in fields_inserted_quota_history:
                history_field_id = switch_quota_history_field(field, 0)
                update_quota_field_update_date(
                    md_quota_insert_cursor,
                    row_dict,
                    quota_id,
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

    except Exception as error:
        logger.error(f"Erro ao atualizar o histórico da cota: {str(error)}")
        raise error


def update_quota_field_update_date(
    md_quota_insert_cursor, row_dict, quota_id, history_field_id, data_source_id
):
    try:
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
                row_dict["data_info"],
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )
    except Exception as error:
        logger.error(
            f"Erro ao atualizar a data de atualização do campo da cota: {str(error)}"
        )
        raise error


def update_stage_raw(md_quota_update_cursor, id_quotas_santander):
    try:
        query_update_stage_raw = f"""
            UPDATE stage_raw.tb_quotas_santander_pre tqsp
            SET is_processed = true
            WHERE tqsp.id_quotas_santander = {id_quotas_santander};
        """
        logger.info(f"query de atualização: {query_update_stage_raw}")
        md_quota_update_cursor.execute(query_update_stage_raw)

    except Exception as error:
        logger.error(f"Erro ao atualizar o estágio bruto: {str(error)}")
        raise error


def process_row(
    row_dict,
    row,
    quotas_dict,
    groups_dict,
    md_quota_update_cursor,
    md_quota_insert_cursor,
    id_adm,
    md_quota_connection,
    data_source_id,
    md_quota_select_cursor,
):
    row_md_quota = get_dict_by_id(
        row_dict["nr_contrato"], quotas_dict, "external_reference"
    )
    logger.info(f"cota md_cota: {row_md_quota}")
    row_group_code = cd_group_right_justified(row_dict["cd_grupo"])
    # buscando grupo no md-cota
    row_md_quota_group = get_dict_by_id(row_group_code, groups_dict, "group_code")
    logger.info(f"grupo md_cota: {row_md_quota_group}")
    info_date = row["data_info"]
    today = datetime.today()

    assembly_since_statement = relativedelta.relativedelta(today, info_date).months

    total_assembly = row_dict["pz_decorrido_grupo"] + assembly_since_statement

    assembly_to_end = row_dict["pz_contratado"] - total_assembly

    group_end_date = today + relativedelta.relativedelta(months=assembly_to_end)
    if row_md_quota is None:
        if row_md_quota_group is not None:
            group_id = row_md_quota_group["group_id"]
            update_md_quota_group(
                row_md_quota_group,
                group_end_date,
                row_dict,
                md_quota_update_cursor,
                group_id,
            )
        else:
            groups, group_id = insert_group_into_database(
                md_quota_insert_cursor, row_group_code, row_dict, id_adm, group_end_date
            )
            groups_dict.append(groups)
        status_type = switch_status(row["nm_situ_entrega_bem"], 5)
        new_quota = insert_quota_into_database(
            md_quota_insert_cursor,
            md_quota_connection,
            row_dict,
            info_date,
            status_type,
            id_adm,
            group_id,
        )
        quota_id = new_quota["quota_id"]
        logger.info(f"quota_id inserido {quota_id}")
        quotas_dict.append(new_quota)
        asset_type = switch_asset_type(row_dict["cd_produto"], 7)
        insert_quota_status_into_database(md_quota_insert_cursor, quota_id, status_type)
        fields_inserted_quota_history = insert_quota_history_detail_into_database(
            md_quota_insert_cursor, quota_id, row_dict, info_date, asset_type
        )
        for field in fields_inserted_quota_history:
            insert_quota_field_update_date_into_database(
                md_quota_insert_cursor, quota_id, field, data_source_id, row_dict
            )
    else:
        quota_id_md_quota = row_md_quota["quota_id"]
        status_type = switch_status(row_dict["nm_situ_entrega_bem"], 5)
        logger.info(f"status cota: {status_type}")

        if row_md_quota["info_date"] < row_dict["data_info"]:
            logger.info(f'quota_status_type_id {row_md_quota["quota_status_type_id"]}')

            if row_md_quota["quota_status_type_id"] != status_type:
                logger.info("atualizando cota md-cota")
                update_quota(
                    md_quota_update_cursor,
                    quota_id_md_quota,
                    status_type,
                    row_dict["dt_canc"],
                    row_dict["dt_venda"],
                )

        update_quota_status(
            md_quota_update_cursor,
            md_quota_insert_cursor,
            quota_id_md_quota,
            status_type,
        )

        update_quota_history_detail(
            md_quota_select_cursor,
            md_quota_update_cursor,
            md_quota_insert_cursor,
            row_dict,
            quota_id_md_quota,
            info_date,
            data_source_id,
        )
        update_stage_raw(md_quota_update_cursor, row_dict["id_quotas_santander"])


def process_all(
    groups_dict,
    id_adm,
    data_source_id,
    quotas_dict,
    md_quota_cursor,
    md_quota_update_cursor,
    md_quota_insert_cursor,
    md_quota_connection,
    md_quota_select_cursor,
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
                row_dict = dict(row)
                logger.info(f"cota stage_raw: {row_dict}")
                process_row(
                    row_dict,
                    row,
                    quotas_dict,
                    groups_dict,
                    md_quota_update_cursor,
                    md_quota_insert_cursor,
                    id_adm,
                    md_quota_connection,
                    data_source_id,
                    md_quota_select_cursor,
                )
        except ProgrammingError as error:
            logger.error(f"Transação revertida devido a um erro:{error}")


def santander_quota_ingestion() -> None:
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()
    md_quota_connection.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
    md_quota_insert_cursor = md_quota_connection.cursor()
    md_quota_update_cursor = md_quota_connection.cursor()
    md_quota_select_cursor = md_quota_connection.cursor()
    md_quota_cursor = md_quota_connection.cursor(cursor_factory=extras.RealDictCursor)
    # args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'JOB_NAME'])
    # workflow_name = args['WORKFLOW_NAME']
    # job_name = args['JOB_NAME']
    # event_trigger = EventTrigger(workflow_name, job_name)
    # event = event_trigger.get_event_details()
    # logger.info(f'event: {event}')

    try:
        id_adm = select_administrator_id(md_quota_select_cursor)
        groups_dict = select_group_md_quota(md_quota_select_cursor)
        data_source_id = select_data_source_id(md_quota_select_cursor)
        quotas_dict = select_quota_md_quota(md_quota_select_cursor, id_adm)
        fetch_select_stage_raw_quotas(md_quota_cursor)
        process_all(
            groups_dict,
            id_adm,
            data_source_id,
            quotas_dict,
            md_quota_cursor,
            md_quota_update_cursor,
            md_quota_insert_cursor,
            md_quota_connection,
            md_quota_select_cursor,
        )
        md_quota_connection.commit()
        logger.info("Dados processados com sucesso.")
    except Exception as error:
        raise error

    finally:
        # Close the cursor and connection
        md_quota_cursor.close()
        md_quota_connection.close()
        logger.info("Connection closed.")


if __name__ == "__main__":
    santander_quota_ingestion()
