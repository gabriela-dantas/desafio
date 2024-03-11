import json
from datetime import datetime
from enum import Enum

import boto3
from botocore.exceptions import ClientError
from dateutil import relativedelta

from bazartools.common.logger import get_logger
from bazartools.common.database.glueConnection import GlueConnection
from bazartools.common.database.quotaCodeBuilder import build_quota_code

# from awsglue.utils import getResolvedOptions

BATCH_SIZE = 500
GLUE_DEFAULT_CODE = 2
logger = get_logger()


def get_default_datetime() -> datetime:
    return datetime.now()


def string_right_justified(group: str) -> str:
    if len(str(group)) == 5:
        code_group = str(group)
    else:
        code_group = str(group).rjust(5, "0")
    return code_group


def get_table_dict(cursor, rows):
    column_names = [desc[0] for desc in cursor.description]
    return [dict(zip(column_names, row)) for row in rows]


def cnpj_justified(cnpj: str) -> str:
    justified = str(cnpj) if len(str(cnpj)) == 14 else str(cnpj).rjust(14, "0")
    return justified


def cpf_justified(cpf: str) -> str:
    justified = str(cpf) if len(str(cpf)) == 11 else str(cpf).rjust(11, "0")
    return justified


def get_dict_by_id(id_item: str, data_list: list, field_name: str):
    for item_list in data_list:
        if item_list[field_name] == id_item:
            return item_list
    return None


def status_type_dict(key, value):
    status = {"EXCLUDED": 2, "DESISTENTES": 4, "ATIVOS": 1, "EM_ATRASO": 3}
    return status.get(key, value)


def asset_type_dict(key, value):
    asset = {
        "VEICULOS_PESADOS": 3,
        "VEICULOS_LEVES": 2,
        "IMOVEIS": 1,
        "MOTOCICLETAS": 4,
    }
    return asset.get(key, value)


def switch_asset_type_dict(key, value):
    switch_asset = {
        "VEÍCULOS PESADOS": 3,
        "VEÍCULOS LEVES": 2,
        "IMÓVEIS": 1,
        "MOTOCICLETAS": 4,
    }
    return switch_asset.get(key, value)


def switch_status_dict(key, value):
    status = {
        "A": 1,
        "D": 4,
        "C": 2,
        "EM ATRASO": 3,
    }
    return status.get(key, value)


def switch_quota_history_detail_dict():
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


class Constants(Enum):
    CASE_DEFAULT_TYPES = 5
    CASE_DEFAULT_ASSET_TYPES = 7
    CASE_DEFAULT_HISTORY_DETAIL_FIELD = 0
    QUOTA_ORIGIN_ADM = 1
    QUOTA_ORIGIN_CUSTOMER = 2


def read_data_stage_raw_quotas_volks(md_quota_cursor):
    try:
        logger.info("Buscando dados no stage raw...")
        stage_raw_quotas_pre = """
            SELECT *
            FROM stage_raw.tb_quotas_volks_pre
            WHERE is_processed is FALSE;
            """
        md_quota_cursor.execute(stage_raw_quotas_pre)
        logger.info("Busca no stage raw efetuado com sucesso")
    except Exception as error:
        logger.error(f"Erro ao busca dados na tb_quotas_volks_pre. Error:{error}")
        raise error


def read_adm_id(md_quota_select_cursor):
    try:
        logger.info("Buscando id da adm...")
        query_select_adm = """
            SELECT administrator_id
            FROM md_cota.pl_administrator
            WHERE administrator_code = '0000000289'
            AND is_deleted is false;
            """
        md_quota_select_cursor.execute(query_select_adm)
        query_result_adm = md_quota_select_cursor.fetchall()
        adm_dict = get_table_dict(md_quota_select_cursor, query_result_adm)
        logger.info("Busca do id da adm efetuada com sucesso.")
        return adm_dict[0]["administrator_id"]
    except Exception as error:
        logger.error(f"Error ao buscar id_adm no banco,error:{error}")
        raise error


def read_groups_pl_group(id_adm, md_quota_select_cursor):
    try:
        logger.info("Buscando informações de grupo na pl_group")
        query_groups_pl_group = f"""
            SELECT * 
            FROM md_cota.pl_group
            WHERE administrator_id = {id_adm} AND is_deleted is FALSE;
            """
        md_quota_select_cursor.execute(query_groups_pl_group)
        query_result_groups = md_quota_select_cursor.fetchall()
        logger.info("Informações de grupos recuperadas com sucesso.")
        return get_table_dict(md_quota_select_cursor, query_result_groups)
    except Exception as error:
        logger.error(f"Erro ao buscas informações de grupos, error:{error}")
        raise error


def read_quotas_pl_quota(id_adm, md_quota_select_cursor):
    try:
        logger.info("Buscando dados de quotas na pl_quota")
        query_quotas_md_quota = f"""
            SELECT * 
            FROM md_cota.pl_quota
            WHERE administrator_id = {id_adm} AND is_deleted is FALSE;
            """
        md_quota_select_cursor.execute(query_quotas_md_quota)
        query_result_groups = md_quota_select_cursor.fetchall()
        logger.info("Dados da pl_quota obtidos com sucesso.")
        return get_table_dict(md_quota_select_cursor, query_result_groups)
    except Exception as error:
        logger.error(f"Error ao buscas informações de quotas, error:{error}")
        raise error


def read_quotas_volks(md_quota_select_cursor):
    try:
        query_quotas_volks = """
            SELECT * 
            FROM stage_raw.tb_quotas_volks_pre
            WHERE is_processed is FALSE;
            """
        logger.info("Obtendo dados não processados da stage_raw.")
        md_quota_select_cursor.execute(query_quotas_volks)
        query_result_quotas = md_quota_select_cursor.fetchall()
        logger.info("Obtidos dados de stage_raw")
        return get_table_dict(md_quota_select_cursor, query_result_quotas)
    except Exception as error:
        logger.error(f"Erro ao buscas informações de quotas volks, error:{error}")
        raise error


def get_data_source_id(md_quota_select_cursor):
    try:
        query_data_source_id = """
            SELECT data_source_id 
            FROM md_cota.pl_data_source
            WHERE data_source_desc = 'FILE';
            """
        logger.info("Obtendo source_id pl_data_source")
        md_quota_select_cursor.execute(query_data_source_id)
        query_result_adm = md_quota_select_cursor.fetchall()
        adm_dict = get_table_dict(md_quota_select_cursor, query_result_adm)
        logger.info("Obtido source_id pl_data_source")
        return adm_dict[0]["data_source_id"] if adm_dict is not None else None
    except Exception as error:
        logger.error(f"Erro ao buscar id, error:{error}")
        raise error


def read_tb_client_volks_pre(cd_group, cd_quota, cd_digito, md_quota_select_cursor):
    try:
        query_tb_client = f"""
            SELECT * 
            FROM stage_raw.tb_clientes_volks_pre
            WHERE is_processed is FALSE
              AND cd_grupo = '{cd_group}'
              AND cd_cota = '{cd_quota}'
              AND cd_digito = '{cd_digito}';
            """
        logger.info("Obtendo dados de stage_raw tb_clientes_volks_pre")
        md_quota_select_cursor.execute(query_tb_client)
        query_result_client = md_quota_select_cursor.fetchall()
        logger.info("Obitido dados de stage_raw tb_clientes_volks_pre")
        return get_table_dict(md_quota_select_cursor, query_result_client)
    except Exception as error:
        logger.error(f"Erro ao buscas informações de clientes, error:{error}")
        raise error


def pl_group_insert_new_group(
    md_quota_insert_cursor,
    group_code,
    group_deadline,
    administrator_id,
    group_closing_date,
):
    try:
        logger.info("Inserindo dados na pl_group")
        query_group_insert = """
            INSERT INTO md_cota.pl_group
            (group_code, group_deadline, administrator_id, group_closing_date,
             created_at,modified_at, created_by, modified_by, is_deleted)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);

            """
        params = (
            group_code,
            group_deadline,
            administrator_id,
            group_closing_date,
            get_default_datetime(),
            get_default_datetime(),
            GLUE_DEFAULT_CODE,
            GLUE_DEFAULT_CODE,
            False,
        )
        md_quota_insert_cursor.execute(query_group_insert, params)
        query_result_client = md_quota_insert_cursor.fetchall()
        logger.info("Dados Inserindos na pl_group")
        return get_table_dict(md_quota_insert_cursor, query_result_client)[0]
    except Exception as error:
        logger.error(f"Erro ao buscas informações de grupos, error:{error}")
        raise error


def pl_quota_insert_new_quota_from_quotas(
    md_quota_insert_cursor,
    quota_code_final,
    external_reference,
    row,
    is_contemplated,
    multiple_owner,
    status_type,
    id_adm,
    group_id_md_quota,
    quota_person_type_id,
):
    try:
        logger.info("Inserindo dados de quota na pl_quota")

        new_quota = {
            "quota_code": quota_code_final,
            "external_reference": external_reference,
            "total_installments": row["nr_prazo_cota"],
            "is_contemplated": is_contemplated,
            "contemplation_date": row["dt_contempla"],
            "is_multiple_ownership": multiple_owner,
            "administrator_fee": row["vl_taxa_adm"],
            "fund_reservation_fee": row["vl_tx_fundo_reserva"],
            "info_date": row["data_info"],
            "quota_status_type_id": status_type,
            "administrator_id": id_adm,
            "group_id": group_id_md_quota,
            "quota_origin_id": Constants.QUOTA_ORIGIN_ADM.value,
            "contract_number": external_reference,
            "quota_number": row["cd_cota"],
            "check_digit": row["cd_digito"],
            "quota_person_type_id": quota_person_type_id,
            "cancel_date": row["dt_cancel_cota"],
            "created_at": get_default_datetime(),
            "modified_at": get_default_datetime(),
            "created_by": GLUE_DEFAULT_CODE,
            "modified_by": GLUE_DEFAULT_CODE,
            "is_deleted": False,
        }

        query_insert_new_quota = """
            INSERT INTO md_cota.pl_quota (
                quota_code, external_reference, total_installments, is_contemplated, contemplation_date,
                is_multiple_ownership, administrator_fee, fund_reservation_fee, info_date,
                quota_status_type_id, administrator_id, group_id, quota_origin_id,
                quota_number, check_digit, contract_number, cancel_date,
                created_at, modified_at, created_by, modified_by, is_deleted
            )
            VALUES (
                %(quota_code)s, %(external_reference)s, %(total_installments)s, %(is_contemplated)s,
                %(contemplation_date)s,%(is_multiple_ownership)s, %(administrator_fee)s,
                %(fund_reservation_fee)s, %(info_date)s,
                %(quota_status_type_id)s, %(administrator_id)s, %(group_id)s, %(quota_origin_id)s,
                %(quota_number)s, %(check_digit)s, %(contract_number)s, %(cancel_date)s,
                %(created_at)s, %(modified_at)s, %(created_by)s, %(modified_by)s, %(is_deleted)s
            )
            RETURNING *;
        """

        md_quota_insert_cursor.execute(query_insert_new_quota, new_quota)
        query_result = md_quota_insert_cursor.fetchall()
        logger.info("Dados inseridos na pl_quota")
        return get_table_dict(md_quota_insert_cursor, query_result)[0]
    except Exception as error:
        logger.error(f"Erro ao inserir informações na pl_quota, error: {error}")
        raise error


def pl_volks_add_data_rep_insert_new_data(md_quota_insert_cursor, new_quota):
    try:
        logger.info("Inserindo dados na pl_volks_additional_data")
        query_quota_insert = """
            INSERT INTO md_cota.pl_volks_additional_data (
                quota_id, good_object, fabricator, installments_remaining,
                life_insurance_percentage, qb_insurance_percentage, brand,
                total_participants, valid_from, created_at, modified_at, 
                created_by, modified_by, is_deleted
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING *;
        """
        params = (
            new_quota["quota_id"],
            new_quota["good_object"],
            new_quota["fabricator"],
            new_quota["installments_remaining"],
            new_quota["life_insurance_percentage"],
            new_quota["qb_insurance_percentage"],
            new_quota["brand"],
            new_quota["total_participants"],
            new_quota["valid_from"],
            get_default_datetime(),
            get_default_datetime(),
            GLUE_DEFAULT_CODE,
            GLUE_DEFAULT_CODE,
            False,
        )
        md_quota_insert_cursor.execute(query_quota_insert, params)
        logger.info("Dados inseridos na pl_volks_additional_data")
    except Exception as error:
        logger.error(f"Erro ao inserir informações na pl_volks, error:{error}")
        raise error


def pl_quota_status_insert_quota_status(md_quota_insert_cursor, quota_status_insert):
    try:
        logger.info("Inserindo dado na pl_quota_status.")
        query_quota_status_insert = """
            INSERT INTO md_cota.pl_quota_status (
                quota_id, quota_status_type_id, valid_from,
                created_at, modified_at, created_by, modified_by, is_deleted
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING *;
        """
        params = (
            quota_status_insert["quota_id"],
            quota_status_insert["quota_status_type_id"],
            get_default_datetime(),
            get_default_datetime(),
            get_default_datetime(),
            GLUE_DEFAULT_CODE,
            GLUE_DEFAULT_CODE,
            False,
        )
        md_quota_insert_cursor.execute(query_quota_status_insert, params)
        logger.info("Dados inseridos na pl_quota_status")
    except Exception as error:
        logger.error(f"Erro ao inserir quota_status, error:{error}")
        raise error


def pl_quota_history_detail_insert_new_quota_history(
    md_quota_insert_cursor, quota_history
):
    try:
        logger.info("Inserindo dados na pl_quota_history_detail ")
        query_quota_history_detail_insert = """
            INSERT INTO md_cota.pl_quota_history_detail (
                quota_history_detail_id, quota_id, old_quota_number, old_digit,
                quota_plan, installments_paid_number, overdue_installments_number,
                overdue_percentage, per_amount_paid, per_mutual_fund_paid,
                per_reserve_fund_paid, per_adm_paid, per_subscription_paid,
                per_mutual_fund_to_pay, per_reserve_fund_to_pay, per_adm_to_pay,
                per_subscription_to_pay, per_insurance_to_pay,
                per_install_diff_to_pay, per_total_amount_to_pay,
                amnt_mutual_fund_to_pay, amnt_reserve_fund_to_pay, amnt_adm_to_pay,
                amnt_subscription_to_pay, amnt_insurance_to_pay, amnt_fine_to_pay,
                amnt_interest_to_pay, amnt_others_to_pay, amnt_install_diff_to_pay,
                amnt_to_pay, quitter_assembly_number, cancelled_assembly_number,
                adjustment_date, current_assembly_date, current_assembly_number,
                asset_adm_code, asset_description, asset_value, asset_type_id,
                info_date, valid_from, valid_to
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s
            )
            RETURNING *;
        """
        params = (
            quota_history.get("quota_history_detail_id"),
            quota_history.get("quota_id"),
            quota_history.get("old_quota_number"),
            quota_history.get("old_digit"),
            quota_history.get("quota_plan"),
            quota_history.get("installments_paid_number"),
            quota_history.get("overdue_installments_number"),
            quota_history.get("overdue_percentage"),
            quota_history.get("per_amount_paid"),
            quota_history.get("per_mutual_fund_paid"),
            quota_history.get("per_reserve_fund_paid"),
            quota_history.get("per_adm_paid"),
            quota_history.get("per_subscription_paid"),
            quota_history.get("per_mutual_fund_to_pay"),
            quota_history.get("per_reserve_fund_to_pay"),
            quota_history.get("per_adm_to_pay"),
            quota_history.get("per_subscription_to_pay"),
            quota_history.get("per_insurance_to_pay"),
            quota_history.get("per_install_diff_to_pay"),
            quota_history.get("per_total_amount_to_pay"),
            quota_history.get("amnt_mutual_fund_to_pay"),
            quota_history.get("amnt_reserve_fund_to_pay"),
            quota_history.get("amnt_adm_to_pay"),
            quota_history.get("amnt_subscription_to_pay"),
            quota_history.get("amnt_insurance_to_pay"),
            quota_history.get("amnt_fine_to_pay"),
            quota_history.get("amnt_interest_to_pay"),
            quota_history.get("amnt_others_to_pay"),
            quota_history.get("amnt_install_diff_to_pay"),
            quota_history.get("amnt_to_pay"),
            quota_history.get("quitter_assembly_number"),
            quota_history.get("cancelled_assembly_number"),
            quota_history.get("adjustment_date"),
            quota_history.get("current_assembly_date"),
            quota_history.get("current_assembly_number"),
            quota_history.get("asset_adm_code"),
            quota_history.get("asset_description"),
            quota_history.get("asset_value"),
            quota_history.get("asset_type_id"),
            quota_history.get("info_date"),
            quota_history.get("valid_from"),
            quota_history.get("valid_to"),
        )
        md_quota_insert_cursor.execute(query_quota_history_detail_insert, params)
        logger.info("Dados inseridos na pl_quota_history_detail")
    except Exception as error:
        logger.error(f"Erro ao inserir na quota_history, error:{error}")
        raise error


def insert_quota_field_update_date(
    md_quota_insert_cursor, quota_field_update_date_to_insert
):
    try:
        logger.info("Inserindo dados na pl_quota_field_update_date")
        query_quota_field_update_date_insert = """
            INSERT INTO md_cota.pl_quota_field_update_date (
                update_date, quota_history_field_id, data_source_id, quota_id,
                created_at, modified_at, created_by, modified_by, is_deleted
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING *;
        """
        params = (
            quota_field_update_date_to_insert.get("update_date"),
            quota_field_update_date_to_insert.get("quota_history_field_id"),
            quota_field_update_date_to_insert.get("data_source_id"),
            quota_field_update_date_to_insert.get("quota_id"),
            get_default_datetime(),
            get_default_datetime(),
            GLUE_DEFAULT_CODE,
            GLUE_DEFAULT_CODE,
            False,
        )
        md_quota_insert_cursor.execute(query_quota_field_update_date_insert, params)
        logger.info("Dados inseridos na pl_quota_field_update_date")
    except Exception as error:
        logger.error(f"Erro ao inserir quota_field_update_date, error:{error}")
        raise error


def update_is_processed(md_quota_insert_cursor, id_cliente_volks):
    try:
        logger.info("Fazendo update na tb_clientes_volks_pre")
        query_update_clients_processed = """
            UPDATE stage_raw.tb_clientes_volks_pre
            SET is_processed = %s
            WHERE id_cliente_vilks = %s
            RETURNING *;
        """
        params = (True, id_cliente_volks)
        md_quota_insert_cursor.execute(query_update_clients_processed, params)
        logger.info("Update na tb_clientes_volks_pre")
    except Exception as error:
        logger.error(f"Erro ao fazer update na tb_clientes_volks_pre, error:{error}")
        raise error


def update_is_processed_stage_raw(md_quota_insert_cursor, id_quotas_volks):
    try:
        logger.info("Fazendo update na tb_quotas_volks_pre")
        query_update_quotas_processed = """
            UPDATE stage_raw.tb_quotas_volks_pre
            SET is_processed = %s
            WHERE id_quotas_volks = %s
            RETURNING *;
        """
        params = (True, id_quotas_volks)
        md_quota_insert_cursor.execute(query_update_quotas_processed, params)
        logger.info("Update efetuado na tb_quotas_volks_pre")
    except Exception as error:
        logger.error(f"Erro ao fazer update na tb_quotas_volks_pre, error:{error}")
        raise error


def search_additional_data(md_quota_select_cursor, quota_id: int):
    try:
        logger.info("Recuperando dados adicionais da pl_volks_additional_data")
        query_additional_data = f"""
            SELECT *
            FROM md_cota.pl_volks_additional_data
            WHERE quota_id = {quota_id} AND valid_to is NULL AND is_deleted is FALSE;
            """
        md_quota_select_cursor.execute(query_additional_data)
        query_result = md_quota_select_cursor.fetchall()
        logger.info("Dados recuperados da pl_volks_additional_data")
        return get_table_dict(md_quota_select_cursor, query_result)[0]
    except Exception as error:
        logger.error(
            f"Erro ao buscas informações adicionais na pl_volks, error:{error}"
        )
        raise error


def update_valid_to(md_quota_update_cursor, quota_id):
    try:
        logger.info("Fazendo update na pl_volks_additional_data")
        query_update_valid_to = """
            UPDATE md_cota.pl_volks_additional_data
            SET valid_to = %s,
                modified_at = %s,
                modified_by = %s
            WHERE quota_id = %s AND valid_to IS NULL AND is_deleted = FALSE;
            """
        md_quota_update_cursor.execute(
            query_update_valid_to,
            (
                get_default_datetime(),
                get_default_datetime(),
                GLUE_DEFAULT_CODE,
                quota_id,
            ),
        )
        logger.info("Update efetudado na pl_volks_additional_data")
    except Exception as error:
        logger.error(f"Erro ao atualizaar informações na pl_volks_add, error:{error}")
        raise error


def update_quota_referenceId(md_quota_update_cursor, quota_to_update):
    try:
        logger.info("Fazendo update na pl_quota")
        query_update_quota_referenceId = """
            UPDATE md_cota.pl_quota
            SET
                quota_status_type_id = %s,
                external_reference = %s,
                quota_number = %s,
                contract_number = %s,
                is_contemplated = %s,
                contemplation_date = %s,
                is_multiple_ownership = %s,
                cancel_date = %s,
                check_digit = %s,
                quota_person_type_id = %s,
                modified_at = %s,
                modified_by = %s
            WHERE
                quota_id = %s AND is_deleted = FALSE;
        """
        md_quota_update_cursor.execute(
            query_update_quota_referenceId,
            (
                quota_to_update["quota_status_type_id"],
                quota_to_update["external_reference"],
                quota_to_update["quota_number"],
                quota_to_update["contract_number"],
                quota_to_update["is_contemplated"],
                quota_to_update["contemplation_date"],
                quota_to_update["is_multiple_ownership"],
                quota_to_update["cancel_date"],
                quota_to_update["check_digit"],
                quota_to_update["quota_person_type_id"],
                get_default_datetime(),
                GLUE_DEFAULT_CODE,
                quota_to_update["quota_id"],
            ),
        )
    except Exception as error:
        logger.error(f"Erro ao atualizar informações na PlQuota, error: {error}")
        raise error


def update_quota_status(md_quota_update_cursor, quota_status_to_update):
    try:
        logger.info("Fazendo uptade na pl_quotas_status")
        query_update_quota_status = """
            UPDATE md_cota.pl_quota_status
            SET
                valid_to = %s,
                modified_at = %s,
                modified_by = %s
            WHERE
                quota_id = %s AND valid_to IS NULL AND is_deleted = FALSE;
        """
        md_quota_update_cursor.execute(
            query_update_quota_status,
            (
                get_default_datetime(),
                get_default_datetime(),
                GLUE_DEFAULT_CODE,
                quota_status_to_update["quota_id"],
            ),
        )
    except Exception as error:
        logger.error(f"Erro ao atualizar informações na PlQuotaStatus, error: {error}")
        raise error


def insert_quota_status(md_quota_insert_cursor, quota_status_insert):
    try:
        logger.info("Inserindo dados na pl_quotas_status")
        query_insert_quota_status = """
            INSERT INTO md_cota.pl_quota_status
            (
                quota_id,
                quota_status_type_id,
                valid_from,
                created_at,
                modified_at,
                created_by,
                modified_by,
                is_deleted
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s, %s, %s
            );
        """
        md_quota_insert_cursor.execute(
            query_insert_quota_status,
            (
                quota_status_insert["quota_id"],
                quota_status_insert["quota_status_type_id"],
                get_default_datetime(),
                get_default_datetime(),
                get_default_datetime(),
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
                False,
            ),
        )
    except Exception as error:
        logger.error(f"Erro ao inserir informações na PlQuotaStatus, error: {error}")
        raise error


def get_quota_history_detail(md_quota_select_cursor, quota_id):
    try:
        logger.info("Obetndo dados da pl_quota_history_detail")
        query_get_quota_history_detail = f"""
            SELECT *
            FROM md_cota.pl_quota_history_detail
            WHERE quota_id = {quota_id} AND valid_to IS NULL AND is_deleted is FALSE;
        """
        md_quota_select_cursor.execute(query_get_quota_history_detail)
        query_result = md_quota_select_cursor.fetchall()
        logger.info("Obtido dados da pl_quota_history_detail")
        return get_table_dict(md_quota_select_cursor, query_result)[0]
    except Exception as error:
        logger.error(
            f"Erro ao obter informações na PlQuotaHistoryDetail, error: {error}"
        )
        raise error


def update_quota_history_detail(md_quota_update_cursor, quota_id):
    try:
        logger.info("Fazendo update na tabela pl_quota_history_detail")
        query_update_quota_history_detail = """
            UPDATE md_cota.pl_quota_history_detail
            SET
                valid_to = %s,
                modified_at = %s,
                modified_by = %s
            WHERE
                quota_id = %s AND valid_to IS NULL AND is_deleted = FALSE;
        """
        md_quota_update_cursor.execute(
            query_update_quota_history_detail,
            (
                get_default_datetime(),
                get_default_datetime(),
                GLUE_DEFAULT_CODE,
                quota_id,
            ),
        )
        logger.info("Update na tabela pl_quota_history_detail")
    except Exception as error:
        logger.error(
            f"Erro ao atualizar informações na PlQuotaHistoryDetail, error: {error}"
        )
        raise error


def update_quota_field_update_date(
    md_quota_update_cursor, quota_field_update_date_to_update
):
    try:
        logger.info("Fazendo update na pl_quota_field_update_date")
        query_update_quota_field_update_date = """
            UPDATE md_cota.pl_quota_field_update_date
            SET
                update_date = %s,
                data_source_id = %s,
                modified_at = %s,
                modified_by = %s
            WHERE
                quota_id = %s
                AND quota_history_field_id = %s
                AND is_deleted = FALSE;
        """
        md_quota_update_cursor.execute(
            query_update_quota_field_update_date,
            (
                quota_field_update_date_to_update["update_date"],
                quota_field_update_date_to_update["data_source_id"],
                get_default_datetime(),
                GLUE_DEFAULT_CODE,
                quota_field_update_date_to_update["quota_id"],
                quota_field_update_date_to_update["quota_history_field_id"],
            ),
        )
        logger.info("Update na tabela pl_quota_field_update_date efetuado")
    except Exception as error:
        logger.error(
            f"Erro ao atualizar informações na PlQuotaFieldUpdateDate, error: {error}"
        )
        raise error


def make_requests(quota_id_md_quota, ownership_percentage, cpf_cnpj, customers):
    try:
        payload_lambda = {
            "quota_id": quota_id_md_quota,
            "ownership_percentage": ownership_percentage,
            "main_owner": cpf_cnpj,
            "cubees_request": customers,
        }
        request_lambda = json.dumps(payload_lambda)
        lambda_client = boto3.client("lambda")
        lambda_client.invoke(
            FunctionName="md-cota-cubees-customer-sandbox",
            InvocationType="Event",
            Payload=request_lambda,
        )
    except ClientError as error:
        logger.error(f"Erro ao fazer o invoke da lambda: {str(error)}")
        raise error


def field_quota_dict():
    fields_inserted_quota_history = [
        "per_mutual_fund_paid",
        "quota_plan",
        "old_digit",
        "asset_adm_code",
        "asset_value",
        "asset_type_id",
        "old_quota_number",
        "asset_description",
        "current_assembly_number",
    ]
    return fields_inserted_quota_history


def insert_additional_data_dict(quota_id_md_quota, row, md_quota_insert_cursor):
    additional_data = {
        "quota_id": quota_id_md_quota,
        "good_object": row["bem_objeto"],
        "fabricator": row["fabricacao"],
        "installments_remaining": row["pcl_a_pagar"],
        "life_insurance_percentage": row["pe_seguro_vida"],
        "qb_insurance_percentage": row["pe_seg_quebra_garantia"],
        "brand": row["marca"],
        "total_participants": row["qt_participantes"],
        "valid_from": row["data_info"],
    }
    pl_volks_add_data_rep_insert_new_data(md_quota_insert_cursor, additional_data)


def update_quota_field_data(
    fields_updated_quota_history,
    switch_quota_history_field,
    row,
    data_source_id,
    quota_id_md_quota,
    md_quota_update_cursor,
):
    for field in fields_updated_quota_history:
        history_field_id = switch_quota_history_field.get(
            field,
            Constants.CASE_DEFAULT_HISTORY_DETAIL_FIELD.value,
        )

        quota_field_update_date_to_update = {
            "update_date": row["data_info"],
            "quota_history_field_id": history_field_id,
            "data_source_id": data_source_id,
            "quota_id": quota_id_md_quota,
        }

        update_quota_field_update_date(
            md_quota_update_cursor, quota_field_update_date_to_update
        )


def func_quota_history_detail(
    quota_history_detail_to_insert,
    quota_id_md_quota,
    row,
    asset_type,
    total_assembly,
    md_quota_insert_cursor,
):
    quota_history_detail_to_insert["quota_id"] = quota_id_md_quota
    quota_history_detail_to_insert["quota_plan"] = row["nr_plano"]
    quota_history_detail_to_insert["per_mutual_fund_paid"] = row["vl_perc_pago"]
    quota_history_detail_to_insert["overdue_percentage"] = row["vl_percatr"]
    quota_history_detail_to_insert["old_quota_number"] = row["cd_cota"]
    quota_history_detail_to_insert["old_digit"] = row["cd_digito"]
    quota_history_detail_to_insert["asset_value"] = row["vl_bem_basico_atu"]
    quota_history_detail_to_insert["asset_adm_code"] = row["cd_bem_basico"]
    quota_history_detail_to_insert["asset_description"] = row["ds_bem_basico"]
    quota_history_detail_to_insert["asset_type_id"] = asset_type
    quota_history_detail_to_insert["current_assembly_number"] = total_assembly
    quota_history_detail_to_insert["info_date"] = row["data_info"]
    quota_history_detail_to_insert["valid_from"] = get_default_datetime()
    quota_history_detail_to_insert["valid_to"] = None

    pl_quota_history_detail_insert_new_quota_history(
        md_quota_insert_cursor, quota_history_detail_to_insert
    )


def create_payload_company(
    person_ext_code, person_type, customer, contacts, document_type
):
    payload = {
        "person_ext_code": person_ext_code,
        "person_type": person_type,
        "administrator_code": "0000000289",
        "channel_type": "EMAIL",
        "legal_person": {
            "company_name": customer["nm_pessoa"],
            "company_fantasy_name": customer["nm_pessoa"],
            "founding_date": None,
        },
        "contacts": contacts,
        "documents": [
            {
                "document_number": person_ext_code,
                "expiring_date": "2040-12-01",
                "person_document_type": document_type,
            }
        ],
        "reactive": False,
    }
    return payload


def created_payload_person(
    person_ext_code, person_type, customer, contacts, document_type
):
    payload = {
        "person_ext_code": person_ext_code,
        "person_type": person_type,
        "administrator_code": "0000000289",
        "channel_type": "EMAIL",
        "natural_person": {
            "full_name": customer["nm_pessoa"],
            "birthdate": None,
        },
        "contacts": contacts,
        "documents": [
            {
                "document_number": person_ext_code,
                "expiring_date": "2040-12-01",
                "person_document_type": document_type,
            }
        ],
        "reactive": False,
    }
    return payload


def md_quota_is_none(
    md_quota_connection,
    md_quota_group,
    md_quota_insert_cursor,
    code_group,
    row,
    id_adm,
    groups_md_quota,
    quota_customer,
    external_reference,
    quotas_md_quota,
    total_assembly,
    data_source_id,
    md_quota_update_cursor,
):
    logger.info("cota ainda não existe no md-cota")
    quota_code_final = build_quota_code(md_quota_connection)
    if md_quota_group is None:
        group_md_quota = pl_group_insert_new_group(
            md_quota_insert_cursor,
            code_group,
            row["nr_prazo_grupo"],
            id_adm,
            row["dt_ult_assembleia"],
        )

        group_id_md_quota = group_md_quota["group_id"]
        groups_md_quota.append(group_md_quota)
    else:
        group_id_md_quota = md_quota_group["group_id"]

    status_type = switch_status_dict("C", Constants.CASE_DEFAULT_TYPES.value)
    is_contemplated = False
    multiple_owner = True if quota_customer else False
    ownership_percentage = 1 / len(quota_customer) if quota_customer else 1
    quota_person_type_id = 1 if row["tp_pes_pes"] == "FÍSICA" else 2

    quota_md_quota = pl_quota_insert_new_quota_from_quotas(
        md_quota_insert_cursor,
        quota_code_final,
        external_reference,
        row,
        is_contemplated,
        multiple_owner,
        status_type,
        id_adm,
        group_id_md_quota,
        quota_person_type_id,
    )

    quota_id_md_quota = quota_md_quota["quota_id"]
    quotas_md_quota.append(quota_md_quota)

    insert_additional_data_dict(quota_id_md_quota, row, md_quota_insert_cursor)
    quota_status_to_insert = {
        "quota_id": quota_id_md_quota,
        "quota_status_type_id": status_type,
    }
    pl_quota_status_insert_quota_status(md_quota_insert_cursor, quota_status_to_insert)
    asset_type = switch_asset_type_dict(
        "VEICULOS LEVES",
        Constants.CASE_DEFAULT_ASSET_TYPES.value,
    )
    quota_history_detail_to_insert = {}
    switch_quota_history_field = switch_quota_history_detail_dict()
    for keyword in switch_quota_history_field:
        quota_history_detail_to_insert[keyword] = None

    func_quota_history_detail(
        quota_history_detail_to_insert,
        quota_id_md_quota,
        row,
        asset_type,
        total_assembly,
        md_quota_insert_cursor,
    )
    fields_inserted_quota_history = field_quota_dict()

    for field in fields_inserted_quota_history:
        history_field_id = switch_quota_history_field.get(
            field, Constants.CASE_DEFAULT_HISTORY_DETAIL_FIELD.value
        )
        quota_field_update_date_insert = {
            "update_date": row["data_info"],
            "quota_history_field_id": history_field_id,
            "data_source_id": data_source_id,
            "quota_id": quota_id_md_quota,
        }
        insert_quota_field_update_date(
            md_quota_insert_cursor, quota_field_update_date_insert
        )
    customers = []
    cpf_cnpj = ""
    for customer in quota_customer:
        cpf_cnpj = (
            customer["cd_cpf_cnpj"].replace(".", "").replace("-", "").replace("/", "")
        )
        if row["tp_pes_pes"] == "FÍSICA":
            person_ext_code = cpf_justified(cpf_cnpj)
            person_type = "NATURAL"
            contacts = []

            add_contact_if_not_empty(
                customer, contacts, "ds_endereco_eml_p", "EMAIL PESSOAL", "EMAIL", True
            )
            add_contact_if_not_empty(
                customer, contacts, "ds_numero_tel_cel", "MOBILE", "PHONE", False
            )
            payload = created_payload_person(
                person_ext_code, person_type, customer, contacts, "CPF"
            )
        else:
            person_ext_code = cnpj_justified(cpf_cnpj)
            person_type = "LEGAL"
            contacts = []
            add_contact_if_not_empty(
                customer, contacts, "ds_endereco_eml_p", "EMAIL PESSOAL", "EMAIL", True
            )
            add_contact_if_not_empty(
                customer, contacts, "ds_numero_tel_cel", "MOBILE", "PHONE", False
            )

            payload = create_payload_company(
                person_ext_code, person_type, customer, contacts, "CS"
            )
        customers.append(json.dumps(payload))
        update_is_processed(md_quota_update_cursor, customer["id_cliente_vilks"])
    make_requests(quota_id_md_quota, ownership_percentage, cpf_cnpj, customers)
    update_is_processed(md_quota_insert_cursor, row["id_quotas_volks"])


def md_quota_not_none(
    md_quota_quota,
    quota_customer,
    row,
    md_quota_insert_cursor,
    md_quota_select_cursor,
    md_quota_update_cursor,
    external_reference,
    total_assembly,
    data_source_id,
):
    logger.info("cota já existe no md-cota")
    quota_id_md_quota = md_quota_quota["quota_id"]
    status_type = switch_status_dict("C", Constants.CASE_DEFAULT_TYPES.value)
    ownership_percentage = 1
    if len(quota_customer) > 1:
        ownership_percentage = 1 / len(quota_customer)
        multiple_owner = True
    else:
        multiple_owner = False
    customers = []
    cpf_cnpj = 0
    quota_person_type_id = 0
    for customer in quota_customer:
        cpf_cnpj = customer["cd_cpf_cnpj"]
        if row["tp_pes_pes"] == "FÍSICA":
            person_ext_code = cpf_justified(cpf_cnpj)
            quota_person_type_id = 1
            person_type = "NATURAL"
            contacts = []
            add_contact_if_not_empty(
                customer, contacts, "ds_endereco_eml_p", "EMAIL PESSOAL", "EMAIL", True
            )
            add_contact_if_not_empty(
                customer,
                contacts,
                "ds_numero_tel_cel",
                "TELEFONE CELULAR",
                "MOBILE",
                False,
            )
            payload = created_payload_person(
                person_ext_code, person_type, customer, contacts, "CPF"
            )
        else:
            person_ext_code = cnpj_justified(cpf_cnpj)
            quota_person_type_id = 2
            person_type = "LEGAL"
            contacts = []
            add_contact_if_not_empty(
                customer, contacts, "ds_endereco_eml_p", "EMAIL PESSOAL", "EMAIL", True
            )
            contacts[-1]["contact_type"] = "MOBILE"
            add_contact_if_not_empty(
                customer,
                contacts,
                "ds_numero_tel_cel",
                "TELEFONE CELULAR",
                "MOBILE",
                False,
            )
            contacts[-1]["contact_type"] = "MOBILE"
            payload = create_payload_company(
                person_ext_code, person_type, customer, contacts, "CS"
            )
        customers.append(json.dumps(payload))
        update_is_processed(md_quota_insert_cursor, customer["id_cliente_vilks"])

    make_requests(quota_id_md_quota, ownership_percentage, cpf_cnpj, customers)

    is_contemplated = False

    md_quota_additional_data = search_additional_data(
        md_quota_select_cursor, quota_id_md_quota
    )
    if (
        md_quota_additional_data is not None
        and md_quota_additional_data["valid_from"] < row["data_info"]
    ):
        update_valid_to(md_quota_update_cursor, quota_id_md_quota)
        insert_additional_data_dict(quota_id_md_quota, row, md_quota_insert_cursor)
    else:
        insert_additional_data_dict(quota_id_md_quota, row, md_quota_insert_cursor)

    if row["data_info"] > md_quota_quota["info_date"]:
        quota_update = {
            "quota_id": quota_id_md_quota,
            "quota_status_type_id": status_type,
            "contract_number": external_reference,
            "external_reference": external_reference,
            "quota_number": row["cd_cota"],
            "is_contemplated": is_contemplated,
            "contemplation_date": row["dt_contempla"],
            "is_multiple_ownership": multiple_owner,
            "info_date": row["data_info"],
            "check_digit": row["cd_digito"],
            "quota_person_type_id": quota_person_type_id,
            "cancel_date": row["dt_cancel_cota"],
        }
        logger.info(f"cota to update: {quota_update}")

        update_quota_referenceId(md_quota_update_cursor, quota_update)

    if md_quota_quota["quota_status_type_id"] != status_type:
        quota_status_to_update = {"quota_id": quota_id_md_quota}
        update_quota_status(md_quota_update_cursor, quota_status_to_update)
        quota_status_to_insert = {
            "quota_id": quota_id_md_quota,
            "quota_status_type_id": status_type,
        }
        insert_quota_status(md_quota_insert_cursor, quota_status_to_insert)
    quota_history_detail_md_quota = get_quota_history_detail(
        md_quota_select_cursor, quota_id_md_quota
    )

    if quota_history_detail_md_quota["info_date"] < row["data_info"]:
        update_quota_history_detail(md_quota_update_cursor, quota_id_md_quota)

        asset_type = switch_asset_type_dict(
            "VEICULOS LEVES",
            Constants.CASE_DEFAULT_ASSET_TYPES.value,
        )

        quota_history_detail_to_insert = {}
        switch_quota_history_field = switch_quota_history_detail_dict()
        for keyword in switch_quota_history_field:
            logger.info(keyword)
            logger.info(quota_history_detail_md_quota)
            quota_history_detail_to_insert[keyword] = quota_history_detail_md_quota[
                keyword
            ]

        func_quota_history_detail(
            quota_history_detail_to_insert,
            quota_id_md_quota,
            row,
            asset_type,
            total_assembly,
            md_quota_insert_cursor,
        )
        fields_updated_quota_history = field_quota_dict()
        switch_quota_history_field = switch_quota_history_detail_dict()
        update_quota_field_data(
            fields_updated_quota_history,
            switch_quota_history_field,
            row,
            data_source_id,
            quota_id_md_quota,
            md_quota_update_cursor,
        )


def create_contact(data_type, contact_desc, contact, contact_type, preferred_contact):
    if data_type is not None:
        return {
            "contact_desc": contact_desc,
            "contact": contact,
            "contact_type": contact_type,
            "preferred_contact": preferred_contact,
        }


def add_contact_if_not_empty(
    customer, contacts, data_key, contact_desc, contact_type, preferred_contact
):
    if customer[data_key] is not None and customer[data_key] != "":
        contact = create_contact(
            customer[data_key],
            contact_desc,
            customer[data_key],
            contact_type,
            preferred_contact,
        )
        contacts.append(contact)


def process_row(
    row,
    groups_md_quota,
    quotas_md_quota,
    md_quota_select_cursor,
    id_adm,
    md_quota_insert_cursor,
    data_source_id,
    md_quota_update_cursor,
    md_quota_connection,
):
    external_reference = row["contrato"]
    code_group = (
        row["cd_grupo"][-5:]
        if len(row["cd_grupo"]) > 5
        else string_right_justified(row["cd_grupo"])
    )
    md_quota_group = get_dict_by_id(code_group, groups_md_quota, "group_code")
    md_quota_quota = get_dict_by_id(
        external_reference, quotas_md_quota, "external_reference"
    )
    quota_customer = read_tb_client_volks_pre(
        row["cd_grupo"], row["cd_cota"], row["cd_digito"], md_quota_select_cursor
    )

    info_date = row["data_info"]
    today = datetime.today()

    assembly_since_statement = relativedelta.relativedelta(today, info_date).months
    total_assembly = row["nr_assembleia_vigente"] + assembly_since_statement
    if md_quota_quota is None:
        md_quota_is_none(
            md_quota_connection,
            md_quota_group,
            md_quota_insert_cursor,
            code_group,
            row,
            id_adm,
            groups_md_quota,
            quota_customer,
            external_reference,
            quotas_md_quota,
            total_assembly,
            data_source_id,
            md_quota_update_cursor,
        )

    else:
        md_quota_not_none(
            md_quota_quota,
            quota_customer,
            row,
            md_quota_insert_cursor,
            md_quota_select_cursor,
            md_quota_update_cursor,
            external_reference,
            total_assembly,
            data_source_id,
        )
    update_is_processed(md_quota_update_cursor, row["id_quotas_volks"])
    update_is_processed_stage_raw(md_quota_update_cursor, row["id_quotas_volks"])


def process_all(
    md_quota_cursor,
    groups_md_quota,
    quotas_md_quota,
    md_quota_select_cursor,
    id_adm,
    md_quota_insert_cursor,
    data_source_id,
    md_quota_update_cursor,
    md_quota_connection,
):
    batch_counter = 0
    while True:
        batch_counter += 1
        rows = md_quota_cursor.fetchmany(size=BATCH_SIZE)
        column_names = [desc[0] for desc in md_quota_cursor.description]
        if not rows:
            break
        logger.info(f"Fetch {BATCH_SIZE} rows at a time - Batch number {batch_counter}")
        for row in rows:
            row_dict = dict(zip(column_names, row))
            process_row(
                row_dict,
                groups_md_quota,
                quotas_md_quota,
                md_quota_select_cursor,
                id_adm,
                md_quota_insert_cursor,
                data_source_id,
                md_quota_update_cursor,
                md_quota_connection,
            )


def from_file_to_model():
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()
    md_quota_insert_cursor = md_quota_connection.cursor()
    md_quota_select_cursor = md_quota_connection.cursor()
    md_quota_update_cursor = md_quota_connection.cursor()
    md_quota_cursor = md_quota_connection.cursor()

    id_adm = read_adm_id(md_quota_select_cursor)
    groups_md_quota = read_groups_pl_group(id_adm, md_quota_select_cursor)
    quotas_md_quota = read_quotas_pl_quota(id_adm, md_quota_select_cursor)
    data_source_id = get_data_source_id(md_quota_select_cursor)

    try:
        read_data_stage_raw_quotas_volks(md_quota_cursor)
        process_all(
            md_quota_cursor,
            groups_md_quota,
            quotas_md_quota,
            md_quota_select_cursor,
            id_adm,
            md_quota_insert_cursor,
            data_source_id,
            md_quota_update_cursor,
            md_quota_connection,
        )
        logger.info("Dados processados com sucesso.")
        md_quota_connection.commit()
    except Exception as error:
        logger.error("Error ao processar dados")
        raise error
    finally:
        md_quota_connection.close()


if __name__ == "__main__":
    from_file_to_model()
