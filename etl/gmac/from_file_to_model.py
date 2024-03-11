import json
from datetime import datetime
from enum import Enum
import sys
from awsglue.utils import getResolvedOptions
import boto3
from bazartools.common.database.glueConnection import GlueConnection
from bazartools.common.database.quotaCodeBuilder import build_quota_code
from bazartools.common.logger import get_logger
from botocore.exceptions import ClientError
from dateutil import relativedelta

logger = get_logger()

GLUE_DEFAULT_CODE = 2


def get_default_datetime() -> datetime:
    return datetime.now()


def get_table_dict(cursor, rows):
    column_names = [desc[0] for desc in cursor.description]
    return [dict(zip(column_names, row)) for row in rows]


def get_dict_by_id(id_item: str, data_list: list, field_name: str):
    for item_list in data_list:
        if item_list[field_name] == id_item:
            return item_list
    return None


def string_right_justified(group: str) -> str:
    code_group = str(group) if len(str(group)) == 5 else str(group).rjust(5, "0")
    return code_group


def cnpj_justified(cnpj: str) -> str:
    justified = str(cnpj) if len(str(cnpj)) == 14 else str(cnpj).rjust(14, "0")
    return justified


def cpf_justified(cpf: str) -> str:
    justified = str(cpf) if len(str(cpf)) == 11 else str(cpf).rjust(11, "0")
    return justified


def asset_type_dict(key, value):
    asset = {
        "VEICULOS_PESADOS": 3,
        "VEICULOS_LEVES": 2,
        "IMOVEIS": 1,
        "MOTOCICLETAS": 4,
    }
    return asset.get(key, value)


def switch_status_dict(key, value):
    status = {
        "ATIVOS": 1,
        "DESISTENTES": 4,
        "EXCLUIDOS": 2,
        "EM ATRASO": 3,
    }
    return status.get(key, value)


class Constants(Enum):
    CASE_DEFAULT_TYPES = 5
    CASE_DEFAULT_ASSET_TYPES = 7
    CASE_DEFAULT_HISTORY_DETAIL_FIELD = 0
    QUOTA_ORIGIN_ADM = 1
    QUOTA_ORIGIN_CUSTOMER = 2


class EtlInfo(Enum):
    ADM_NAME = "GMAC ADM CONS  LTDA"
    QUOTA_CODE_PREFIX = "BZ"


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


def contact_customer(
    tell, ddd, contact_desc, contact_category, contact_type, preferred_contact
):
    if tell is not None and tell != "":
        contact = {
            "contact_desc": contact_desc,
            "contact": f"{ddd} {tell}",
            "contact_category": contact_category,
            "contact_type": contact_type,
            "preferred_contact": preferred_contact,
        }
        return contact


def requests_lambda(quota_id_md_quota, ownership_percentage, cpf_cnpj, customers, lambda_customer_cubees):
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
            FunctionName=lambda_customer_cubees,
            InvocationType="Event",
            Payload=request_lambda,
        )
    except ClientError as error:
        logger.info(
            f"Erro ao fazer invoke na lambda: {lambda_customer_cubees}"
        )
        raise error


def read_adm(md_quota_select_cursor):
    try:
        logger.info("Buscando id da adm...")
        query_select_adm = """
            SELECT administrator_id
            FROM md_cota.pl_administrator
            WHERE administrator_code = '0000000131'
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


def read_groups(id_adm, md_quota_select_cursor):
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


def quotas_all(id_adm, md_quota_select_cursor):
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


def read_data_stage_raw(md_quota_select_cursor):
    try:
        logger.info("Buscando dados no stage raw...")
        stage_raw_clientes_pre = """
            SELECT *
            FROM stage_raw.tb_quotas_gmac_pre
            WHERE is_processed is FALSE;
            """
        md_quota_select_cursor.execute(stage_raw_clientes_pre)
        logger.info("Busca no stage raw efetuado com sucesso")
        query_result_groups = md_quota_select_cursor.fetchall()
        return get_table_dict(md_quota_select_cursor, query_result_groups)
    except Exception as error:
        logger.error(f"Erro ao busca dados na tb_quotas_gmac_pre. Error:{error}")
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


def client_gmac_pre_read_data_stage_raw(md_quota_select_cursor, groups, quota):
    try:
        stage_raw_clientes_gmac_pre = f"""
        SELECT *
        FROM stage_raw.tb_clientes_gmac_pre
        WHERE is_processed is FALSE AND grupo = '{groups}' AND cota = '{quota}';
        """
        md_quota_select_cursor.execute(stage_raw_clientes_gmac_pre)
        logger.info("Busca no stage raw efetuado com sucesso")
        query_result_groups = md_quota_select_cursor.fetchall()
        return get_table_dict(md_quota_select_cursor, query_result_groups)

    except Exception as error:
        logger.error(f"Erro ao busca dados na tb_clientes_gmac_pre. Error:{error}")
        raise error


def insert_new_group(md_quota_insert_cursor, group_data: dict):
    try:
        group_pl_group = """
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
            RETURNING *
        """
        md_quota_insert_cursor.execute(
            group_pl_group,
            (
                group_data["group_code"],
                group_data["group_deadline"],
                group_data["administrator_id"],
                group_data["group_closing_date"],
                get_default_datetime(),
                get_default_datetime(),
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )
        logger.info("Insert na pl_group feita com sucesso")
        query_result_groups = md_quota_insert_cursor.fetchall()
        return get_table_dict(md_quota_insert_cursor, query_result_groups)[0]
    except Exception as error:
        logger.error(f"Erro ao inserir dados na pl_group. Error:{error}")
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
            "total_installments": row["pz_cota"],
            "is_contemplated": is_contemplated,
            "contemplation_date": None,
            "cancel_date": None,
            "is_multiple_ownership": multiple_owner,
            "administrator_fee": row["tx_adm"],
            "fund_reservation_fee": row["tx_fr"],
            "info_date": row["data_info"],
            "quota_status_type_id": status_type,
            "check_digit": row["versao"],
            "administrator_id": id_adm,
            "group_id": group_id_md_quota,
            "quota_origin_id": Constants.QUOTA_ORIGIN_ADM.value,
            "contract_number": external_reference,
            "quota_number": row["cota"],
            "quota_person_type_id": quota_person_type_id,
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


def read_data_stage_raw_clients(md_quota_cursor, group, quota):
    try:
        logger.info("Buscando dados no stage raw...")
        stage_raw_clientes_pre = f"""
            SELECT *
            FROM stage_raw.tb_clientes_gmac_pre
            WHERE is_processed is FALSE AND grupo= '{group}' AND cota='{quota}';
            """
        md_quota_cursor.execute(stage_raw_clientes_pre)
        logger.info("Busca no stage raw efetuado com sucesso")
    except Exception as error:
        logger.error(f"Erro ao busca dados na tb_clientes_gmac_pre. Error:{error}")
        raise error


def pl_quota_history_detail_insert_new_quota_history(
    md_quota_insert_cursor, quota_history
):
    try:
        logger.info("Inserindo dados na pl_quota_history_detail ")
        query_quota_history_detail_insert = """
            INSERT INTO md_cota.pl_quota_history_detail (
                quota_id, old_quota_number, old_digit,
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
                info_date, valid_from, valid_to, created_at, modified_at, created_by, modified_by
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
            RETURNING *;
        """
        params = (
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
            "now()",
            "now()",
            GLUE_DEFAULT_CODE,
            GLUE_DEFAULT_CODE,
        )
        md_quota_insert_cursor.execute(query_quota_history_detail_insert, params)
        logger.info("Dados inseridos na pl_quota_history_detail")
    except Exception as error:
        logger.error(f"Erro ao inserir na quota_history, error:{error}")
        raise error


def update_is_processed_client(md_quota_insert_cursor, id_cliente_gmac):
    try:
        logger.info("Fazendo update na tb_clientes_gmac_pre")
        query_update_clients_processed = """
            UPDATE stage_raw.tb_clientes_gmac_pre
            SET is_processed = %s
            WHERE id_cliente_gmac = %s
            RETURNING *;
        """
        params = (True, id_cliente_gmac)
        md_quota_insert_cursor.execute(query_update_clients_processed, params)
        logger.info("Update na tb_clientes_gmac_pre")
    except Exception as error:
        logger.error(f"Erro ao fazer update na tb_clientes_gmac_pre, error:{error}")
        raise error


def update_is_processed_quota(md_quota_insert_cursor, id_quota_gmac):
    try:
        logger.info("Fazendo update na tb_quotas_gmac_pre")
        query_update_quotas_processed = """
            UPDATE stage_raw.tb_quotas_gmac_pre
            SET is_processed = %s
            WHERE id_quota_gmac = %s
            RETURNING *;
        """
        params = (True, id_quota_gmac)
        md_quota_insert_cursor.execute(query_update_quotas_processed, params)
        logger.info("Update efetuado na tb_quotas_gmac_pre")
    except Exception as error:
        logger.error(f"Erro ao fazer update na tb_quotas_gmac_pre, error:{error}")
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
        logger.info("Fazendo update na pl_quotas_status")
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
        logger.error(
            f"Erro ao atualizar informações na pl_quota_status, error: {error}"
        )
        raise error


def search_quota_history_detail(md_quota_select_cursor, quota_id):
    try:
        logger.info("Obtendo dados da pl_quota_history_detail")
        query_get_quota_history_detail = f"""
            SELECT *
            FROM md_cota.pl_quota_history_detail
            WHERE quota_id = {quota_id} AND valid_to IS NULL AND is_deleted is FALSE;
        """
        md_quota_select_cursor.execute(query_get_quota_history_detail)
        query_result = md_quota_select_cursor.fetchall()
        logger.info("Obtidos dados da pl_quota_history_detail")

        return get_table_dict(md_quota_select_cursor, query_result)
    except Exception as error:
        logger.error(
            f"Erro ao obter informações na PlQuotaHistoryDetail, error: {error}"
        )
        raise error


def update_valid_to(md_quota_update_cursor, quota_id):
    try:
        logger.info("Fazendo update na pl_quota_history_detail")
        query_update_valid_to = """
            UPDATE md_cota.pl_quota_history_detail
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
        logger.info("Update efetudado na pl_quota_history_detail")
    except Exception as error:
        logger.error(f"Erro ao atualizaar informações na pl_quota_history_detail, error:{error}")
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


def create_payload_person(
    person_ext_code, person_type, customer, contacts, person_document_type
):
    payload = {
        "person_ext_code": person_ext_code,
        "person_type": person_type,
        "administrator_code": "0000000131",
        "channel_type": "EMAIL",
        "natural_person": {
            "full_name": customer["nome_razao"],
            "birthdate": None,
        },
        "contacts": contacts,
        "documents": [
            {
                "document_number": person_ext_code,
                "expiring_date": "2040-12-01",
                "person_document_type": person_document_type,
            }
        ],
        "reactive": False,
    }
    return payload


def create_payload_company(
    person_ext_code, person_type, customer, contacts, person_document_type
):
    payload = {
        "person_ext_code": person_ext_code,
        "person_type": person_type,
        "administrator_code": "0000000131",
        "channel_type": "EMAIL",
        "legal_person": {
            "company_name": customer["nome_razao"],
            "company_fantasy_name": customer["nome_razao"],
            "founding_date": None,
        },
        "contacts": contacts,
        "documents": [
            {
                "document_number": person_ext_code,
                "expiring_date": "2040-12-01",
                "person_document_type": person_document_type,
            }
        ],
        "reactive": False,
    }
    return payload


def process_quota_history_detail(
    md_quota_insert_cursor, quota_id_md_quota, row, total_assembly
):
    asset_type = asset_type_dict("VEICULOS_LEVES", None)

    quota_history_detail_to_insert = {}
    switch_quota_history_field = switch_quota_history_detail_dict()

    for keyword in switch_quota_history_field:
        quota_history_detail_to_insert[keyword] = None

    quota_history_detail_to_insert["quota_id"] = quota_id_md_quota
    quota_history_detail_to_insert["installments_paid_number"] = row["total_parc_pagas"]
    quota_history_detail_to_insert["per_adm_paid"] = row["perc_tx_adm_pg"]
    quota_history_detail_to_insert["per_adm_to_pay"] = (
        row["tx_adm"] - row["perc_tx_adm_pg"]
    )
    quota_history_detail_to_insert["current_assembly_number"] = total_assembly
    quota_history_detail_to_insert["per_mutual_fund_paid"] = row["perc_fc_pg"]
    quota_history_detail_to_insert["old_quota_number"] = row["cota"]
    quota_history_detail_to_insert["asset_value"] = row["valor_bem_atual"]
    quota_history_detail_to_insert["asset_type_id"] = asset_type
    quota_history_detail_to_insert["info_date"] = row["data_info"]
    quota_history_detail_to_insert["valid_from"] = get_default_datetime()
    quota_history_detail_to_insert["valid_to"] = None

    pl_quota_history_detail_insert_new_quota_history(
        md_quota_insert_cursor, quota_history_detail_to_insert
    )


def insert_update_quota_history_fields(
    row, quota_id_md_quota, data_source_id, cursor, funcao
):
    fields_inserted_quota_history = [
        "per_mutual_fund_paid",
        "installments_paid_number",
        "per_adm_paid",
        "per_adm_to_pay",
        "asset_value",
        "asset_type_id",
        "old_quota_number",
        "current_assembly_number",
    ]
    switch_quota_history_field = switch_quota_history_detail_dict()

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
        funcao(cursor, quota_field_update_date_insert)


def md_quota_is_none(
    md_quota_connection,
    row,
    md_quota_group,
    code_group,
    id_adm,
    md_quota_insert_cursor,
    groups_md_quota,
    quota_customer,
    quotas_md_quota,
    md_quota_select_cursor,
    data_source_id,
    lambda_customer_cubees
):
    quota_code_final = build_quota_code(md_quota_connection)
    info_date = row["data_info"]
    today = datetime.today()
    assembly_since_statement = relativedelta.relativedelta(today, info_date).months
    total_assembly = row["pz_atual"] + assembly_since_statement
    assembly_to_end = row["pz_grupo"] + total_assembly
    group_end_date = today + relativedelta.relativedelta(months=assembly_to_end)

    if md_quota_group is None:
        group_to_insert = {
            "group_code": code_group,
            "group_deadline": row["pz_grupo"],
            "administrator_id": id_adm,
            "group_closing_date": group_end_date,
        }
        group_md_quota = insert_new_group(md_quota_insert_cursor, group_to_insert)
        group_id_md_quota = group_md_quota["group_id"]
        groups_md_quota.append(group_md_quota)

    else:
        group_id_md_quota = md_quota_group["group_id"]

    status_type = switch_status_dict("EXCLUIDOS", Constants.CASE_DEFAULT_TYPES.value)
    ownership_percentage = 1
    if len(quota_customer) > 1:
        ownership_percentage = 1 / len(quota_customer)

    quota_person_type_id = 1 if row["tipo_pessoa"] == "F" else 2

    quota_md_quota = pl_quota_insert_new_quota_from_quotas(
        md_quota_insert_cursor,
        quota_code_final,
        row["n_contrato"],
        row,
        False,
        False,
        status_type,
        id_adm,
        group_id_md_quota,
        quota_person_type_id,
    )
    quota_id_md_quota = quota_md_quota["quota_id"]
    quotas_md_quota.append(quota_md_quota)

    quota_status_to_insert = {
        "quota_id": quota_id_md_quota,
        "quota_status_type_id": status_type,
    }
    insert_quota_status(md_quota_insert_cursor, quota_status_to_insert)

    process_quota_history_detail(
        md_quota_insert_cursor, quota_id_md_quota, row, total_assembly
    )
    cpf_cnpj = 0
    customers = []
    for customer in quota_customer:
        cpf_cnpj = customer["cpf_cnpj"]
        if row["tipo_pessoa"] == "F":
            person_ext_code = cpf_justified(cpf_cnpj)

            person_type = "NATURAL"
            contacts = [
                {
                    "contact_desc": "Celular",
                    "contact": f'{customer["ddd"]} {customer["celular"]}',
                    "contact_category": "PERSONAL",
                    "contact_type": "MOBILE",
                    "preferred_contact": True,
                }
            ]
            for i in range(1, 7):
                key_tell = f"tel{i}"
                key_ddd = f"ddd{i}"
                contact = contact_customer(
                    customer[key_tell],
                    customer[key_ddd],
                    "Telefone",
                    "PERSONAL",
                    "MOBILE",
                    False,
                )
                if contact is not None:
                    contacts.append(contact)
                    logger.info(contact)
            payload = create_payload_person(
                person_ext_code, person_type, customer, contacts, "CPF"
            )

        else:
            person_ext_code = cnpj_justified(cpf_cnpj)
            person_type = "LEGAL"
            contacts = [
                {
                    "contact_desc": "Celular",
                    "contact": f'{customer["ddd"]} {customer["celular"]}',
                    "contact_category": "BUSINESS",
                    "contact_type": "MOBILE",
                    "preferred_contact": True,
                }
            ]
            for i in range(1, 7):
                key_tell = f"tel{i}"
                key_ddd = f"ddd{i}"
                contact = contact_customer(
                    customer[key_tell],
                    customer[key_ddd],
                    "Telefone",
                    "BUSINESS",
                    "MOBILE",
                    False,
                )
                if contact is not None:
                    contacts.append(contact)
            payload = create_payload_company(
                person_ext_code, person_type, customer, contacts, "CS"
            )
        customers.append(payload)
        update_is_processed_client(md_quota_insert_cursor, customer["id_cliente_gmac"])
    logger.info(customers)
    requests_lambda(quota_id_md_quota, ownership_percentage, cpf_cnpj, customers, lambda_customer_cubees)
    insert_update_quota_history_fields(
        row,
        quota_id_md_quota,
        data_source_id,
        md_quota_insert_cursor,
        insert_quota_field_update_date,
    )

    update_is_processed_quota(md_quota_insert_cursor, row["id_quota_gmac"])


def md_quota_not_none(
    md_quota_quota,
    md_quota_cursor,
    row,
    md_quota_update_cursor,
    md_quota_insert_cursor,
    md_quota_select_cursor,
    data_source_id,
    lambda_customer_cubees,
    quota_customer
):
    cpf_cnpj = 0
    quota_id_md_quota = md_quota_quota["quota_id"]
    status_type = switch_status_dict(
        "EXCLUIDOS",
        Constants.CASE_DEFAULT_TYPES.value,
    )
    data_info = row["data_info"]
    info_date = data_info
    today = datetime.today()

    assembly_since_statement = relativedelta.relativedelta(today, info_date).months
    total_assembly = row["pz_atual"] + assembly_since_statement
    customers = []
    ownership_percentage = 1 / len(quota_customer) if len(quota_customer) > 1 else 1

    for customer in quota_customer:
        cpf_cnpj = customer["cpf_cnpj"]
        if row["tipo_pessoa"] == "F":
            person_ext_code = cpf_justified(cpf_cnpj)
            person_type = "NATURAL"
            contacts = [
                {
                    "contact_desc": "Celular",
                    "contact": f'{customer["ddd"]} {customer["celular"]}',
                    "contact_category": "PERSONAL",
                    "contact_type": "MOBILE",
                    "preferred_contact": True,
                }
            ]
            for i in range(1, 7):
                key_tell = f"tel{i}"
                key_ddd = f"ddd{i}"
                contact = contact_customer(
                    customer[key_tell],
                    customer[key_ddd],
                    "Telefone",
                    "PERSONAL",
                    "MOBILE",
                    False,
                )
                logger.info(contact)
                if contact is not None:
                    contacts.append(contact)

            payload = create_payload_person(
                person_ext_code, person_type, customer, contacts, "CPF"
            )

        else:
            person_ext_code = cnpj_justified(cpf_cnpj)
            person_type = "LEGAL"

            contacts = [
                {
                    "contact_desc": "Celular",
                    "contact": f'{customer["ddd"]} {customer["celular"]}',
                    "contact_category": "BUSINESS",
                    "contact_type": "MOBILE",
                    "preferred_contact": True,
                }
            ]
            for i in range(1, 7):
                key_tell = f"tel{i}"
                key_ddd = f"ddd{i}"
                contact = contact_customer(
                    customer[key_tell],
                    customer[key_ddd],
                    "Telefone",
                    "BUSINESS",
                    "MOBILE",
                    False,
                )
                if contact is not None:
                    contacts.append(contact)
            payload = create_payload_company(
                person_ext_code, person_type, customer, contacts, "CS"
            )
        customers.append(payload)
        update_is_processed_client(md_quota_update_cursor, customer["id_cliente_gmac"])
    logger.info(customers)
    requests_lambda(quota_id_md_quota, ownership_percentage, cpf_cnpj, customers, lambda_customer_cubees)
    if row["data_info"] > md_quota_quota["info_date"]:
        quota_update = {
            "quota_id": quota_id_md_quota,
            "quota_status_type_id": status_type,
            "contract_number": row["n_contrato"],
            "external_reference": row["n_contrato"],
            "quota_number": row["cota"],
        }

        update_quota_referenceId(md_quota_update_cursor, quota_update)

    if md_quota_quota["quota_status_type_id"] != status_type:
        quota_status_to_update = {"quota_id": quota_id_md_quota}

        update_quota_status(md_quota_update_cursor, quota_status_to_update)

        quota_status_to_insert = {
            "quota_id": quota_id_md_quota,
            "quota_status_type_id": status_type,
        }

        insert_quota_status(md_quota_insert_cursor, quota_status_to_insert)

    quota_history_detail_md_quota = search_quota_history_detail(
        md_quota_select_cursor, quota_id_md_quota
    )
    if len(quota_history_detail_md_quota) > 0:
        quota_history_detail_md_quota = quota_history_detail_md_quota[0]
        if quota_history_detail_md_quota["info_date"] < row["data_info"]:
            update_valid_to(md_quota_update_cursor, quota_id_md_quota)

            process_quota_history_detail(
                md_quota_insert_cursor, quota_id_md_quota, row, total_assembly
            )
            insert_update_quota_history_fields(
                row,
                quota_id_md_quota,
                data_source_id,
                md_quota_update_cursor,
                update_quota_field_update_date,
            )
    else:
        process_quota_history_detail(
            md_quota_insert_cursor, quota_id_md_quota, row, total_assembly
        )
        insert_update_quota_history_fields(
            row,
            quota_id_md_quota,
            data_source_id,
            md_quota_update_cursor,
            update_quota_field_update_date,
        )

    update_is_processed_quota(md_quota_update_cursor, row["id_quota_gmac"])


def process_row(
    row,
    groups_md_quota,
    md_quota_select_cursor,
    quotas_md_quota,
    md_quota_connection,
    id_adm,
    md_quota_insert_cursor,
    md_quota_cursor,
    data_source_id,
    md_quota_update_cursor,
    lambda_customer_cubees
):
    code_group = string_right_justified(row["grupo"])
    md_quota_group = get_dict_by_id(code_group, groups_md_quota, "group_code")
    md_quota_quota = get_dict_by_id(
        str(row["n_contrato"]),
        quotas_md_quota,
        "external_reference",
    )
    quota_customer = client_gmac_pre_read_data_stage_raw(
        md_quota_select_cursor, str(row["grupo"]), str(row["cota"])
    )
    if md_quota_quota is None:
        md_quota_is_none(
            md_quota_connection,
            row,
            md_quota_group,
            code_group,
            id_adm,
            md_quota_insert_cursor,
            groups_md_quota,
            quota_customer,
            quotas_md_quota,
            md_quota_select_cursor,
            data_source_id,
            lambda_customer_cubees
        )

    else:
        md_quota_not_none(
            md_quota_quota,
            md_quota_cursor,
            row,
            md_quota_update_cursor,
            md_quota_insert_cursor,
            md_quota_select_cursor,
            data_source_id,
            lambda_customer_cubees,
            quota_customer
        )


def process_all(
    quotas_gmac,
    groups_md_quota,
    md_quota_select_cursor,
    quotas_md_quota,
    md_quota_connection,
    id_adm,
    md_quota_insert_cursor,
    md_quota_cursor,
    data_source_id,
    md_quota_update_cursor,
    lambda_customer_cubees
):
    for row in quotas_gmac:
        process_row(
            row,
            groups_md_quota,
            md_quota_select_cursor,
            quotas_md_quota,
            md_quota_connection,
            id_adm,
            md_quota_insert_cursor,
            md_quota_cursor,
            data_source_id,
            md_quota_update_cursor,
            lambda_customer_cubees
        )


def from_file_to_model():
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()
    md_quota_insert_cursor = md_quota_connection.cursor()
    md_quota_select_cursor = md_quota_connection.cursor()
    md_quota_update_cursor = md_quota_connection.cursor()
    md_quota_cursor = md_quota_connection.cursor()
    args = getResolvedOptions(
        sys.argv,
        [
            "md_cota_cubees_customer_lambda"
        ],
    )
    lambda_customer_cubees = args["md_cota_cubees_customer_lambda"]
    try:
        id_adm = read_adm(md_quota_select_cursor)
        groups_md_quota = read_groups(id_adm, md_quota_select_cursor)
        quotas_md_quota = quotas_all(id_adm, md_quota_select_cursor)
        quotas_gmac = read_data_stage_raw(md_quota_select_cursor)
        data_source_id = get_data_source_id(md_quota_select_cursor)
        process_all(
            quotas_gmac,
            groups_md_quota,
            md_quota_select_cursor,
            quotas_md_quota,
            md_quota_connection,
            id_adm,
            md_quota_insert_cursor,
            md_quota_cursor,
            data_source_id,
            md_quota_update_cursor,
            lambda_customer_cubees
        )
        logger.info("Dados processados com sucesso.")
        md_quota_connection.commit()
    except Exception as error:
        logger.error(f"Erro ao processar dados: error:{error}")
        md_quota_connection.rollback()
        raise error

    finally:
        # Close the cursor and connection
        md_quota_cursor.close()
        md_quota_connection.close()
        logger.info("Connection closed.")


if __name__ == "__main__":
    from_file_to_model()
