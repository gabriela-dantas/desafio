from datetime import datetime

from bazartools.common.database.quotaCodeBuilder import build_quota_code
from bazartools.common.logger import get_logger
from bazartools.common.database.glueConnection import GlueConnection
# from bazartools.common.database.quotaCodeBuilder import build_quota_code
from psycopg2 import extras

import psycopg2.extensions as psycopg2

# from awsglue.utils import getResolvedOptions

GLUE_DEFAULT_CODE = 2
BATCH_SIZE = 500
logger = get_logger()
QUOTA_ORIGIN_ID = 3
ADMINISTRATOR_CODE = '0000000131'
ASSET_TYPE_ID = 7


def get_default_datetime() -> datetime:
    return datetime.now()


def cd_group_right_justified(group: str) -> str:
    code_group = group if len(group) == 5 else group.rjust(5, "0")
    return code_group


def get_table_dict(cursor: psycopg2.cursor, rows: dict) -> list:
    column_names = [desc[0] for desc in cursor.description]
    return [dict(zip(column_names, row)) for row in rows]


def get_dict_by_id(id_item: str, data_list: list, field_name: str) -> dict:
    filtered_items = filter(lambda item: item[field_name] == id_item, data_list)
    return next(filtered_items, None)


def switch_status(key: str, value: int) -> int:
    status = {
        "NORMAL": 1,
        "COTA CONTEMPLADA PENDENTE": 4,
        "QUITADO": 6,
    }
    return status.get(key, value)


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


def select_data_source_id(md_quota_select_cursor):
    try:
        logger.info(f"Buscando id da fonte de dados no md-cota...")
        query_select_data_source = (
            f"""
        SELECT 
            pds.data_source_id 
        FROM 
            md_cota.pl_data_source pds 
        WHERE 
            pds.data_source_desc = 'FILE'
        AND
            pds.is_deleted is false
        """
        )
        md_quota_select_cursor.execute(query_select_data_source)
        query_result_data_source = md_quota_select_cursor.fetchall()
        data_source_dict = get_table_dict(
            md_quota_select_cursor, query_result_data_source
        )
        logger.info(f"Id da fonte de dados recuperado com sucesso md-cota!")
        return data_source_dict[0]["data_source_id"]
    except Exception as error:
        logger.info(f"Erro ao fazer o select na tabela pl_data_source_desc, error:{error}")
        raise error


def insert_quota_status(cursor: psycopg2.cursor, quota_id: int, status_type: int) -> None:
    try:
        logger.info(f"Inserindo novo status para quota_id: {quota_id}")
        query_insert_quota_status = (
            f"""
            INSERT INTO md_cota.pl_quota_status
            (
                quota_id,
                quota_status_type_id,
                created_by,
                modified_by,
                created_at,
                modified_at,
                valid_from
            )
            VALUES
            (
                %s, %s, %s, %s,%s, %s, %s
            );
        """
        )
        cursor.execute(
            query_insert_quota_status,
            (quota_id, status_type, GLUE_DEFAULT_CODE, GLUE_DEFAULT_CODE, "now()", "now()", "now()"),
        )
        logger.info("Status inserido com sucesso!")

    except Exception as error:
        logger.error(f"Erro ao executar a query de inserção do status da quota: {str(error)}")
        raise error


def select_quota_md_quota(cursor: psycopg2.cursor, id_adm: int) -> list:
    try:
        logger.info("Buscando informações de cotas no md-cota...")
        query_select_quota_md_quota = (
            f"""
        SELECT
            pq.*
        FROM
            md_cota.pl_quota pq
        LEFT JOIN
            md_cota.pl_administrator pa ON pa.administrator_id = pq.administrator_id 
        WHERE
            pq.is_deleted IS FALSE
        AND 
            pa.administrator_code = '{ADMINISTRATOR_CODE}'
        AND
            pq.is_deleted is false;
        """
        )
        cursor.execute(query_select_quota_md_quota, [id_adm])
        query_result_quotas = cursor.fetchall()
        logger.info("Informações de cotas recuperadas com sucesso no md-cota!")
        return get_table_dict(cursor, query_result_quotas)
    except Exception as error:
        logger.error(f"Não foi possível executar"
                     f" o select na tabela pl_quota: error:{error}")
        raise error


def select_administrator_id(cursor: psycopg2.cursor) -> int:
    try:
        logger.info(f"Buscando id da adm no md-cota...")
        query_select_adm = (
            f"""
        SELECT
            pa.administrator_id
        FROM
            md_cota.pl_administrator pa
        WHERE 
            pa.administrator_code = %s
        AND
            pa.is_deleted is false;
        """
        )
        logger.info(f"query leitura adm md-cota: {query_select_adm}")
        cursor.execute(query_select_adm, [ADMINISTRATOR_CODE])
        query_result_adm = cursor.fetchall()
        logger.info(f"Id da adm recuperado com sucesso md-cota!")
        adm_dict = get_table_dict(cursor, query_result_adm)
        return adm_dict[0]["administrator_id"] if adm_dict is not None else None
    except Exception as error:
        logger.error(f"Não foi possível executar"
                     f" o select na tabela pl_administrator: error:{error}")
        raise error


def select_group_md_quota(cursor: psycopg2.cursor) -> list:
    logger.info("Buscando informações de grupos no md-cota...")
    try:
        query_select_group_md_quota = (
            f"""
        SELECT
            pg.group_id,
            pg.group_code,
            pg.group_deadline
        FROM
            md_cota.pl_group pg
        LEFT JOIN
            md_cota.pl_administrator pa ON pa.administrator_id = pg.administrator_id
        WHERE
            pa.administrator_code = %s
        AND
            pg.is_deleted is false;
        """
        )
        cursor.execute(query_select_group_md_quota, [ADMINISTRATOR_CODE])
        logger.info("Informações de grupos recuperadas com sucesso no md-cota!")
        query_result_groups = cursor.fetchall()
        groups_list = get_table_dict(cursor, query_result_groups)
        return groups_list
    except Exception as error:
        logger.error(f"Não foi possível executar"
                     f" o select na tabela pl_group: error:{error}")
        raise error


def select_quotas_pos_stage_raw(cursor: psycopg2.cursor) -> None:
    try:
        logger.info("Buscando quotas na tb_quotas_gmac_pos")
        query = (
            f"""
            SELECT *
            FROM stage_raw.tb_quotas_gmac_pos
            WHERE is_processed is FALSE;
            """
        )
        cursor.execute(query)
    except Exception as error:
        logger.error(f"Error ao busca quota na tb_quotas_gmac_pos, error:{error}")
        raise error


def update_quota_pl_quota(cursor: psycopg2.cursor, row_dict: dict, quota_id: int,
                          is_contemplated: bool) -> None:
    try:
        logger.info("Fazendo update da quota na pl_quota")
        query = (
            f"""
            UPDATE md_cota.pl_quota
            SET quota_number = {row_dict['cota']},
                check_digit = {row_dict['versao']},
                is_contemplated = {is_contemplated},
                contract_number = {row_dict['contrato']},
                modified_at = now(),
                modified_by = {GLUE_DEFAULT_CODE}
            WHERE quota_id = {quota_id}; 
            """
        )
        cursor.execute(query)
        logger.info("Informações da pl_quota atualizadas com sucesso.")
    except Exception as error:
        logger.error(f"Error ao fazer update de quota na pl_quota, error:{error}")
        raise error


def insert_quota_pl_quota(md_quota_connection: psycopg2.connection, cursor: psycopg2.cursor, row_dict: dict,
                          is_contemplated: bool, status_type: int, external_reference: str,
                          id_adm: int, group_id: int, origin_id: int) -> dict:
    quota_code = build_quota_code(md_quota_connection)
    try:
        logger.info(f"Inserindo nova cota no md-cota...")
        query = (
            f"""
            INSERT INTO md_cota.pl_quota (
                quota_code,
                external_reference,
                administrator_id,
                group_id,
                quota_origin_id,
                contract_number,
                quota_number,
                check_digit,
                is_contemplated,
                info_date,
                quota_status_type_id,
                total_installments,
                created_at,
                modified_at,
                created_by,
                modified_by
            )
            VALUES (%s, %s, %s,%s, %s, %s,
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *;
        """
        )
        params = (
            quota_code,
            external_reference,
            id_adm,
            group_id,
            origin_id,
            row_dict.get('contrato'),
            row_dict.get('cota'),
            row_dict.get("versao"),
            is_contemplated,
            row_dict.get('data_info'),
            status_type,
            row_dict.get('pz_cota'),
            "now()", "now()", GLUE_DEFAULT_CODE, GLUE_DEFAULT_CODE,
        )
        cursor.execute(query, params)
        result = cursor.fetchall()
        quota_dict = get_table_dict(cursor, result)
        logger.info("Nova cota inserida com sucesso!")
        return quota_dict[0] if quota_dict else None
    except Exception as error:
        logger.error(f"Erro ao executar a query de inserção da quota: {str(error)}")
        raise error


def update_pl_quotas_status(cursor: psycopg2.cursor, quota_id: int) -> None:
    try:
        logger.info(f"Atualizando registro antigo do status da cota com quota_id {quota_id}")
        query = (
            f"""
        UPDATE md_cota.pl_quota_status
        SET
            valid_to = %s,
            modified_at = %s,
            modified_by = %s
        WHERE quota_id = {quota_id} AND valid_to IS NULL;
        """
        )
        cursor.execute(
            query,
            (
                "now()",
                "now()",
                GLUE_DEFAULT_CODE
            ),
        )
        logger.info("Status atualizado com sucesso!")
    except Exception as error:
        logger.error(f"Erro ao atualizar o status da cota: {str(error)}")
        raise error


def update_quota_history_detail_valid_to(cursor: psycopg2.cursor,
                                         quota_history_detail_id: int) -> None:
    try:
        logger.info(f"Atualizando histórico da cota com quota_history_detail_id {quota_history_detail_id}")
        query = (
            f"""
        UPDATE md_cota.pl_quota_history_detail
        SET
            valid_to = now(),
            modified_by = {GLUE_DEFAULT_CODE},
            modified_at = now()
        WHERE quota_history_detail_id = {quota_history_detail_id};
        """
        )
        cursor.execute(query)
        logger.info("Histórico da cota atualizado com sucesso!")

    except Exception as error:
        logger.error(f"Erro ao atualizar o histórico da cota: {str(error)}")
        raise error


def insert_quota_history_detail(cursor: psycopg2.cursor, quota_id: int, row_dict: dict) -> None:
    try:
        logger.info(f"Inserindo novo histórico de cota para quota_id: {quota_id}")
        query = (
            f"""
            INSERT INTO md_cota.pl_quota_history_detail
            (
                quota_id,
                per_amount_paid,
                info_date,
                asset_value,
                asset_type_id,
                valid_from,
                created_at,
                modified_at,
                created_by,
                modified_by
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s
            );
        """
        )
        cursor.execute(
            query,
            (
                quota_id,
                row_dict['perc_pago'],
                row_dict['data_info'],
                row_dict['valor_credito'],
                ASSET_TYPE_ID,
                "now()",
                'now()',
                'now()',
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE
            ),
        )
        logger.info("Histórico da cota inserido com sucesso!")
    except Exception as error:
        logger.error(f"Erro ao executar a query de inserção no histórico da quota: {error}")
        raise error


def insert_quota_history_detail_update(cursor: psycopg2.cursor,
                                       history_detail: dict) -> None:
    try:
        logger.info(f"Inserindo novo histórico de cota para quota_id: {history_detail['quota_id']}")
        query = (
            """
            INSERT INTO md_cota.pl_quota_history_detail
            (
                quota_id,
                old_quota_number,
                old_digit,
                quota_plan,
                installments_paid_number,
                overdue_installments_number,
                overdue_percentage,
                per_amount_paid,
                per_mutual_fund_paid,
                per_reserve_fund_paid,
                per_adm_paid,
                per_subscription_paid,
                per_mutual_fund_to_pay,
                per_reserve_fund_to_pay,
                per_adm_to_pay,
                per_subscription_to_pay,
                per_insurance_to_pay,
                per_install_diff_to_pay,
                per_total_amount_to_pay,
                amnt_mutual_fund_to_pay,
                amnt_reserve_fund_to_pay,
                amnt_adm_to_pay,
                amnt_subscription_to_pay,
                amnt_insurance_to_pay,
                amnt_fine_to_pay,
                amnt_interest_to_pay,
                amnt_others_to_pay,
                amnt_install_diff_to_pay,
                amnt_to_pay,
                quitter_assembly_number,
                cancelled_assembly_number,
                adjustment_date,
                current_assembly_date,
                current_assembly_number,
                asset_adm_code,
                asset_description,
                asset_value,
                asset_type_id,
                info_date,
                valid_from,
                created_at,
                modified_at,
                created_by,
                modified_by
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            );
        """)

        cursor.execute(
            query,
            (
                history_detail['quota_id'],
                history_detail['old_quota_number'],
                history_detail['old_digit'],
                history_detail['quota_plan'],
                history_detail['installments_paid_number'],
                history_detail['overdue_installments_number'],
                history_detail['overdue_percentage'],
                history_detail['per_amount_paid'],
                history_detail['per_mutual_fund_paid'],
                history_detail['per_reserve_fund_paid'],
                history_detail['per_adm_paid'],
                history_detail['per_subscription_paid'],
                history_detail['per_mutual_fund_to_pay'],
                history_detail['per_reserve_fund_to_pay'],
                history_detail['per_adm_to_pay'],
                history_detail['per_subscription_to_pay'],
                history_detail['per_insurance_to_pay'],
                history_detail['per_install_diff_to_pay'],
                history_detail['per_total_amount_to_pay'],
                history_detail['amnt_mutual_fund_to_pay'],
                history_detail['amnt_reserve_fund_to_pay'],
                history_detail['amnt_adm_to_pay'],
                history_detail['amnt_subscription_to_pay'],
                history_detail['amnt_insurance_to_pay'],
                history_detail['amnt_fine_to_pay'],
                history_detail['amnt_interest_to_pay'],
                history_detail['amnt_others_to_pay'],
                history_detail['amnt_install_diff_to_pay'],
                history_detail['amnt_to_pay'],
                history_detail['quitter_assembly_number'],
                history_detail['cancelled_assembly_number'],
                history_detail['adjustment_date'],
                history_detail['current_assembly_date'],
                history_detail['current_assembly_number'],
                history_detail['asset_adm_code'],
                history_detail['asset_description'],
                history_detail['asset_value'],
                history_detail['asset_type_id'],
                history_detail['info_date'],
                history_detail['valid_from'],
                'now()',
                'now()',
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE
            ),
        )

        logger.info("Histórico da cota inserido com sucesso!")
    except Exception as error:
        logger.error(f"Erro ao executar a query de inserção no histórico da quota: {error}")
        raise error


def insert_quota_field_update_date(cursor: psycopg2.cursor, quota_id: int, row_dict: dict,
                                   history_field_id: str, data_source_id: int) -> None:
    try:
        logger.info(f"Inserindo nova data de atualização do histórico do quota_id {quota_id}")
        query = (
            f"""
            INSERT INTO md_cota.pl_quota_field_update_date
            (
                quota_id,
                data_source_id,
                quota_history_field_id,
                update_date,
                created_by,
                modified_by,
                created_at,
                modified_at
            )
            VALUES
            (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        )
        cursor.execute(
            query,
            (
                quota_id,
                data_source_id,
                history_field_id,
                row_dict["data_info"],
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
                "now()",
                "now()"
            ),
        )
        logger.info("Data de atualização inserida com sucesso!")

    except Exception as error:
        logger.error(
            f"Erro ao executar a query de inserção na tabela de datas de atualização do campo da quota: {str(error)}")
        raise error


def insert_group(cursor: psycopg2.cursor, group_deadline: str, id_adm: int,
                 group: str) -> dict:
    try:
        logger.info(f"Inserindo novo grupo no md-cota: {group_deadline}")
        query = (
            f"""
            INSERT INTO md_cota.pl_group
            (
                group_code,
                group_deadline,
                administrator_id,
                created_at,
                modified_at,
                created_by,
                modified_by
            )
            VALUES
            (
                %s,%s,%s,%s,%s,%s,%s
            )
            RETURNING *
        """
        )
        cursor.execute(
            query,
            (
                group,
                group_deadline,
                id_adm,
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )

        group_inserted = cursor.fetchall()
        logger.info("Grupo inserido com sucesso!")
        group = get_table_dict(cursor, group_inserted)[0]
        logger.info(f"group_id inserido {group}")
        return group if group else None
    except Exception as error:
        logger.error(f"Erro ao executar a query de inserção de grupo no md-cota: {str(error)}")
        raise error


def update_field_update_date(cursor: psycopg2.cursor, data_source_id: int,
                             history_field_id: str, data_info: str, quota_id: int) -> None:
    try:
        logger.info("Fazendo update na field_update_date")
        query = (
            f"""
            UPDATE md_cota.pl_quota_field_update_date
            SET 
                data_source_id = {data_source_id},
                update_date = '{data_info}',
                modified_at = now(),
                modified_by = {GLUE_DEFAULT_CODE}
            WHERE quota_history_field_id = {history_field_id} AND quota_id = {quota_id};
            """
        )
        cursor.execute(query)
        logger.info("Update na field_update_date concluído com sucesso.")
    except Exception as error:
        logger.error(f"Erro ao atualizar a field_update_date: {str(error)}")
        raise error


def select_quota_history_detail(cursor: psycopg2.cursor, quota_id: int) -> dict:
    try:
        logger.info("Buscando quotas na pl_quota_history_detail")
        query = (
            f"""
            SELECT *
            FROM md_cota.pl_quota_history_detail
            WHERE quota_id = {quota_id} AND valid_to is null;
            """
        )
        cursor.execute(query)
        result = cursor.fetchall()
        quota_dict = get_table_dict(cursor, result)
        logger.info("Nova cota inserida com sucesso!")
        return quota_dict[0] if quota_dict else None
    except Exception as error:
        logger.error(f"Error ao busca quota na pl_quota_history_detail, error:{error}")
        raise error


def select_quota_field_update_date(cursor: psycopg2.cursor, quota_id: int, quota_history_field_id: str) -> dict:
    try:
        logger.info("Buscando quotas na pl_quota_field_update_date")
        query = (
            f"""
            SELECT *
            FROM md_cota.pl_quota_field_update_date
            WHERE quota_id = {quota_id} AND 
            quota_history_field_id = {quota_history_field_id};
            """
        )
        cursor.execute(query)
        result = cursor.fetchall()
        quota_dict = get_table_dict(cursor, result)
        logger.info("Registro buscado com sucesso na tabela pl_quota_field_update_date!")
        return quota_dict[0] if quota_dict else None
    except Exception as error:
        logger.error(f"Error ao busca quota na pl_quota_history_detail, error:{error}")
        raise error


def update_stage_raw(cursor: psycopg2.cursor, id_quotas: int) -> None:
    try:
        logger.info(f"Atualizando cota já processada no stage_raw...")
        query_update_stage_raw = f"""
            UPDATE stage_raw.tb_quotas_gmac_pos 
            SET is_processed = true
            WHERE id_quotas_gmac_pos = {id_quotas};
        """
        logger.info(f"query de atualização: {query_update_stage_raw}")
        cursor.execute(query_update_stage_raw)
        logger.info("Cota processada atualizada com sucesso!")

    except Exception as error:
        logger.error(f"Erro ao atualizar o estágio bruto: {str(error)}")
        raise error


def fields_inserted_quota_history() -> list:
    return [
        "old_quota_number",
        "asset_value",
    ]


def insert_field_history_detail(history_detail: dict, row_dict: dict) -> dict:
    history_detail['old_quota_number'] = row_dict['cota']
    history_detail['installments_number'] = row_dict['plano_cota']
    history_detail['asset_value'] = row_dict['valor_credito']
    history_detail['info_date'] = row_dict['data_info']

    return history_detail


def select_pl_quota_owner(cursor: psycopg2.cursor, quota_id: int) -> dict:
    try:
        logger.info("Buscando dados na pl_quota_owner")
        query = (
            f"""
            SELECT *
            FROM md_cota.pl_quota_owner
            WHERE quota_id = {quota_id} AND valid_to is null;
            """
        )
        cursor.execute(query)
        result = cursor.fetchall()
        quota_dict = get_table_dict(cursor, result)
        logger.info("Busca de dados na pl_quota_owner")
        return quota_dict[0] if quota_dict else None
    except Exception as error:
        logger.info("Error ao busca dados pl_quota_owner")
        raise error


def update_pl_quota_owner(cursor: psycopg2.cursor, quota_id: int) -> None:
    try:
        logger.info("Fazendo update na pl_quota_onwer")
        query = (
            f"""
            UPDATE md_cota.pl_quota_owner
            SET valid_to = now()
            WHERE quota_id = {quota_id} AND valid_to is null;
            """
        )
        cursor.execute(query)
    except Exception as error:
        logger.error("Erro aos fazer update na pl_quota_owner")
        raise error


def insert_pl_quota_owner(cursor: psycopg2.cursor, quota_id: int, person_code: str) -> None:
    try:
        logger.info("Inserindo dados na pl_quota_owner")
        query = (
            f"""
            INSERT INTO md_cota.pl_quota_owner
            (
             ownership_percent,
             quota_id,
             person_code,
             valid_from,
             created_at,
             modified_at,
             created_by,
             modified_by
            )
            VALUES 
            (%s, %s, %s, %s, %s, %s, %s, %s);           
            """)
        params = (
            "1",
            quota_id,
            person_code,
            "now()",
            "now()",
            "now()",
            GLUE_DEFAULT_CODE,
            GLUE_DEFAULT_CODE
        )
        cursor.execute(query, params)
    except Exception as error:
        logger.error("Erro ao inserir dados na pl_quota_owner")
        raise error


def data_fund():
    data = {"28472352000169": '0000061932',
            '31904252000179': '0000061935',
            '35448967000115': '0000061936',
            '42401957000190': '0000227564'}
    return data


def update_owner_md_quota(cursor: psycopg2.cursor, row_dict: dict, quota_id: int) -> None:
    data_owner = select_pl_quota_owner(cursor, quota_id)
    dt_fund = data_fund()
    key = row_dict["cpf_cnpj"].replace('.', '').replace('-', '').replace('/', '')
    person_code = dt_fund[key]
    if data_owner:
        if data_owner['person_code'] != person_code:
            update_pl_quota_owner(cursor, quota_id)
            insert_pl_quota_owner(cursor, quota_id, person_code)
    else:
        insert_pl_quota_owner(cursor, quota_id, person_code)


def quota_exist_md_quota(cursor: psycopg2.cursor, quota_exist: dict, row_dict: dict,
                         is_contemplated: bool, status_type: int, data_source_id: int) -> None:
    quota_id = quota_exist['quota_id']
    logger.info(f"A quota:{quota_id} existe todos os dados serão atualizados...")
    update_quota_pl_quota(cursor, row_dict, quota_exist['quota_id'], is_contemplated)

    if quota_exist["quota_status_type_id"] != status_type and status_type != 5:
        update_pl_quotas_status(cursor, quota_id)
        insert_quota_status(cursor, quota_id, status_type)

    history_detail = select_quota_history_detail(cursor, quota_id)
    if history_detail:
        logger.info("History detail encontrado, continuando o fluxo de update da cota...")
        history_detail_insert = insert_field_history_detail(history_detail, row_dict)

        update_quota_history_detail_valid_to(cursor, history_detail["quota_history_detail_id"])
        insert_quota_history_detail_update(cursor, history_detail_insert)

        field_inserted = fields_inserted_quota_history()
        for field in field_inserted:
            history_field_id = switch_quota_history_field(field, 0)
            field_update_date = select_quota_field_update_date(cursor, quota_id, history_field_id)
            if field_update_date:
                update_field_update_date(cursor, data_source_id, history_field_id,
                                         history_detail['info_date'], quota_id)
            else:
                insert_quota_field_update_date(cursor, quota_id, row_dict, history_field_id, data_source_id)
    else:
        insert_quota_history_detail(cursor, quota_id, row_dict)
        field_inserted = fields_inserted_quota_history()
        for field in field_inserted:
            history_field_id = switch_quota_history_field(field, 0)
            insert_quota_field_update_date(cursor, quota_id, row_dict, history_field_id, data_source_id)


def quota_not_exist_md_quota(cursor: psycopg2.cursor, row_dict: dict, md_quota_connection: psycopg2.connection,
                             is_contemplated: bool, status_type: int, external_reference: str,
                             id_adm: int, group_id: int, data_source_id: int, quotas_list: list) -> int:
    logger.info(f"A quota:{row_dict.get('cota')} não existe todos os dados serão inseridos...")
    quota = insert_quota_pl_quota(md_quota_connection, cursor, row_dict, is_contemplated, status_type,
                                  external_reference, id_adm, group_id, QUOTA_ORIGIN_ID)
    quotas_list.append(quota)
    quota_id = quota['quota_id']
    insert_quota_status(cursor, quota_id, status_type)
    insert_quota_history_detail(cursor, quota_id, row_dict)
    field_inserted = fields_inserted_quota_history()
    for field in field_inserted:
        history_field_id = switch_quota_history_field(field, 0)
        insert_quota_field_update_date(cursor, quota_id, row_dict, history_field_id, data_source_id)
    return quota_id


def get_group_id(cursor: psycopg2.cursor, id_adm: int, group_exist: dict,
                 groups_list: list, group: str, group_deadline: str) -> int:
    try:
        group_id = group_exist['group_id']
    except TypeError:
        group_inserted = insert_group(cursor, group_deadline, id_adm, group)
        groups_list.append(group_inserted)
        group_id = group_inserted['group_id']

    return group_id


def process_row(row_dict: dict, cursor: psycopg2.cursor, md_quota_connection: psycopg2.connection,
                groups_list: list, quotas_list: list, id_adm: int, data_source_id: int) -> None:
    group_deadline = row_dict['plano_grupo']
    is_contemplated = True if row_dict['data_contemplacao'] is not None else False
    group = cd_group_right_justified(row_dict['grupo'])
    status_type = switch_status(row_dict['situacao_cota_desc'], 5)
    group_exist = get_dict_by_id(group, groups_list, "group_code")
    quota_exist = get_dict_by_id(row_dict['contrato'], quotas_list, "contract_number")
    external_reference = row_dict['contrato']
    group_id = get_group_id(cursor, id_adm, group_exist, groups_list, group, group_deadline)

    if quota_exist:
        if row_dict['data_info'] > quota_exist["info_date"]:
            quota_exist_md_quota(cursor, quota_exist, row_dict,
                                 is_contemplated, status_type, data_source_id)
        quota_id = quota_exist['quota_id']
    else:
        quota_id = quota_not_exist_md_quota(cursor, row_dict, md_quota_connection,
                                            is_contemplated, status_type, external_reference,
                                            id_adm, group_id, data_source_id, quotas_list)
    update_stage_raw(cursor, row_dict['id_quotas_gmac_pos'])
    update_owner_md_quota(cursor, row_dict, quota_id)


def process_all(quotas_cursor: psycopg2.cursor, cursor: psycopg2.cursor, md_quota_connection: psycopg2.connection,
                groups_list: list, quotas_list: list, id_adm: int, data_source_id: int) -> None:
    batch_counter = 0
    while True:
        try:
            batch_counter += 1
            rows = quotas_cursor.fetchmany(size=BATCH_SIZE)
            logger.info(f"Fetch {BATCH_SIZE} rows at a time - Batch number {batch_counter}")
            if not rows:
                break
            for row in rows:
                row_dict = dict(row)
                process_row(row_dict, cursor, md_quota_connection, groups_list, quotas_list,
                            id_adm, data_source_id)
                logger.info(f"Quota a ser processada: {row_dict}")
            md_quota_connection.commit()
        except Exception as error:
            logger.error(f"Transação revertida devido a um erro:{error}")
            md_quota_connection.rollback()
            raise error


def quota_ingestion_gmac_pos() -> None:
    connection_factory = GlueConnection(connection_name="md-cota")
    connection = connection_factory.get_connection()
    connection.set_isolation_level(psycopg2.ISOLATION_LEVEL_READ_COMMITTED)
    md_quota_cursor = connection.cursor(cursor_factory=extras.RealDictCursor)
    cursor = connection.cursor()
    try:
        id_adm = select_administrator_id(cursor)
        groups_dict = select_group_md_quota(cursor)
        quotas_dict = select_quota_md_quota(cursor, id_adm)
        data_source_id = select_data_source_id(cursor)
        select_quotas_pos_stage_raw(md_quota_cursor)
        process_all(md_quota_cursor, cursor, connection, groups_dict, quotas_dict,
                    id_adm, data_source_id)
        logger.info("Dados processados com sucesso. Todas as informações atualizadas foram inseridas no banco.")
    except Exception as error:
        logger.error(f"Erro ao processar dados da tb_quotas_gmac_pos. Error{error}")
        raise error
    finally:
        connection.close()
        cursor.close()
        logger.info("Conexão com o banco finalizada.")


if __name__ == "__main__":
    quota_ingestion_gmac_pos()
