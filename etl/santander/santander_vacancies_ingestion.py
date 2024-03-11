from typing import Union

from bazartools.common.logger import get_logger
from bazartools.common.database.glueConnection import GlueConnection
from bazartools.common.database.assetCodeBuilder import build_asset_code

from datetime import date
from dateutil.relativedelta import relativedelta
from itertools import groupby
from operator import itemgetter
import sys

from psycopg2 import ProgrammingError

BATCH_SIZE = 500
GLUE_DEFAULT_CODE = 2
logger = get_logger()


def get_table_dict(cursor, rows):
    column_names = [desc[0] for desc in cursor.description]
    return [dict(zip(column_names, row)) for row in rows]


def cd_group_right_justified(group: str) -> str:
    if len(str(group)) == 5:
        code_group = str(group)
    else:
        code_group = str(group).rjust(5, "0")
    return code_group


def switch_asset_type(key: str, value: int) -> str:
    asset_type = {
        "AUTO": 2,
        "IMOVEL": 1,
        "SERVIC": 4,
        "CAMIN": 3,
    }
    return asset_type.get(key, value)


def fetch_stage_raw_groups(md_quota_cursor):
    try:
        query_select_stage_raw = """
        SELECT 
        *
        FROM 
        stage_raw.tb_sorteios_santander_pre 
        WHERE 
        is_processed = false;
        """
        logger.info(f"query leitura stage_raw: {query_select_stage_raw}")
        md_quota_cursor.execute(query_select_stage_raw)
    except Exception as error:
        logger.error(
            f"Error na busca de dados na tabela tb_grupos_santander_pre, error:{error}"
        )
        raise error


def fetch_md_quota_groups(md_quota_cursor):
    try:
        query_select_groups_md_quota = """
            SELECT
            pg.group_id,
            pg.group_code,
            pg.group_closing_date,
            pg.group_deadline
            FROM
            md_cota.pl_group pg
            LEFT JOIN md_cota.pl_administrator pa ON pa.administrator_id = pg.administrator_id
            WHERE
            pa.administrator_desc = 'SANTANDER ADM. CONS. LTDA'
            AND
            pg.is_deleted is false
            """
        logger.info(f"query leitura grupos md-cota: {query_select_groups_md_quota}")
        md_quota_cursor.execute(query_select_groups_md_quota)
        query_result_groups = md_quota_cursor.fetchall()
        return get_table_dict(md_quota_cursor, query_result_groups)
    except Exception as error:
        logger.error(
            f"Não foi possível fazer o select na tabela pl_group, error:{error}"
        )
        raise error


def fetch_administrator_id(md_quota_cursor) -> Union[None, str]:
    try:
        query_select_adm = """
            select
            pa.administrator_id
            from
            md_cota.pl_administrator pa
            where 
            pa.administrator_desc = 'SANTANDER ADM. CONS. LTDA'
            AND
            pa.is_deleted is false
            """
        logger.info(f"query leitura adm md-cota: {query_select_adm}")
        md_quota_cursor.execute(query_select_adm)
        query_result_adm = md_quota_cursor.fetchall()
        adm_dict = get_table_dict(md_quota_cursor, query_result_adm)
        return adm_dict[0]["administrator_id"] if adm_dict else None
    except Exception as error:
        logger.error(f"Não foi possível obter o id da administradora, error:{error}")
        raise error


def insert_group_into_database(md_quota_insert_cursor, row_group_code, id_adm):
    try:
        query_insert_group = """
            INSERT INTO md_cota.pl_group
            (
                group_code,
                administrator_id,
                created_at,
                modified_at,
                created_by,
                modified_by
            )
            VALUES
            (
                %s,%s,%s,%s,%s,%s
            )
            RETURNING GROUP_ID
        """
        md_quota_insert_cursor.execute(
            query_insert_group,
            (
                row_group_code,
                id_adm,
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )

        group_inserted = md_quota_insert_cursor.fetchall()
        group_id = get_table_dict(md_quota_insert_cursor, group_inserted)[0]["group_id"]
        logger.info(f"group_id inserido {group_id}")

        return {
            "group_id": group_id,
            "group_code": row_group_code,
            "group_closing_date": None,
        }

    except Exception as error:
        logger.error(f"Erro ao executar a query de inserção: {str(error)}")
        raise error


def get_group_vacancies(md_quota_select_cursor, group_id):
    try:
        query_select_md_quota_vacancies = """
            SELECT *
            FROM md_cota.pl_group_vacancies
            WHERE valid_to IS NULL
            AND 
            group_id = %s
            AND
            is_deleted is false
        """
        logger.info(group_id)
        md_quota_select_cursor.execute(query_select_md_quota_vacancies, [group_id])
        query_result_group_vacancies = md_quota_select_cursor.fetchall()
        group_vacancies_dict = get_table_dict(
            md_quota_select_cursor, query_result_group_vacancies
        )

        return group_vacancies_dict

    except Exception as error:
        logger.error(f"Erro ao executar a consulta: {error}")
        raise error


def update_group_vacancies(md_quota_update_cursor, vacancies):
    try:
        query_update_vacancies = """
            UPDATE md_cota.pl_group_vacancies 
            SET 
            valid_to = %s,
            modified_at = %s,
            modified_by = %s
            WHERE 
            group_vacancies_id = %s
        """

        md_quota_update_cursor.execute(
            query_update_vacancies,
            (
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                vacancies["group_vacancies_id"],
            ),
        )

        new_vacancies_to_insert = True
        return new_vacancies_to_insert

    except Exception as error:
        logger.error(f"Erro ao executar a atualização: {error}")
        raise error


def insert_group_vacancies(md_quota_insert_cursor, row_dict, group_id):
    try:
        query_insert_vacancies = """
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
                %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        md_quota_insert_cursor.execute(
            query_insert_vacancies,
            (
                row_dict["vagas"],
                row_dict["data_info"],
                group_id,
                row_dict["data_info"],
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )

        # Se você deseja obter o ID da linha recém-inserida, você pode usar o método lastrowid.
        group_vacancies_id = md_quota_insert_cursor.lastrowid

        # Se necessário, você pode realizar outras operações após a inserção.

        logger.info(f"group_vacancies_id inserido {group_vacancies_id}")
        return group_vacancies_id

    except Exception as error:
        logger.error(f"Erro ao executar a inserção: {error}")
        raise error


def update_stage_raw(md_quota_update_cursor, row_dict):
    try:
        query_update_stage_raw = f"""
            UPDATE stage_raw.tb_sorteios_santander_pre 
            SET 
            is_processed = true
            WHERE 
            id_sorteios_santander = %s;
        """

        logger.info(f"query de atualização: {query_update_stage_raw}")

        md_quota_update_cursor.execute(
            query_update_stage_raw, [row_dict["id_sorteios_santander"]]
        )

    except Exception as error:
        logger.error(f"Erro ao executar a atualização: {error}")
        raise error


def process_row(
    groups_dict,
    row_dict,
    md_quota_insert_cursor,
    md_quota_update_cursor,
    md_quota_select_cursor,
    id_adm,
):
    row_group_code = cd_group_right_justified(row_dict["grupo"])
    group_id = ""
    # buscando grupo no md-cota
    for item in groups_dict:
        if item["group_code"] == row_group_code:
            logger.info(f'group_code md-cota: {item["group_code"]}')
            logger.info(f"group_code stage_raw: {row_group_code}")
            logger.info("grupo encontrado")
            group_id = item["group_id"]
    if group_id == "":
        group = insert_group_into_database(
            md_quota_insert_cursor, row_group_code, id_adm
        )
        group_id = group["group_id"]
        groups_dict.append(group)
    group_vacancies_dict = get_group_vacancies(md_quota_select_cursor, group_id)
    new_vacancies_to_insert = False
    if group_vacancies_dict is not None:
        for vacancies in group_vacancies_dict:
            if vacancies["info_date"] < row_dict["data_info"]:
                new_vacancies_to_insert = update_group_vacancies(
                    md_quota_update_cursor, vacancies
                )
    if new_vacancies_to_insert:
        insert_group_vacancies(md_quota_insert_cursor, row_dict, group_id)
    update_stage_raw(md_quota_update_cursor, row_dict)


def process_all(
    groups_dict,
    id_adm,
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
                row_dict = dict(zip(column_names, row))
                logger.info(f"Dados stage_raw: {row_dict}")
                process_row(
                    groups_dict,
                    row_dict,
                    md_quota_insert_cursor,
                    md_quota_update_cursor,
                    md_quota_select_cursor,
                    id_adm,
                )
                md_quota_connection.commit()
                logger.info("Transaction committed successfully!")
        except ProgrammingError as error:
            logger.error(f"Transação revertida devido a um erro:{error}")


def santander_vacancies_ingestion():
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()
    md_quota_insert_cursor = md_quota_connection.cursor()
    md_quota_update_cursor = md_quota_connection.cursor()
    md_quota_cursor = md_quota_connection.cursor()
    md_quota_select_cursor = md_quota_connection.cursor()

    # args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'JOB_NAME'])
    # workflow_name = args['WORKFLOW_NAME']
    # job_name = args['JOB_NAME']
    # event_trigger = EventTrigger(workflow_name, job_name)
    # event = event_trigger.get_event_details()
    # logger.info(f'event: {event}')
    try:
        groups_dict = fetch_md_quota_groups(md_quota_cursor)
        id_adm = fetch_administrator_id(md_quota_cursor)
        fetch_stage_raw_groups(md_quota_cursor)
        process_all(
            groups_dict,
            id_adm,
            md_quota_cursor,
            md_quota_update_cursor,
            md_quota_insert_cursor,
            md_quota_connection,
            md_quota_select_cursor,
        )
        md_quota_connection.commit()
        logger.info("Transaction committed successfully!")
    except Exception as error:
        logger.error(f"Error durante o processamento dos dados: {error}")
        raise error

    finally:
        md_quota_connection.close()
        logger.info("Conexão md_quota_connection finalizada")


if __name__ == "__main__":
    santander_vacancies_ingestion()
