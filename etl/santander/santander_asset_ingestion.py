from typing import Union

from bazartools.common.logger import get_logger
from bazartools.common.database.glueConnection import GlueConnection
from bazartools.common.database.assetCodeBuilder import build_asset_code
from psycopg2 import ProgrammingError

from bazartools.common.eventTrigger import EventTrigger
from awsglue.utils import getResolvedOptions
from datetime import date
from dateutil.relativedelta import relativedelta
from itertools import groupby
from operator import itemgetter
import sys

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


def fetch_stage_raw_groups(md_quota_cursor):
    try:
        query_select_stage_raw = """
            select 
            * 
            from 
            stage_raw.tb_grupos_santander_pre 
            where 
            is_processed is false
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
            """
        logger.info(f"query leitura adm md-cota: {query_select_adm}")
        md_quota_cursor.execute(query_select_adm)
        query_result_adm = md_quota_cursor.fetchall()
        adm_dict = get_table_dict(md_quota_cursor, query_result_adm)
        return adm_dict[0]["administrator_id"] if adm_dict else None
    except Exception as error:
        logger.error(f"Não foi possível obter o id da administradora, error:{error}")
        raise error


def insert_group(md_quota_cursor, row_group_code, id_adm):
    try:
        query_insert_group = """
            INSERT INTO
            md_cota.pl_group
            (
            group_code, administrator_id,
            created_at, modified_at,
            created_by,modified_by
            )
            VALUES
            (
            %s,%s,%s,%s,%s,%s
            )
            RETURNING GROUP_ID
            """
        md_quota_cursor.execute(
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
        group_inserted = md_quota_cursor.fetchall()
        return get_table_dict(md_quota_cursor, group_inserted)[0]["group_id"]
    except Exception as error:
        logger.error(f"Erro em insert_group: error{error}")
        raise error


def fetch_group_assets(md_quota_cursor, group_id):
    try:
        query_select_group_assets = """
            SELECT
            *
            FROM
            md_cota.pl_asset pa
            LEFT JOIN md_cota.pl_group pg ON pg.group_id = pa.group_id
            LEFT JOIN md_cota.pl_administrator pa2 ON pa2.administrator_id = pg.administrator_id
            WHERE
            pa.valid_to IS NULL
            AND
            pg.group_id = %s
            """
        md_quota_cursor.execute(query_select_group_assets, [group_id])
        query_result_group_assets = md_quota_cursor.fetchall()
        return get_table_dict(md_quota_cursor, query_result_group_assets)
    except Exception as error:
        logger.error(f"Erro em fetch_group_assets: error:{error}")
        raise error


def update_asset(md_quota_cursor, asset_id):
    try:
        query_update_asset = """
            UPDATE
            md_cota.pl_asset pa
            SET
            valid_to = %s,
            modified_at = %s,
            modified_by = %s
            WHERE
            asset_id = %s
            """
        md_quota_cursor.execute(
            query_update_asset, ("now()", "now()", GLUE_DEFAULT_CODE, asset_id)
        )
    except Exception as error:
        logger.error(f"Error em update_asset: {error}")
        raise error


def insert_asset(md_quota_cursor, row_dict, group_id, asset_type, md_quota_connection):
    try:
        query_insert_asset = """
            INSERT INTO
            md_cota.pl_asset
            (
            asset_desc,asset_code,asset_value,
            asset_type_id,info_date,valid_from,
            group_id,created_at,modified_at,
            created_by,modified_by
            )
            VALUES
            (
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s
            )
            RETURNING *
            """
        md_quota_cursor.execute(
            query_insert_asset,
            (
                row_dict["nm_bem"],
                build_asset_code(md_quota_connection),
                row_dict["vl_bem_atual"],
                asset_type,
                row_dict["data_info"],
                row_dict["data_info"],
                group_id,
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )
        asset_inserted = md_quota_cursor.fetchall()
        return get_table_dict(md_quota_cursor, asset_inserted)[0]["asset_id"]
    except Exception as error:
        logger.error(f"Error em insert_asset: error{error}")
        raise error


def update_stage_raw(md_quota_cursor, row_dict):
    try:
        query_update_stage_raw = """
            UPDATE
            stage_raw.tb_grupos_santander_pre
            SET
            is_processed = true
            WHERE
            id_grupos_santander = %s;
            """
        md_quota_cursor.execute(
            query_update_stage_raw, [row_dict["id_grupos_santander"]]
        )
    except Exception as e:
        logger.error(f"Erro em update_stage_raw: {e}")
        raise


def switch_asset_type(key: str, number: int) -> int:
    asset_type = {
        "AUTO": 2,
        "IMOVEL": 1,
        "SERVIC": 4,
        "CAMIN": 3,
        "MOTO": 4,
    }
    return asset_type.get(key, number)


def process_row(md_quota_cursor, row_dict, groups_dict, id_adm, md_quota_connection):
    try:
        row_group_code = cd_group_right_justified(row_dict["grupo"])
        group_id = ""

        for item in groups_dict:
            if item["group_code"] == row_group_code:
                logger.info(
                    f'group_code md-cota: {item["group_code"]}'
                    f"group_code stage_raw: {row_group_code}"
                )
                group_id = item["group_id"]
                break

        if not group_id:
            group_id = insert_group(md_quota_cursor, row_group_code, id_adm)
            groups_dict.append(
                {
                    "group_id": group_id,
                    "group_code": row_group_code,
                    "group_closing_date": None,
                }
            )

        group_assets_dict = fetch_group_assets(md_quota_cursor, group_id)

        new_asset_to_insert = False
        asset_type = switch_asset_type(row_dict["modalidade"], 7)

        if group_assets_dict:
            for asset in group_assets_dict:
                if asset["info_date"] < row_dict["data_info"]:
                    update_asset(md_quota_cursor, asset["asset_id"])
                    new_asset_to_insert = True

            if new_asset_to_insert:
                insert_asset(
                    md_quota_cursor, row_dict, group_id, asset_type, md_quota_connection
                )

        else:
            insert_asset(
                md_quota_cursor, row_dict, group_id, asset_type, md_quota_connection
            )

    except Exception as error:
        logger.error("Error ao processar linha:", error)
        raise error


def process_all(
    groups_dict, id_adm, md_quota_cursor, md_quota_connection, md_quota_query_cursor
):
    batch_counter = 0
    while True:
        try:
            batch_counter += 1
            rows = md_quota_cursor.fetchmany(size=BATCH_SIZE)
            if not rows:
                break
            column_names = [desc[0] for desc in md_quota_cursor.description]
            for row in rows:
                row_dict = dict(zip(column_names, row))
                process_row(
                    md_quota_query_cursor,
                    row_dict,
                    groups_dict,
                    id_adm,
                    md_quota_connection,
                )
                update_stage_raw(md_quota_query_cursor, row_dict)
            md_quota_connection.commit()
            logger.info("Transação realizada com sucesso!")
        except ProgrammingError as error:
            logger.error(f"Transação revertida devido a um erro:{error}")


def santander_asset_ingestion() -> None:
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()
    md_quota_query_cursor = md_quota_connection.cursor()
    md_quota_cursor = md_quota_connection.cursor()
    # args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'JOB_NAME'])
    # workflow_name = args['WORKFLOW_NAME']
    # job_name = args['JOB_NAME']
    # event_trigger = EventTrigger(workflow_name, job_name)
    # event = event_trigger.get_event_details()
    # logger.info(f'event: {event}')

    try:
        logger.info("Obtendo informações do banco de dados...")
        id_adm = fetch_administrator_id(md_quota_cursor)
        groups_dict = fetch_md_quota_groups(md_quota_cursor)
        fetch_stage_raw_groups(md_quota_cursor)
        process_all(
            groups_dict,
            id_adm,
            md_quota_cursor,
            md_quota_connection,
            md_quota_query_cursor,
        )
        logger.info("ETL finalizada com sucesso")
        md_quota_cursor.close()
        md_quota_connection.close()
    except Exception as error:
        raise error


if __name__ == "__main__":
    santander_asset_ingestion()
