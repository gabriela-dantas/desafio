from typing import Union

from bazartools.common.logger import get_logger
from bazartools.common.database.glueConnection import GlueConnection

# from bazartools.common.eventTrigger import EventTrigger
# from awsglue.utils import getResolvedOptions
from datetime import date
from dateutil.relativedelta import relativedelta
from itertools import groupby
from operator import itemgetter

# import sys

from psycopg2 import ProgrammingError

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


def fetch_stage_raw_groups(md_quota_cursor):
    try:
        query_select_stage_raw = """
        SELECT 
        *
        FROM 
        stage_raw.tb_lances_santander_pre
        WHERE 
        is_processed = false;
        """
        logger.info(f"query leitura stage_raw: {query_select_stage_raw}")
        md_quota_cursor.execute(query_select_stage_raw)
    except Exception as error:
        logger.error(
            f"Error na busca de dados na tabela tb_lances_santander_pre, error:{error}"
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
        md_quota_cursor.execute(query_select_adm)
        query_result_adm = md_quota_cursor.fetchall()
        adm_dict = get_table_dict(md_quota_cursor, query_result_adm)
        return adm_dict[0]["administrator_id"] if adm_dict else None
    except Exception as error:
        logger.error(f"Não foi possível obter o id da administradora, error:{error}")
        raise error


def fetch_select_free_bid_type(md_quota_cursor) -> Union[None, str]:
    try:
        query_select_free_bid_type = """
           SELECT
                pbt.bid_type_id
            FROM 
                md_cota.pl_bid_type pbt
            WHERE 
                pbt.bid_type_desc = 'FREE BID'
            """
        md_quota_cursor.execute(query_select_free_bid_type)
        query_result_free_bid = md_quota_cursor.fetchall()
        free_bid_dict = get_table_dict(md_quota_cursor, query_result_free_bid)
        return free_bid_dict[0]["bid_type_id"]
    except Exception as error:
        logger.error(f"Não foi possível obter o bid_type_id, error:{error}")
        raise error


def fetch_select_bid_value_type_id(md_quota_cursor) -> Union[None, str]:
    try:
        query_select_bid_value_type_id = """
            SELECT
                pbvt.bid_value_type_id
            FROM 
                md_cota.pl_bid_value_type pbvt
            WHERE 
                pbvt.bid_value_type_desc = 'WINNING BID'
            """
        md_quota_cursor.execute(query_select_bid_value_type_id)
        query_result_bid_value_type = md_quota_cursor.fetchall()
        bid_value_type_dict = get_table_dict(
            md_quota_cursor, query_result_bid_value_type
        )
        return bid_value_type_dict[0]["bid_value_type_id"]
    except Exception as error:
        logger.error(f"Não foi possível obter o bid_value_type_id, error:{error}")
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


def insert_bid_md_quota(
    md_quota_insert_cursor, row_dict, group_id, row_bid_type, bid_value_type_id
):
    try:
        query_insert_bid_md_quota = """
            INSERT
            INTO
            md_cota.pl_bid
            (
            value,
            assembly_date,
            info_date,
            group_id,
            bid_type_id,
            bid_value_type_id,
            created_at,
            modified_at,
            created_by,
            modified_by
            )
            VALUES
            (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
            RETURNING BID_ID
            """
        md_quota_insert_cursor.execute(
            query_insert_bid_md_quota,
            (
                row_dict["menor_lance"],
                ("'" + str(row_dict["dt_contmp"]) + "'"),
                ("'" + str(row_dict["data_info"]) + "'"),
                group_id,
                row_bid_type,
                bid_value_type_id,
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )
        bid_inserted = md_quota_insert_cursor.fetchall()
        bid_id = get_table_dict(md_quota_insert_cursor, bid_inserted)[0]["bid_id"]
        logger.info(f"bid_id inserido {bid_id}")
        # inserindo lance médio
        md_quota_insert_cursor.execute(
            query_insert_bid_md_quota,
            (
                row_dict["medio_lance"],
                ("'" + str(row_dict["dt_contmp"]) + "'"),
                ("'" + str(row_dict["data_info"]) + "'"),
                group_id,
                row_bid_type,
                bid_value_type_id,
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )
        bid_inserted = md_quota_insert_cursor.fetchall()
        bid_id = get_table_dict(md_quota_insert_cursor, bid_inserted)[0]["bid_id"]
        logger.info(f"bid_id inserido {bid_id}")

        # inserindo lance de maior valor
        md_quota_insert_cursor.execute(
            query_insert_bid_md_quota,
            (
                row_dict["maior_lance"],
                ("'" + str(row_dict["dt_contmp"]) + "'"),
                ("'" + str(row_dict["data_info"]) + "'"),
                group_id,
                row_bid_type,
                bid_value_type_id,
                "now()",
                "now()",
                GLUE_DEFAULT_CODE,
                GLUE_DEFAULT_CODE,
            ),
        )
        bid_inserted = md_quota_insert_cursor.fetchall()
        bid_id = get_table_dict(md_quota_insert_cursor, bid_inserted)[0]["bid_id"]
        logger.info(f"bid_id inserido {bid_id}")
    except Exception as error:
        logger.error(f"Erro ao inserir bid na tabela md_cota.pl_bid: {error}")
        raise error


def update_stage_raw(md_quota_update_cursor, row_dict):
    try:
        query_update_stage_raw = f"""
            UPDATE stage_raw.tb_lances_santander_pre 
            SET is_processed = true
            WHERE 
            id_lances_santader = {row_dict['id_lances_santader']};
            """
        logger.info(f"query de atualização: {query_update_stage_raw}")
        md_quota_update_cursor.execute(query_update_stage_raw)
    except Exception as error:
        logger.error(f"Erro ao executar a query de atualização: {error}")
        raise error


def calculate_max_bid_perc(group, today):
    if group["group_closing_date"] is not None:
        delta = relativedelta(group["group_closing_date"], today)
        months_to_end_group = delta.months + delta.years * 12
        return (months_to_end_group / group["group_deadline"]) * 100
    return 0


def query_select_bids_group(md_quota_cursor, group, limit_date):
    try:
        select_bids_group = f"""
                SELECT
                    pb.value,
                    pb.assembly_date
                FROM
                    md_cota.pl_bid pb
                WHERE
                    pb.group_id = {group['group_id']}
                    AND pb.assembly_date > '{limit_date}'
                    AND pb.value <> 0
                GROUP BY
                    pb.assembly_date,
                    pb.value
            """
        md_quota_cursor.execute(select_bids_group)
    except Exception as error:
        logger.error(error)
        raise error


def process_bids_group_dict(bids_group_dict, max_bid_perc):
    bids_group_dict = sorted(bids_group_dict, key=itemgetter("assembly_date"))
    average_bids = []

    for key, value in groupby(bids_group_dict, key=itemgetter("assembly_date")):
        bids_qtd = 0
        total_value = 0

        for item in list(value):
            bids_qtd += 1
            total_value += item["value"]

        average_bid = total_value / bids_qtd
        average_bids.append(average_bid)

    bids_greater_than_max_bid = sum(1 for bid in average_bids if bid >= max_bid_perc)

    return average_bids, bids_greater_than_max_bid


def update_group_info(
    md_quota_cursor, group, chosen_bid, bids_greater_than_max_occurrence
):
    try:
        query_update_group = f"""
                UPDATE
                    md_cota.pl_group pg
                SET
                    chosen_bid = {chosen_bid},
                    max_bid_occurrences_perc = {bids_greater_than_max_occurrence},
                    bid_calculation_date = NOW(),
                    modified_at = NOW(),
                    modified_by = {GLUE_DEFAULT_CODE}
                WHERE
                    pg.group_id = {group['group_id']}
            """
        md_quota_cursor.execute(query_update_group)
        logger.info(f"query atualização grupo: {query_update_group}")
        logger.info("Grupos atualizados com infos de lances.")
    except Exception as error:
        logger.error(error)
        raise error


def group_bid_info(groups, md_quota_cursor):
    logger.info("Iniciando processamento de lances para atualizar infos de grupos.")
    try:
        today = date.today()
        for group in groups:
            limit_date = today - relativedelta(months=6)
            max_bid_perc = calculate_max_bid_perc(group, today)

            query_select_bids_group(md_quota_cursor, group, limit_date)
            query_result_bids_group = md_quota_cursor.fetchall()
            bids_group_dict = get_table_dict(md_quota_cursor, query_result_bids_group)

            if bids_group_dict is not None:
                average_bids, bids_greater_than_max_bid = process_bids_group_dict(
                    bids_group_dict, max_bid_perc
                )

                if len(average_bids) > 0:
                    chosen_bid = max(average_bids)
                    bids_greater_than_max_occurrence = (
                        bids_greater_than_max_bid / len(average_bids)
                    ) * 100
                    update_group_info(
                        md_quota_cursor,
                        group,
                        chosen_bid,
                        bids_greater_than_max_occurrence,
                    )

    except Exception as error:
        logger.error(f"Erro ao processar informações de lances para grupos: {error}")
        raise error


def process_row(
    row_dict,
    groups_dict,
    md_quota_update_cursor,
    id_adm,
    free_bid_id,
    md_quota_insert_cursor,
    bid_value_type_id,
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
        group_id = insert_group(md_quota_insert_cursor, row_group_code, id_adm)
        logger.info(f"group_id inserido {group_id}")
        groups_dict.append(
            {
                "group_id": group_id,
                "group_code": row_group_code,
                "group_closing_date": None,
            }
        )
    row_bid_type = free_bid_id
    insert_bid_md_quota(
        md_quota_insert_cursor, row_dict, group_id, row_bid_type, bid_value_type_id
    )
    update_stage_raw(md_quota_update_cursor, row_dict)


def process_all(
    bid_value_type_id,
    free_bid_id,
    id_adm,
    groups_dict,
    md_quota_cursor,
    md_quota_insert_cursor,
    md_quota_update_cursor,
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
                    groups_dict,
                    md_quota_update_cursor,
                    id_adm,
                    free_bid_id,
                    md_quota_insert_cursor,
                    bid_value_type_id,
                )
            group_bid_info(groups_dict, md_quota_update_cursor)
            md_quota_connection.commit()
            logger.info("Transação realizada com sucesso!")
        except ProgrammingError as error:
            logger.error(f"Transação revertida devido a um erro:{error}")


def santander_bid_ingestion():
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()
    md_quota_insert_cursor = md_quota_connection.cursor()
    md_quota_update_cursor = md_quota_connection.cursor()
    md_quota_cursor = md_quota_connection.cursor()
    # args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'JOB_NAME'])
    # workflow_name = args['WORKFLOW_NAME']
    # job_name = args['JOB_NAME']
    # event_trigger = EventTrigger(workflow_name, job_name)
    # event = event_trigger.get_event_details()
    # logger.info(f'event: {event}')

    try:
        bid_value_type_id = fetch_select_bid_value_type_id(md_quota_cursor)
        free_bid_id = fetch_select_free_bid_type(md_quota_cursor)
        id_adm = fetch_administrator_id(md_quota_cursor)
        groups_dict = fetch_md_quota_groups(md_quota_cursor)
        fetch_stage_raw_groups(md_quota_cursor)
        process_all(
            bid_value_type_id,
            free_bid_id,
            id_adm,
            groups_dict,
            md_quota_cursor,
            md_quota_insert_cursor,
            md_quota_update_cursor,
            md_quota_connection,
        )
        logger.info("ETL finalizada com sucesso")
        md_quota_cursor.close()
        md_quota_connection.close()
    except Exception as error:
        raise error


if __name__ == "__main__":
    santander_bid_ingestion()
