from bazartools.common.logger import get_logger
from bazartools.common.database.glueConnection import GlueConnection
from awsglue.utils import getResolvedOptions
from datetime import date
from dateutil.relativedelta import relativedelta
from itertools import groupby
from operator import itemgetter
import sys

# import os

BATCH_SIZE = 500
GLUE_DEFAULT_CODE = 2
logger = get_logger()


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


def calculate_max_bid_perc(group):
    today = date.today()
    if group["group_closing_date"] is not None:
        delta = relativedelta(group["group_closing_date"], today)
        months_to_end_group = delta.months + delta.years * 12
        return (months_to_end_group / group["group_deadline"]) * 100
    return 0


def fetch_bids_group(md_quota_cursor, group, limit_date):
    try:
        query_select_bids_group = f"""
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
        md_quota_cursor.execute(query_select_bids_group)
        return md_quota_cursor.fetchall()
    except Exception as error:
        logger.error(f"Erro ao fazer select na pl_bid, error:{error}")
        raise error


def process_bids_group(
    bids_group_dict, max_bid_perc, max_assembly_number, min_assembly_number
):
    bids_group_dict = sorted(bids_group_dict, key=itemgetter("assembly_date"))
    average_bids = []
    bids_greater_then_max_bid = 0

    for key, value in groupby(bids_group_dict, key=itemgetter("assembly_date")):
        bids_qtd = 0
        total_value = 0
        for item in list(value):
            bids_qtd += 1
            total_value += item["value"]
        average_bid = total_value / bids_qtd
        average_bids.append(average_bid)

    for bid in average_bids:
        if bid >= max_bid_perc:
            bids_greater_then_max_bid += 1

    if len(average_bids) > max_assembly_number:
        average_bids = average_bids[:5]

    if len(average_bids) > min_assembly_number:
        return max(average_bids), bids_greater_then_max_bid / len(average_bids) * 100
    else:
        return None, None


def update_group_info(
    md_quota_cursor, group, chosen_bid, bids_greater_then_max_occurrence
):
    try:
        query_update_group = f"""
            UPDATE md_cota.pl_group pg
            SET
                chosen_bid = {chosen_bid},
                max_bid_occurrences_perc = {bids_greater_then_max_occurrence},
                bid_calculation_date = NOW(),
                embedded_bid_percent = 10,
                modified_at = NOW(),
                modified_by = {GLUE_DEFAULT_CODE}
            WHERE
                pg.group_id = {group['group_id']}
        """
        md_quota_cursor.execute(query_update_group)
    except Exception as error:
        logger.error(f"Erro ao fazer update na pl_group, error:{error}")
        raise error


def reset_group_info(md_quota_cursor, group):
    try:
        query_update_group = f"""
            UPDATE md_cota.pl_group pg
            SET
                chosen_bid = NULL,
                max_bid_occurrences_perc = NULL,
                bid_calculation_date = NULL,
                embedded_bid_percent = NULL,
                modified_at = NOW(),
                modified_by = {GLUE_DEFAULT_CODE}
            WHERE
                pg.group_id = {group['group_id']}
        """
        md_quota_cursor.execute(query_update_group)
    except Exception as error:
        logger.error(f"Erro ao fazer update na pl_group, error:{error}")
        raise error


def group_bid_info(
    groups, md_quota_cursor, min_assembly_date, min_assembly_number, max_assembly_number
):
    logger.info("Iniciando processamento de lances para atualizar infos de grupos.")
    for group in groups:
        try:
            limit_date = date.today() - relativedelta(months=min_assembly_date)
            max_bid_perc = calculate_max_bid_perc(group)
            bids_group_dict = fetch_bids_group(md_quota_cursor, group, limit_date)

            if bids_group_dict:
                chosen_bid, bids_greater_then_max_occurrence = process_bids_group(
                    bids_group_dict,
                    max_bid_perc,
                    max_assembly_number,
                    min_assembly_number,
                )

                if chosen_bid is not None:
                    update_group_info(
                        md_quota_cursor,
                        group,
                        chosen_bid,
                        bids_greater_then_max_occurrence,
                    )
                else:
                    reset_group_info(md_quota_cursor, group)
            else:
                reset_group_info(md_quota_cursor, group)

        except Exception as error:
            logger.error(f"Erro ao processar grupo {group['group_id']}: {error}")
            raise error


def select_stage_raw(md_quota_cursor):
    try:
        query_select_stage_raw = """
            SELECT * FROM stage_raw.tb_contemplados_itau tci
            WHERE tci.is_processed IS FALSE
        """
        logger.info(f"Query leitura stage_raw: {query_select_stage_raw}")
        md_quota_cursor.execute(query_select_stage_raw)
    except Exception as error:
        logger.error(f"Erro ao obter dados da tb_contemplados, error:{error}")
        raise error


def select_groups_md_quota(md_quota_cursor):
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
                pa.administrator_desc = 'ITAÚ ADM DE CONSÓRCIOS LTDA'
            AND
                pg.is_deleted is false
        """
        logger.info(f"Query leitura grupos md-cota: {query_select_groups_md_quota}")
        md_quota_cursor.execute(query_select_groups_md_quota)
        query_result_groups = md_quota_cursor.fetchall()
        return get_table_dict(md_quota_cursor, query_result_groups)
    except Exception as error:
        logger.error(f"Erro ao obter dados da pl_group, error:{error}")
        raise error


def select_adm(md_quota_cursor):
    try:
        query_select_adm = """
            SELECT pa.administrator_id
            FROM md_cota.pl_administrator pa
            WHERE pa.administrator_desc = 'ITAÚ ADM DE CONSÓRCIOS LTDA'
        """
        logger.info(f"Query leitura adm md-cota: {query_select_adm}")
        md_quota_cursor.execute(query_select_adm)
        query_result_adm = md_quota_cursor.fetchall()
        adm_dict = get_table_dict(md_quota_cursor, query_result_adm)
        return adm_dict[0]["administrator_id"]
    except Exception as error:
        logger.error(f"Erro ao obter dados id da adm, error:{error}")
        raise error


def select_free_bid_type(md_quota_cursor):
    try:
        query_select_free_bid_type = """
            SELECT pbt.bid_type_id
            FROM md_cota.pl_bid_type pbt
            WHERE pbt.bid_type_desc = 'FREE BID'
        """
        md_quota_cursor.execute(query_select_free_bid_type)
        query_result_free_bid = md_quota_cursor.fetchall()
        free_bid_dict = get_table_dict(md_quota_cursor, query_result_free_bid)
        return free_bid_dict[0]["bid_type_id"]
    except Exception as error:
        logger.error(f"Erro ao obter dados do type_id, error:{error}")
        raise error


def select_draw_bid_type(md_quota_cursor):
    try:
        query_select_draw_bid_type = """
            SELECT pbt.bid_type_id
            FROM md_cota.pl_bid_type pbt
            WHERE pbt.bid_type_desc = 'DRAW'
        """
        md_quota_cursor.execute(query_select_draw_bid_type)
        query_result_draw_bid = md_quota_cursor.fetchall()
        draw_bid_dict = get_table_dict(md_quota_cursor, query_result_draw_bid)
        return draw_bid_dict[0]["bid_type_id"]
    except Exception as error:
        logger.error(f"Erro ao obter dados do bid_type_id, error:{error}")
        raise error


def select_bid_value_type_id(md_quota_cursor):
    try:
        query_select_bid_value_type_id = """
            SELECT pbvt.bid_value_type_id
            FROM md_cota.pl_bid_value_type pbvt
            WHERE pbvt.bid_value_type_desc = 'WINNING BID'
        """
        md_quota_cursor.execute(query_select_bid_value_type_id)
        query_result_bid_value_type = md_quota_cursor.fetchall()
        bid_value_type_dict = get_table_dict(
            md_quota_cursor, query_result_bid_value_type
        )
        return bid_value_type_dict[0]["bid_value_type_id"]

    except Exception as error:
        logger.error(f"Erro ao obter dados do bid_value_type_id, error:{error}")
        raise error


def find_group_id(groups_dict, row_group_code, row_dict):
    for item in groups_dict:
        if (
            item["group_code"] == row_group_code
            or item["group_code"] == row_dict["cd_grupo"]
        ):
            logger.info(f'group_code md-cota: {item["group_code"]}')
            logger.info(f"group_code stage_raw: {row_group_code}")
            logger.info("grupo encontrado")
            return item["group_id"]
    return None


def insert_group(md_quota_insert_cursor, id_adm, row_group_code, row_dict):
    try:
        query_insert_group = f"""
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
                {row_group_code},
                {row_dict['pz_cota']},
                {id_adm},
                now(),
                now(),
                {GLUE_DEFAULT_CODE},
                {GLUE_DEFAULT_CODE}
            )
            RETURNING GROUP_ID
        """

        md_quota_insert_cursor.execute(query_insert_group)
        group_inserted = md_quota_insert_cursor.fetchall()
        group_id = get_table_dict(md_quota_insert_cursor, group_inserted)[0]["group_id"]
        logger.info(f"group_id inserido {group_id}")
        return group_id
    except Exception as error:
        logger.error(f"Erro ao obter ao inserir grupos, error:{error}")
        raise error


def set_bid_type(row_dict, free_bid_id, draw_bid_id):
    if row_dict["st_modalidade"] == "Lance Livre":
        return free_bid_id
    elif row_dict["st_modalidade"] == "Sorteio":
        return draw_bid_id


def insert_bid_md_cota(
    md_cota_insert_cursor, row_dict, group_id, bid_type_id, bid_value_type_id
):
    try:
        query_insert_bid_md_cota = f"""
            INSERT INTO md_cota.pl_bid
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
                {row_dict['pe_lance']},
                {"'" + str(row_dict['dt_contemplacao']) + "'"},
                {"'" + str(row_dict['data_info']) + "'"},
                {group_id},
                {bid_type_id},
                {bid_value_type_id},
                now(),
                now(),
                {GLUE_DEFAULT_CODE},
                {GLUE_DEFAULT_CODE}
            )
            RETURNING BID_ID
        """

        md_cota_insert_cursor.execute(query_insert_bid_md_cota)
        bid_inserted = md_cota_insert_cursor.fetchall()
        bid_id = get_table_dict(md_cota_insert_cursor, bid_inserted)[0]["bid_id"]
        logger.info(f"bid_id inserido {bid_id}")
        return bid_id
    except Exception as error:
        logger.error(f"Erro ao inserir bid, error:{error}")
        raise error


def update_stage_raw(md_cota_update_cursor, row_dict):
    try:
        query_update_stage_raw = f"""
            UPDATE stage_raw.tb_contemplados_itau tci
            SET is_processed = true
            WHERE tci.id_contemplados_itau = {row_dict['id_contemplados_itau']};
        """
        logger.info(f"query de atualização: {query_update_stage_raw}")
        md_cota_update_cursor.execute(query_update_stage_raw)

    except Exception as error:
        logger.error(f"Erro ao fazer update stage_raw, error:{error}")
        raise error


def process_row(
    row_dict,
    groups_dict,
    md_quota_insert_cursor,
    md_quota_update_cursor,
    id_adm,
    free_bid_id,
    draw_bid_id,
    bid_value_type_id,
):
    row_group_code = cd_grupo_right_justified(row_dict["cd_grupo"])
    group_id = find_group_id(groups_dict, row_group_code, row_dict)

    if group_id is None:
        group_id = insert_group(
            md_quota_insert_cursor, id_adm, row_group_code, row_dict
        )
        groups_dict.append(
            {
                "group_id": group_id,
                "group_code": row_group_code,
                "group_closing_date": None,
            }
        )

    bid_type_id = set_bid_type(row_dict, free_bid_id, draw_bid_id)
    insert_bid_md_cota(
        md_quota_insert_cursor, row_dict, group_id, bid_type_id, bid_value_type_id
    )

    update_stage_raw(md_quota_update_cursor, row_dict)


def process_all(
    groups_dict,
    md_quota_insert_cursor,
    md_quota_update_cursor,
    id_adm,
    free_bid_id,
    draw_bid_id,
    bid_value_type_id,
    md_quota_cursor,
    md_quota_connection,
    min_assembly_date,
    min_assembly_number,
    max_assembly_number,
):
    batch_counter = 0

    while True:
        batch_counter += 1
        rows = md_quota_cursor.fetchmany(size=BATCH_SIZE)  # Fetch XXX rows at a time
        column_names = [desc[0] for desc in md_quota_cursor.description]
        if not rows:
            break
        logger.info(f"Fetch {BATCH_SIZE} rows at a time - Batch number {batch_counter}")
        for row in rows:
            row_dict = dict(zip(column_names, row))
            process_row(
                row_dict,
                groups_dict,
                md_quota_insert_cursor,
                md_quota_update_cursor,
                id_adm,
                free_bid_id,
                draw_bid_id,
                bid_value_type_id,
            )
        # Commit the transaction
        md_quota_connection.commit()
        logger.info("Transaction committed successfully!")

    group_bid_info(
        groups_dict,
        md_quota_update_cursor,
        min_assembly_date,
        min_assembly_number,
        max_assembly_number,
    )

    # Commit the transaction
    md_quota_connection.commit()
    logger.info("Transaction committed successfully!")


def bid_ingestion():
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()
    md_quota_insert_cursor = md_quota_connection.cursor()
    md_quota_update_cursor = md_quota_connection.cursor()
    md_quota_cursor = md_quota_connection.cursor()

    # min_assembly_date = int(os.environ.get("min_assembly_date"))
    # min_assembly_number = int(os.environ.get("min_assembly_number"))
    # max_assembly_number = int(os.environ.get("max_assembly_number"))

    args = getResolvedOptions(
        sys.argv,
        [
            "WORKFLOW_NAME",
            "JOB_NAME",
            "min_assembly_date",
            "min_assembly_number",
            "max_assembly_number",
        ],
    )
    min_assembly_date = int(args["min_assembly_date"])
    min_assembly_number = int(args["min_assembly_number"])
    max_assembly_number = int(args["max_assembly_number"])

    try:
        bid_value_type_id = select_bid_value_type_id(md_quota_cursor)
        draw_bid_id = select_draw_bid_type(md_quota_cursor)
        free_bid_dict = select_free_bid_type(md_quota_cursor)
        id_adm = select_adm(md_quota_cursor)
        groups_dict = select_groups_md_quota(md_quota_cursor)
        select_stage_raw(md_quota_cursor)

        process_all(
            groups_dict,
            md_quota_insert_cursor,
            md_quota_update_cursor,
            id_adm,
            free_bid_dict,
            draw_bid_id,
            bid_value_type_id,
            md_quota_cursor,
            md_quota_connection,
            min_assembly_date,
            min_assembly_number,
            max_assembly_number,
        )
        logger.info("Dados processados com sucesso.")
    except Exception as e:
        # Rollback the transaction in case of an error
        md_quota_connection.rollback()
        logger.error("Transaction rolled back due to an error:", e)

    finally:
        # Close the cursor and connection
        md_quota_cursor.close()
        md_quota_connection.close()
        logger.info("Connection closed.")


if __name__ == '__main__':
    bid_ingestion()
