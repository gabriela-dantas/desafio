from datetime import date

from bazartools.common.logger import get_logger
from bazartools.common.database.glueConnection import GlueConnection
import boto3
from botocore.exceptions import ClientError
import json

# from bazartools.common.eventTrigger import EventTrigger
from awsglue.utils import getResolvedOptions
import sys
# from datetime import date
from dateutil.relativedelta import relativedelta
from operator import itemgetter

BATCH_SIZE = 500
GLUE_DEFAULT_CODE = 2
logger = get_logger()


def get_table_dict(cursor, rows):
    column_names = [desc[0] for desc in cursor.description]

    # Create a list of dictionaries using list comprehension
    return [dict(zip(column_names, row)) for row in rows]


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
                pa.administrator_code = '0000000206'
                AND
                (pg.group_closing_date > now()
                OR
                pg.group_closing_date IS NULL)
                AND
                pg.is_deleted is false
            """

        md_quota_cursor.execute(query_select_groups_md_quota)
    except Exception as error:
        logger.error(f"Erro ao executar a consulta na pl_group: {error}")


def get_sorted_bids(md_quota_select_cursor, group_id):
    try:
        today = date.today()
        limit_date = today - relativedelta(months=6)

        query_select_bids = f"""
            SELECT 
            pb.value  
            FROM 
            md_cota.pl_bid pb 
            WHERE 
            pb.group_id = {group_id}
            AND 
            pb.info_date >= {"'" + str(limit_date) + "'"}
            AND 
            pb.value <> 0
            AND 
            pb.value IS NOT NULL
            AND
            pb.is_deleted is false
        """

        md_quota_select_cursor.execute(query_select_bids)
        query_result_bids_group = md_quota_select_cursor.fetchall()
        bids_group_dict = get_table_dict(
            md_quota_select_cursor, query_result_bids_group
        )

        sorted_bids = sorted(bids_group_dict, key=itemgetter("value"), reverse=True)

        return sorted_bids

    except Exception as error:
        logger.error(f"Error ao executar a consulta de lances: {error}")
        raise error


def update_group_info(md_quota_update_cursor, group_id, chosen_bid, embedded_bid):
    try:
        query_update_group = f"""
            UPDATE md_cota.pl_group pg
            SET
                chosen_bid = {chosen_bid},
                bid_calculation_date = NOW(),
                embedded_bid_percent = {embedded_bid},
                modified_at = NOW(),
                modified_by = {GLUE_DEFAULT_CODE}
            WHERE
                pg.group_id = {group_id}
        """
        md_quota_update_cursor.execute(query_update_group)
        logger.info(f"Query de atualização do grupo: {query_update_group}")
        logger.info("Grupos atualizados com informações de lances.")

    except Exception as error:
        print(f"Erro ao executar a atualização do grupo: {error}")
        raise error


def process_row(md_quota_select_cursor, md_quota_update_cursor, group_id, row_dict):
    sorted_bids = get_sorted_bids(md_quota_select_cursor, group_id)
    if sorted_bids:
        chosen_bid = sorted_bids[0]["value"]
        group_code = row_dict["group_code"]

        logger.info(f"Calculando lance embutido para o grupo {group_code}")

        embedded_bid = 0

        if group_code.startswith("IK") and group_code[1] == "I":
            embedded_bid = 30
        elif group_code.startswith("00I") or group_code.startswith("0VP"):
            embedded_bid = 30
        elif (
            group_code[1] == "A"
            and group_code[1:3] != "AF"
            and int(group_code[-3:]) >= 305
            and row_dict["group_deadline"] == 80
        ):
            embedded_bid = 20

        logger.info(f"Lance selecionado para o grupo {group_code} foi: {chosen_bid}")
        logger.info(
            f"Lance embutido selecionado para o grupo {group_code} foi: {embedded_bid}"
        )
        update_group_info(md_quota_update_cursor, group_id, chosen_bid, embedded_bid)


def process_all(md_quota_cursor, md_quota_select_cursor, md_quota_update_cursor):
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
                group_id = row_dict["group_id"]
                process_row(
                    md_quota_select_cursor, md_quota_update_cursor, group_id, row_dict
                )
        except Exception as error:
            logger.info(f"Erro ao processar dados, error:{error}")
            raise error


def put_event(event_bus_name):
    logger.info("Iniciando criação do evento")
    event_source = "glue"
    event_detail_type = "porto_quota_ingestion_batch_pricing"
    event_detail = {
        "quota_code_list": [],
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


def porto_bid_selector():
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()
    md_quota_select_cursor = md_quota_connection.cursor()
    md_quota_update_cursor = md_quota_connection.cursor()
    md_quota_cursor = md_quota_connection.cursor()
    args = getResolvedOptions(sys.argv, ["WORKFLOW_NAME", "JOB_NAME", "event_bus_name"])
    # workflow_name = args["WORKFLOW_NAME"]
    # job_name = args["JOB_NAME"]
    event_bus_name = args["event_bus_name"]
    # event_trigger = EventTrigger(workflow_name, job_name)
    # event = event_trigger.get_event_details()
    # logger.info(f"event: {event}")

    try:
        select_groups_md_quota(md_quota_cursor)
        process_all(md_quota_cursor, md_quota_select_cursor, md_quota_update_cursor)
        put_event(event_bus_name)
        logger.info("Processamento de dados finazalidado com sucesso")
    except Exception as error:
        logger.error(f"Error no processamento de dados: error:{error}")
        raise error
    finally:
        md_quota_connection.close()
        logger.info("Conexão finalizada")


if __name__ == "__main__":
    porto_bid_selector()
