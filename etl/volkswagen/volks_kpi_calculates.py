from datetime import datetime
from bazartools.common.logger import get_logger
from bazartools.common.database.glueConnection import GlueConnection
import psycopg2.extensions as psycopg2
import boto3

logger = get_logger()


def get_table_dict(cursor: psycopg2.cursor, rows: list) -> list:
    column_names = [desc[0] for desc in cursor.description]

    # Create a list of dictionaries using list comprehension
    return [dict(zip(column_names, row)) for row in rows]


def get_dict_by_id(id_item: str, data_list: list, field_name: str) -> dict:
    filtered_items = filter(lambda item: item[field_name] == id_item, data_list)
    return next(filtered_items, None)


def select_new_quotas_md_quota(md_quota_cursor: psycopg2.cursor) -> int:
    try:
        query_select_new_quotas_md_quota = (
            """
            SELECT 
            TO_CHAR(quotas.MIN_CREATED_AT - interval '3 hour', 'YYYY-MM-DD') as calculate_date, 
            count(*) as new_quotas 
            FROM
                (SELECT
                    pq.external_reference AS REFERENCEID,
                    pq.created_at AS MIN_CREATED_AT
                FROM
                    md_cota.pl_quota pq 
                LEFT JOIN md_cota.pl_administrator pa on pa.administrator_id = pq.administrator_id 
                WHERE
                    pa.administrator_code = '0000000289'
                    AND pq.CREATED_AT >= (CURRENT_DATE - interval '24 hour')::timestamp) as quotas
            group by TO_CHAR(quotas.MIN_CREATED_AT - interval '3 hour', 'YYYY-MM-DD')
            order by 1 desc limit 7;
        """)
        md_quota_cursor.execute(query_select_new_quotas_md_quota)
        query_result_new_quotas = md_quota_cursor.fetchall()
        return get_table_dict(md_quota_cursor, query_result_new_quotas)[0]['new_quotas'] if len(query_result_new_quotas) > 0 else 0
    except Exception as error:
        logger.error(f"Erro ao obter contagem novas cotas Volkswagen md-cota, error:{error}")
        raise error


def select_updated_quotas_md_quota(md_quota_cursor: psycopg2.cursor) -> int:
    try:
        query_select_updated_quotas_md_quota = (
            """
            SELECT 
            TO_CHAR(quotas.MIN_CREATED_AT - interval '3 hour', 'YYYY-MM-DD') as calculate_date, 
            count(*) as updated_quotas  
            FROM
                (SELECT
                    pq.external_reference AS REFERENCEID,
                    pq.CREATED_AT AS MIN_CREATED_AT,
                    pq.modified_at
                FROM
                    md_cota.pl_quota pq 
                LEFT JOIN md_cota.pl_administrator pa on pa.administrator_id = pq.administrator_id 
                WHERE
                    pa.administrator_code = '0000000289'
                    AND pq.CREATED_AT >= (CURRENT_DATE - interval '24 hour')::timestamp
                    and (pq.modified_at - interval '1 minute') > pq.created_at) as quotas
            group by TO_CHAR(quotas.MIN_CREATED_AT - interval '3 hour', 'YYYY-MM-DD')
            order by 1 desc limit 1;
        """)
        md_quota_cursor.execute(query_select_updated_quotas_md_quota)
        query_result_updated_quotas = md_quota_cursor.fetchall()
        return get_table_dict(md_quota_cursor, query_result_updated_quotas)['updated_quotas'] if len(query_result_updated_quotas) > 0 else 0
    except Exception as error:
        logger.error(f"Erro ao obter contagem cotas atualizadas Volkswagen md-cota, error:{error}")
        raise error


def select_adm_origin_quotas_md_quota(md_quota_cursor: psycopg2.cursor) -> int:
    try:
        query_select_new_adm_origin_quotas_md_quota = (
            """
            SELECT 
            TO_CHAR(quotas.MIN_CREATED_AT - interval '3 hour', 'YYYY-MM-DD') as calculate_date, 
            count(*) as new_auction_quotas 
            FROM
                (SELECT
                    pq.external_reference AS REFERENCEID,
                    pq.CREATED_AT AS MIN_CREATED_AT 
                FROM
                    md_cota.pl_quota pq 
                LEFT JOIN md_cota.pl_administrator pa on pa.administrator_id = pq.administrator_id
                WHERE
                    pa.administrator_code = '0000000289'
                    AND pq.CREATED_AT >= (CURRENT_DATE - interval '50 days')::timestamp
                    AND pq.quota_origin_id = 1) as quotas
            group BY TO_CHAR(quotas.MIN_CREATED_AT - interval '3 hour', 'YYYY-MM-DD')
            order by 1 desc limit 7;
        """)
        md_quota_cursor.execute(query_select_new_adm_origin_quotas_md_quota)
        query_result_new_adm_origin_quotas = md_quota_cursor.fetchall()
        return get_table_dict(md_quota_cursor, query_result_new_adm_origin_quotas)[0]['new_auction_quotas'] if len(query_result_new_adm_origin_quotas) > 0 else 0
    except Exception as error:
        logger.error(f"Erro ao obter contagem cotas ganhadoras leilão Volkswagen md-cota, error:{error}")
        raise error


def select_open_sea_quotas_md_quota(md_quota_cursor: psycopg2.cursor) -> int:
    try:
        query_select_new_open_sea_quotas_md_quota = (
            """
            SELECT 
            TO_CHAR(quotas.MIN_CREATED_AT - interval '3 hour', 'YYYY-MM-DD') as calculate_date, 
            count(*) as new_no_auction_quotas
            FROM
                (SELECT
                    pq.external_reference AS REFERENCEID,
                    pq.CREATED_AT AS MIN_CREATED_AT
                FROM
                    md_cota.pl_quota pq 
                LEFT JOIN md_cota.pl_administrator pa on pa.administrator_id = pq.administrator_id 
                WHERE
                    pa.administrator_code = '0000000289'
                    AND pq.CREATED_AT >= (CURRENT_DATE - interval '50 days')::timestamp
                    and pq.quota_origin_id = 2) as quotas
            GROUP BY TO_CHAR(quotas.MIN_CREATED_AT - interval '3 hour', 'YYYY-MM-DD')
            order by 1 desc limit 7;
        """)
        md_quota_cursor.execute(query_select_new_open_sea_quotas_md_quota)
        query_result_new_open_sea_quotas = md_quota_cursor.fetchall()
        return get_table_dict(md_quota_cursor, query_result_new_open_sea_quotas)[0]['new_no_auction_quotas'] if \
            len(query_result_new_open_sea_quotas) > 0 else 0
    except Exception as error:
        logger.error(f"Erro ao obter contagem novas cotas não leilão Volkswagen md-cota, error:{error}")
        raise error


def select_group_end_date_inconsistency(md_quota_cursor: psycopg2.cursor) -> dict:
    try:
        query_select_group_end_date_inconsistency = (
            """
            SELECT 
            count(*) as total_inconsistencies
            FROM
            (SELECT 
            distinct on (pg.group_code)
            pg.group_code,
            pg.group_closing_date,
            pq.acquisition_date,
            pq.total_installments,
            pg.group_deadline,
            EXTRACT(MONTH FROM group_closing_date - acquisition_date) AS diff_in_months
            FROM 
            md_cota.pl_group pg 
            LEFT JOIN md_cota.pl_administrator pa on pa.administrator_id = pg.administrator_id 
            LEFT JOIN md_cota.pl_quota pq on pq.group_id = pg.group_id 
            WHERE
            pa.administrator_code = '0000000289'
            AND 
            pg.group_closing_date is not null ) as grupos
            WHERE 
            grupos.diff_in_months > 0
        """)
        md_quota_cursor.execute(query_select_group_end_date_inconsistency)
        query_result_group_end_date_inconsistency = md_quota_cursor.fetchall()
        return get_table_dict(md_quota_cursor, query_result_group_end_date_inconsistency)[0]
    except Exception as error:
        logger.error(f"Erro ao obter contagem de inconsistências para data de "
                     f"encerramento de grupos Volkswagen md-cota, error:{error}")
        raise error


def select_cubees_new_customers(cubees_cursor: psycopg2.cursor) -> dict:
    try:
        query_select_cubees_new_customers = (
            """
            SELECT 
            count(*) as new_customers
            FROM 
            (SELECT
                cp.person_code  AS PERSON_CODE,
                cp.created_at AS MIN_CREATED_AT,
                cp.is_deleted,
                cp.anonymize
            FROM
                cubees.cb_person cp 
            WHERE
            cp.CREATED_AT >= (CURRENT_DATE)::timestamp
            AND
            cp.is_deleted is false 
            AND 
            cp.anonymize is false) as persons
            order by 1 desc limit 1;
        """)
        cubees_cursor.execute(query_select_cubees_new_customers)
        query_result_cubees_new_customers = cubees_cursor.fetchall()
        return get_table_dict(cubees_cursor, query_result_cubees_new_customers)[0]
    except Exception as error:
        logger.error(f"Erro ao obter contagem de novos clientes no cubees, error:{error}")
        raise error


def select_bpm_new_cards(bpm_cursor: psycopg2.cursor) -> int:
    try:
        query_select_bpm_new_cards = (
            """
            SELECT
            TO_CHAR(quotas.MIN_CREATED_AT , 'YYYY-MM-DD') as calculate_date, 
            count(*) as new_cards
            FROM
                (SELECT
                     external_reference AS external_reference,
                    MIN(created_at) AS MIN_CREATED_AT
                FROM
                    quotas
                WHERE
                    created_at >= (CURRENT_DATE)::timestamp
                    AND created_at < (CURRENT_DATE)::timestamp
                    AND manager_id = 7
                GROUP BY
                    external_reference) as quotas
            group by TO_CHAR(quotas.MIN_CREATED_AT, 'YYYY-MM-DD')
            order by 1 desc limit 1;
        """)
        bpm_cursor.execute(query_select_bpm_new_cards)
        query_result_bpm_new_cards = bpm_cursor.fetchall()
        return get_table_dict(bpm_cursor, query_result_bpm_new_cards)[0]['new_cards'] if \
            len(query_result_bpm_new_cards) > 0 else 0
    except Exception as error:
        logger.error(f"Erro ao obter contagem de novos cards no bpm, error:{error}")
        raise error


def select_new_offers(pricing_cursor: psycopg2.cursor) -> dict:
    try:
        query_select_new_offers = (
            """
            SELECT
            count(*) as new_offers
            FROM (
            SELECT distinct on (uuid_adm) 
            count(*) 
            FROM oferta.ofertas_unificado
            WHERE ADM = 'VOLKSWAGEN'
                AND ORIGEM_PRECIFICACAO = 'MD_OFERTA'
                AND created_at >= CURRENT_DATE - interval '3 hour'
                group by uuid_adm 
            ) as new_offers
            """)
        pricing_cursor.execute(query_select_new_offers)
        query_result_new_offers = pricing_cursor.fetchall()
        return get_table_dict(pricing_cursor, query_result_new_offers)[0]
    except Exception as error:
        logger.error(f"Erro ao obter contagem de ofertas, error:{error}")
        raise error


def insert_data_into_dynamo(new_quotas_md_quota: int, updated_quotas: int, new_adm_origin_quotas: int,
                            new_open_sea_quotas: int, group_end_date_inconsistency: dict, new_customers_cubees: dict,
                            new_cards: int, new_offers: dict) -> None:
    dynamo_db = boto3.resource('dynamodb')
    dynamo_table = dynamo_db.Table('md_kpi')
    logger.info(f"Criando objeto para inserir no dynamo")
    dynamo_data = {
        'administrator': 'VOLKSWAGEN',
        'calculation_date': str(datetime.now().isoformat()),
        'new_quotas_md_cota': new_quotas_md_quota,
        'updated_quotas_md_cota': updated_quotas,
        'new_adm_origin_quotas': new_adm_origin_quotas,
        'new_open_sea_quotas': new_open_sea_quotas,
        'group_inconsistencies': group_end_date_inconsistency['total_inconsistencies'],
        'cubees_new_customers': new_customers_cubees['new_customers'],
        'new_bpm_cards': new_cards,
        'new_offers': new_offers['new_offers'],
    }
    logger.info(f'Objeto criado: {dynamo_data}')
    logger.info('Inserindo objeto no dynamo')

    dynamo_table.put_item(Item=dynamo_data)
    logger.info('Objeto inserido com sucesso!')


def volks_kpi_calculates():
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()
    md_quota_cursor = md_quota_connection.cursor()
    bpm_connection_factory = GlueConnection(connection_name="BPM")
    bpm_connection = bpm_connection_factory.get_connection()
    bpm_cursor = bpm_connection.cursor()
    cubees_connection_factory = GlueConnection(connection_name="Cubees")
    cubees_connection = cubees_connection_factory.get_connection()
    cubees_cursor = cubees_connection.cursor()
    pricing_connection_factory = GlueConnection(connection_name="Pricing")
    pricing_connection = pricing_connection_factory.get_connection()
    pricing_cursor = pricing_connection.cursor()

    try:
        new_quotas_md_quota = select_new_quotas_md_quota(md_quota_cursor)
        logger.info(f'Quantidade de novas cotas no md-cota: {new_quotas_md_quota}')
        updated_quotas = select_updated_quotas_md_quota(md_quota_cursor)
        logger.info(f'Quantidade de cotas atualizadas no md-cota: {updated_quotas}')
        new_adm_origin_quotas = select_adm_origin_quotas_md_quota(md_quota_cursor)
        logger.info(f'Quantidade de novas cotas leilão no md-cota: {new_adm_origin_quotas}')
        new_open_sea_quotas = select_open_sea_quotas_md_quota(md_quota_cursor)
        logger.info(f'Quantidade de novas cotas não leilão no md-cota: {new_open_sea_quotas}')
        group_end_date_inconsistency = select_group_end_date_inconsistency(md_quota_cursor)
        logger.info(f"Quantidade de incosistências encontradas para data de encerramento de grupos Volkswagen: "
                    f"{group_end_date_inconsistency['total_inconsistencies']}")
        new_cards = select_bpm_new_cards(bpm_cursor)
        logger.info(f"Total de novos cards no bpm é: {new_cards}")
        new_customers_cubees = select_cubees_new_customers(cubees_cursor)
        logger.info(f"Total de novos clientes no cubees é: {new_customers_cubees['new_customers']}")
        new_offers = select_new_offers(pricing_cursor)
        logger.info(f"Total de novas ofertas para Volkswagen: {new_offers['new_offers']}")
        insert_data_into_dynamo(new_quotas_md_quota, updated_quotas, new_adm_origin_quotas, new_open_sea_quotas,
                                group_end_date_inconsistency, new_customers_cubees, new_cards, new_offers)

        logger.info("Dados processados com sucesso.")
    except Exception as error:
        logger.error("Erro durante cálculo de kpis:", error)
        raise error

    finally:
        # Close the cursor and connection
        md_quota_cursor.close()
        md_quota_connection.close()
        logger.info("Connection closed.")


if __name__ == '__main__':
    volks_kpi_calculates()
