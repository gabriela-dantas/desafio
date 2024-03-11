from bazartools.common.logger import get_logger
from bazartools.common.database.glueConnection import GlueConnection
from bazartools.common.database.glueMysqlConnection import GlueMysqlConnection
from pymysql.cursors import SSCursor
from datetime import datetime
from psycopg2 import OperationalError
from bazartools.common.eventTrigger import EventTrigger
from awsglue.utils import getResolvedOptions
import sys

BATCH_SIZE = 2000
GLUE_DEFAULT_CODE = 2
logger = get_logger()


def try_parsing_date(value):
    valid_date_formats = (
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%y-%m-%d",
        "%d/%m/%y",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%y-%m-%d %H:%M:%S",
        "%d/%m/%y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%d/%m/%Y %H:%M:%S.%f",
        "%y-%m-%d %H:%M:%S.%f",
        "%d/%m/%y %H:%M:%S.%f",
    )
    if value is None:
        return value

    for fmt in valid_date_formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    raise ValueError(f"Nenhum formato de data válido encontrado para {value}")


def find_partner_quotas(partner_cursor):
    try:
        # executando queries de busca de informações no banco partner
        query_select_partner_cotas = """
                SELECT 
                    cotas.uuid,
                    cotas.grupo,
                    cotas.share_number,
                    cotas.good_value,
                    cotas.good_type,
                    cotas.total_adm_fee_percentage,
                    cotas.reserve_fund_percentage,
                    cotas.contract_cancellation_date,
                    cotas.remaining_payments,
                    cotas.contract_creation_date,
                    cotas.late_amount,
                    cotas.common_fund_percentage,
                    cotas.late_parcel,
                    cotas.end_group_m,
                    cotas.group_variances,
                    cotas.available_credit_in_group_0,
                    cotas.available_credit_in_group_1,
                    cotas.available_credit_in_group_2,
                    cotas.available_credit_in_group_3,
                    cotas.available_credit_in_group_4,
                    cotas.available_credit_in_group_5,
                    cotas.available_credit_in_group_6,
                    cotas.available_credit_in_group_7,
                    cotas.available_credit_in_group_8,
                    cotas.available_credit_in_group_9,
                    cotas.qtdlancesmaximos_1,
                    cotas.qtdlancesmaximos_2,
                    cotas.qtdlancesmaximos_3,
                    cotas.qtdlancescontemplados_1,
                    cotas.qtdlancescontemplados_2,
                    cotas.qtdlancescontemplados_3,
                    cotas.pctlancescontemplados_1_1,
                    cotas.pctlancescontemplados_1_2,
                    cotas.pctlancescontemplados_1_3,
                    cotas.pctlancescontemplados_1_4,
                    cotas.pctlancescontemplados_1_5,
                    cotas.pctlancescontemplados_1_6,
                    cotas.datetimenow,
                    cotas.insurance_monthly_cost,
                    cotas.end_of_group_months,
                    cotas.end_of_share_months,
                    cotas.share_situation,
                    cotas.taxa_adesao,
                    cotas.seguro_vida,
                    cotas.share_version,
                    cotas.e_ou,
                    cotas.reclamacao,
                    cotas.end_group_value,
                    cotas.nova_tentativa,
                    cotas.ispj,
                    cotas.ts
                FROM cotas
                where cotas.ts >= (
                  SELECT DATE_SUB(DATE_FORMAT(MAX(cotas.ts), '%Y-%m-%d %H:%i:%s'), INTERVAL 24 HOUR)
                  FROM cotas
                )
                AND cotas.ts <= (
                  SELECT (DATE_ADD(DATE_FORMAT(MAX(cotas.ts), '%Y-%m-%d %H:%i:%s'), INTERVAL 24 HOUR))
                  FROM cotas
                );
            """
        logger.info(f"query leitura partner: {query_select_partner_cotas}")
        partner_cursor.execute(query_select_partner_cotas)
        logger.info("finalizou leitura partner")

    except Exception as error:
        logger.error(f"Erro durante tentativa de leitura do banco: {error}")
        raise error


def insert_stage_raw(row_dict, md_quota_cursor, md_quota_connection):
    try:
        info_date = "'" + str(row_dict["ts"]) + "'"
        date_time_now = try_parsing_date(row_dict["datetimenow"])

        query_insert_quota_stage_raw = """
                INSERT 
                INTO 
                stage_raw.tb_quotas_porto_pre 
                ( 
                uuid, 
                grupo, 
                share_number, 
                good_value, 
                good_type, 
                total_adm_fee_percentage, 
                reserve_fund_percentage, 
                contract_cancelation_date, 
                remaining_payments, 
                contract_creation_date, 
                late_amount, 
                common_fund_percentage, 
                late_parcel, 
                date_time_now, 
                insurance_monthly_cos, 
                share_situation, 
                taxa_adesao, 
                seguro_vida, 
                share_version, 
                e_ou, 
                end_group_value, 
                nova_tentativa, 
                is_pj, 
                is_processed, 
                created_at, 
                data_info, 
                end_of_share_months,
                end_group_m,
                group_variances,
                available_credit_in_group_0,
                available_credit_in_group_1,
                available_credit_in_group_2,
                available_credit_in_group_3,
                available_credit_in_group_4,
                available_credit_in_group_5,
                available_credit_in_group_6,
                available_credit_in_group_7,
                available_credit_in_group_8,
                available_credit_in_group_9,
                qtd_lances_maximos_1,
                qtd_lances_maximos_2,
                qtd_lances_maximos_3,
                qtd_lances_contemplados_1,
                qtd_lances_contemplados_2,
                qtd_lances_contemplados_3,
                pct_lances_contemplados_1_1,
                pct_lances_contemplados_1_2,
                pct_lances_contemplados_1_3,
                pct_lances_contemplados_1_4,
                pct_lances_contemplados_1_5,
                pct_lances_contemplados_1_6,
                end_of_group_months
                ) 
                VALUES 
                (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s)
            """
        logger.info(f"query insert cota: {query_insert_quota_stage_raw}")
        md_quota_cursor.execute(
            query_insert_quota_stage_raw,
            (
                row_dict["uuid"],
                row_dict["grupo"],
                row_dict["share_number"],
                row_dict["good_value"],
                row_dict["good_type"],
                row_dict["total_adm_fee_percentage"],
                row_dict["reserve_fund_percentage"].replace('.', '').replace(',', '.'),
                row_dict["contract_cancellation_date"],
                row_dict["remaining_payments"],
                row_dict["contract_creation_date"],
                row_dict["late_amount"],
                row_dict["common_fund_percentage"],
                row_dict["late_parcel"],
                date_time_now,
                row_dict["insurance_monthly_cost"],
                row_dict["share_situation"],
                row_dict["taxa_adesao"],
                row_dict["seguro_vida"],
                row_dict["share_version"],
                row_dict["e_ou"],
                row_dict["end_group_value"],
                row_dict["nova_tentativa"],
                row_dict["ispj"],
                "false",
                "now()",
                info_date,
                row_dict["end_of_share_months"],
                row_dict["end_group_m"],
                row_dict["group_variances"],
                row_dict["available_credit_in_group_0"],
                row_dict["available_credit_in_group_1"],
                row_dict["available_credit_in_group_2"],
                row_dict["available_credit_in_group_3"],
                row_dict["available_credit_in_group_4"],
                row_dict["available_credit_in_group_5"],
                row_dict["available_credit_in_group_6"],
                row_dict["available_credit_in_group_7"],
                row_dict["available_credit_in_group_8"],
                row_dict["available_credit_in_group_9"],
                row_dict["qtdlancesmaximos_1"],
                row_dict["qtdlancesmaximos_2"],
                row_dict["qtdlancesmaximos_3"],
                row_dict["qtdlancescontemplados_1"],
                row_dict["qtdlancescontemplados_2"],
                row_dict["qtdlancescontemplados_3"],
                row_dict["pctlancescontemplados_1_1"],
                row_dict["pctlancescontemplados_1_2"],
                row_dict["pctlancescontemplados_1_3"],
                row_dict["pctlancescontemplados_1_4"],
                row_dict["pctlancescontemplados_1_5"],
                row_dict["pctlancescontemplados_1_6"],
                row_dict["end_of_group_months"],
            ),
        )

    except Exception as error:
        # Rollback the transaction in case of an error
        md_quota_connection.rollback()
        logger.error(
            f"Ocorreu um erro durante tentativa de gravação dos dados no stage_raw: {error}"
        )
        raise error


def from_partner_porto_to_stage_raw():
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    partner_connection_factory = GlueMysqlConnection(connection_name="Partner")
    md_quota_connection = md_quota_connection_factory.get_connection()
    partner_connection = partner_connection_factory.get_connection()
    partner_cursor = partner_connection.cursor(SSCursor)
    md_quota_cursor = md_quota_connection.cursor()
    find_partner_quotas(partner_cursor)

    try:
        batch_counter = 0

        while True:
            batch_counter += 1
            rows = partner_cursor.fetchmany(size=BATCH_SIZE)  # Fetch XXX rows at a time
            column_names = [desc[0] for desc in partner_cursor.description]
            if not rows:
                break
            logger.info(
                f"Fetch {BATCH_SIZE} rows at a time - Batch number {batch_counter}"
            )
            for row in rows:
                row_dict = dict(zip(column_names, row))
                insert_stage_raw(row_dict, md_quota_cursor, md_quota_connection)

            # Commit the transaction
            md_quota_connection.commit()
            logger.info(
                f"Transaction committed successfully to batch number {batch_counter}!"
            )

    except OperationalError as e:
        # Rollback the transaction in case of an error
        md_quota_connection.rollback()
        logger.error("Transaction rolled back due to an oretaional error:", e)

    except Exception as error:
        # Rollback the transaction in case of an error
        md_quota_connection.rollback()
        logger.error("Transaction rolled back due to an error:", error)
        raise error

    finally:
        # Close the cursor and connection
        md_quota_cursor.close()
        md_quota_connection.close()
        partner_cursor.close()
        partner_connection.close()
        logger.info("Connection closed.")


if __name__ == "__main__":
    from_partner_porto_to_stage_raw()
