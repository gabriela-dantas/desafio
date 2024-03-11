from bazartools.common.logger import get_logger
from bazartools.common.database.glueConnection import GlueConnection
from bazartools.common.eventTrigger import EventTrigger
from awsglue.utils import getResolvedOptions
import sys
import requests
import boto3
from collections import defaultdict
import json
import os

from botocore.exceptions import ClientError

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


def get_dict_by_id(id_item: str, data_list: list, field_name: str):
    for item_list in data_list:
        if item_list[field_name] == id_item:
            return item_list
    return None


def verify_contact_type(contact_number: str) -> str:
    phone = contact_number[-11:]
    if len(phone) == 11 or int(phone[-8]) >= 6:
        return "MOBILE"
    else:
        return "LANPHONE"


def get_event(workflow_name: str, job_name: str) -> list:
    logger.info("buscando evento do eventbridge")

    ids = []

    event_trigger = EventTrigger(workflow_name, job_name)

    event = event_trigger.get_event_details()
    logger.info(f"event: {event}")
    if event is not None:
        ids = event["detail"]["quota_id_list"]

    return ids


def query_select_quotas(quota_id_list, md_cota_select_cursor):
    try:
        query_select_quotas_from_quota_view = """
            SELECT 
             *
             FROM 
             md_cota.pl_quota_view pqv 
             WHERE 
             pqv.quota_id IN %s
            """
        md_cota_select_cursor.execute(
            query_select_quotas_from_quota_view, (tuple(quota_id_list),)
        )
        query_result_quotas = md_cota_select_cursor.fetchall()
        quotas_md_cota_dict = get_table_dict(md_cota_select_cursor, query_result_quotas)

        return quotas_md_cota_dict

    except Exception as e:
        logger.error(f"Error in query_select_quotas_from_quota_view: {str(e)}")
        raise


def query_select_owners(quota_id_list, md_cota_select_cursor):
    try:
        query_select_owner = """
            SELECT
             *
             FROM 
             md_cota.pl_quota_owner pqo 
             WHERE 
             pqo.quota_id IN %s
             AND 
             pqo.valid_to IS NULL
            """
        md_cota_select_cursor.execute(query_select_owner, (tuple(quota_id_list),))
        query_result_owners = md_cota_select_cursor.fetchall()
        owners_md_cota_dict = get_table_dict(md_cota_select_cursor, query_result_owners)

        return owners_md_cota_dict

    except Exception as e:
        logger.error(f"Error in query_select_owners: {str(e)}")
        raise


def get_quotas_to_quota_creation_invoke(
    quota_id_list: list, md_cota_select_cursor
) -> list:
    owners_by_quota = defaultdict(list)
    quota_id_list = list(set(quota_id_list))
    quota_code_list = []

    quotas_md_cota_dict = query_select_quotas(quota_id_list, md_cota_select_cursor)
    owners_md_cota_dict = query_select_owners(quota_id_list, md_cota_select_cursor)

    for owner in owners_md_cota_dict:
        owners_by_quota[owner["quota_id"]].append(
            {
                "person_code": owner["person_code"],
                "titular": owner["main_owner"],
            }
        )

    for quota in quotas_md_cota_dict:
        if not owners_by_quota[quota["quota_id"]]:
            logger.info(
                f"quota_code {quota['quota_code']}, "
                f"a ser enviada ao BPM, não possui owner!"
            )
            continue
        data = {"quota_code": quota["quota_code"], "share_id": None}
        quota_code_list.append(data)

    return quota_code_list


def build_customer_payload(request_body: dict):
    contacts = []
    payload = {}

    phone_preferred_contact = True

    if request_body.get("cpf"):
        contact_cat = "PERSONAL"

    else:
        contact_cat = "BUSINESS"

    if request_body["email"] != "":
        contact = {
            "contact_desc": "EMAIL PESSOAL",
            "contact": request_body["email"],
            "contact_category": contact_cat,
            "contact_type": "EMAIL",
            "preferred_contact": True,
        }
        phone_preferred_contact = False
        contacts.append(contact)

    if request_body["phone"] != "":
        contact = {
            "contact_desc": "TELEFONE",
            "contact": request_body["phone"],
            "contact_category": contact_cat,
            "contact_type": verify_contact_type(request_body["phone"]),
            "preferred_contact": phone_preferred_contact,
        }
        contacts.append(contact)

        additional_phones_list = ["secondary_phone", "tertiary_phone"]

        for key in additional_phones_list:
            if request_body[key] != "" and request_body[key] != "0":
                contact = {
                    "contact_desc": "TELEFONE",
                    "contact": request_body[key],
                    "contact_category": contact_cat,
                    "contact_type": verify_contact_type(
                        request_body["secondary_phone"]
                    ),
                    "preferred_contact": False,
                }
                contacts.append(contact)

    addresses = []

    if request_body["address"] != "":
        address = {
            "address": request_body["address"],
            "address_2": "Não informado",
            "street_number": "Não informado",
            "district": request_body["district"],
            "zip_code": request_body["zip"],
            "address_category": "RESI",
            "city": request_body["city"],
            "state": request_body["state"],
        }
        addresses.append(address)

        payload["administrator_code"] = "0000000206"
        payload["channel_type"] = "EMAIL"
        payload["reactive"] = False
    if len(addresses) > 0:
        payload["addresses"] = addresses

    if len(contacts) > 0:
        payload["contacts"] = contacts

    if "cpf" in request_body:
        person_ext_code = format_document(request_body)
        person_type = "NATURAL"
        payload["person_ext_code"] = person_ext_code
        payload["person_type"] = person_type
        payload["natural_person"] = {
            "full_name": request_body["name"],
            "birthdate": None,
        }
        payload["documents"] = [
            {
                "document_number": person_ext_code,
                "expiring_date": "2040-12-01",
                "person_document_type": "CPF",
            }
        ]

    else:
        person_type = "LEGAL"
        person_ext_code = format_document(request_body)
        payload["person_ext_code"] = person_ext_code
        payload["person_type"] = person_type
        payload["legal_person"] = {
            "company_name": request_body["name"],
            "company_fantasy_name": request_body["name"],
            "founding_date": None,
        }
        payload["documents"] = [
            {
                "document_number": person_ext_code,
                "expiring_date": "2040-12-01",
                "person_document_type": "CS",
            }
        ]

    return payload


def fetch_select_quotas_md_quota(md_quota_cursor):
    try:
        query_select_quotas_md_quota = """select 
            *
            from 
            md_cota.pl_quota pq 
            left join md_cota.pl_administrator pa on pa.administrator_id = pq.administrator_id
            where 
            pa.administrator_desc = 'PORTO SEGURO ADM. CONS. LTDA'
            """
        logger.info(f"query leitura quotas md-cota: {query_select_quotas_md_quota}")
        md_quota_cursor.execute(query_select_quotas_md_quota)
        query_result_quotas = md_quota_cursor.fetchall()
        return get_table_dict(md_quota_cursor, query_result_quotas)
    except Exception as error:
        logger.error(f"Erro ao buscar dados na pl_quota: error:{error}")
        raise error


def fetch_select_quotas_api(md_quota_cursor, customer_to_process_list):
    try:
        query_select_customer_data = """
            select
            *
            from
            stage_raw.tb_quotas_api tqa
            where
            tqa.endpoint_generator = 'POST /object/create'
            and
            tqa.is_processed is false
            and
            tqa.administrator = 'PORTO'
            and
            tqa.id_quotas_itau in %s
            """
        logger.info(f"query leitura customers stage_raw: {query_select_customer_data}")
        md_quota_cursor.execute(
            query_select_customer_data, (tuple(customer_to_process_list),)
        )

    except Exception as error:
        logger.error(f"Erro ao buscar dados na pl_quota: error:{error}")
        raise error


def invoke_quota_creation(quota_code_list: list, lambda_name: str):
    try:
        payload_lambda = {"quota_code_list": quota_code_list}
        request_lambda = json.dumps(payload_lambda)
        lambda_client = boto3.client("lambda")
        lambda_client.invoke(
            FunctionName=lambda_name,
            InvocationType="Event",
            Payload=request_lambda,
        )
        logger.info("Quota enviadas para o quota_creation:{len(quota_co}")
    except ClientError as error:
        logger.error(f"Error ao fazer invoke na lambda quota_creation, error:{error}")
        raise error


def invoke_lambda(
    ownership_percentage: dict,
    main_owner: str,
    customers: list,
    lambda_cubees_customer: str,
):
    try:
        payload_lambda = {
            "quota_id": -1,
            "ownership_percentage": ownership_percentage,
            "main_owner": main_owner,
            "cubees_request": customers,
        }
        request_lambda = json.dumps(payload_lambda)
        lambda_client = boto3.client("lambda")
        lambda_client.invoke(
            FunctionName=lambda_cubees_customer,
            InvocationType="Event",
            Payload=request_lambda,
        )
    except ClientError as error:
        logger.error(f"Error ao fazer invoke na lambda cubees, error:{error}")
        raise error


def execute_update_quota_query(md_quota_update_cursor, quota_origin_id, quota_id):
    try:
        query_update_quota = """
            UPDATE md_cota.pl_quota
            SET
            quota_origin_id = %s,
            modified_at = now(),
            modified_by = %s
            WHERE
            quota_id = %s
        """

        parameters = {
            "quota_origin_id": quota_origin_id,
            "modified_by": GLUE_DEFAULT_CODE,
            "quota_id": quota_id,
        }
        params = (
            parameters.get("quota_origin_id"),
            parameters.get("modified_by"),
            parameters.get("quota_id"),
        )

        md_quota_update_cursor.execute(query_update_quota, params)
    except Exception as error:
        logger.error(f"Erro ao executar a query de atualização: {error}")
        raise error


def execute_update_stage_raw_query(md_quota_update_cursor, row_dict):
    try:
        query_update_stage_raw = f"""
            UPDATE stage_raw.tb_quotas_api
            SET is_processed = true
            WHERE id_quotas_itau = {row_dict['id_quotas_itau']};
        """
        logger.info(f"Query de atualização: {query_update_stage_raw}")
        md_quota_update_cursor.execute(query_update_stage_raw)
    except Exception as error:
        logger.error(f"Erro ao executar a query de atualização: {error}")
        raise error


def format_document(request_body):
    if "cpf" in request_body:
        person_ext_code = (
            request_body["cpf"].replace(".", "").replace("-", "").replace("/", "")
        )
    else:
        person_ext_code = (
            request_body["cnpj"].replace(".", "").replace("-", "").replace("/", "")
        )
    return person_ext_code


def process_row(
    row_dict,
    quotas_md_quota_dict,
    lambda_cubees_customer,
    md_quota_update_cursor,
    quota_id_list,
):
    main_owner = 0
    request_body = json.loads(row_dict["request_body"])
    row_md_quota = get_dict_by_id(
        request_body["uuid"], quotas_md_quota_dict, "external_reference"
    )

    person_ext_code = format_document(request_body)

    payload = build_customer_payload(request_body)
    customers = [payload]

    ownership_percentage = request_body["participacao"] / 100
    if request_body["titular"]:
        main_owner = person_ext_code
    invoke_lambda(ownership_percentage, main_owner, customers, lambda_cubees_customer)

    # fluxo de atualização caso a cota exista no md-cota
    if row_md_quota is not None:
        quota_id = row_md_quota["quota_id"]
        quota_id_list.append(row_md_quota["quota_id"])

        quota_origin_id = 2
        if request_body["origin"] == "parceiro_porto_seguro":
            quota_origin_id = 1
        execute_update_quota_query(md_quota_update_cursor, quota_origin_id, quota_id)
    else:
        logger.info(f'Cota não encontrada para o cliente {row_dict["request_body"]}')
    execute_update_stage_raw_query(md_quota_update_cursor, row_dict)


def process_all(
    quotas_md_quota_dict,
    lambda_cubees_customer,
    md_quota_update_cursor,
    quota_id_list,
    md_quota_cursor,
    md_quota_select_cursor,
    md_quota_connection,
    lambda_quota_creation,
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
                    quotas_md_quota_dict,
                    lambda_cubees_customer,
                    md_quota_update_cursor,
                    quota_id_list,
                )
            if quota_id_list:
                quota_code_list = get_quotas_to_quota_creation_invoke(
                    quota_id_list, md_quota_select_cursor
                )
                invoke_quota_creation(quota_code_list, lambda_quota_creation)
            md_quota_connection.commit()
            logger.info("Transaction committed successfully!")
        except Exception as error:
            raise error


def porto_customer_ingestion():
    args = getResolvedOptions(
        sys.argv,
        ["cubees_customer_host", "WORKFLOW_NAME", "JOB_NAME", "quota_creation_lambda"],
    )
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()
    md_quota_select_cursor = md_quota_connection.cursor()
    md_quota_update_cursor = md_quota_connection.cursor()
    md_quota_cursor = md_quota_connection.cursor()
    quota_id_list = []
    # url_bpm = os.environ.get("bpm_url")
    # token = os.environ.get("bpm_token")
    lambda_cubees_customer = args["cubees_customer_host"]
    lambda_quota_creation = args["quota_creation_lambda"]
    workflow_name = args["WORKFLOW_NAME"]
    job_name = args["JOB_NAME"]
    customer_to_process_list = list(set(get_event(workflow_name, job_name)))

    try:
        quota_md_quota_dict = fetch_select_quotas_md_quota(md_quota_cursor)
        fetch_select_quotas_api(md_quota_cursor, customer_to_process_list)
        process_all(
            quota_md_quota_dict,
            lambda_cubees_customer,
            md_quota_update_cursor,
            quota_id_list,
            md_quota_cursor,
            md_quota_select_cursor,
            md_quota_connection,
            lambda_quota_creation,
        )
        logger.info("Processamento de dados finalizados com sucesso.")
    except Exception as error:
        logger.error(f"Error ao processar dados:{error}")
        raise error
    finally:
        md_quota_connection.close()
        logger.info("Conexão encerada com sucesso.")


if __name__ == "__main__":
    porto_customer_ingestion()
