import pytest
import json
import os

from fastapi.testclient import TestClient
from botocore.exceptions import ClientError

from common.models.staging import *
from common.models.md_quota import *
from common.database.session import db_session, engines_by_base
from common.dynamo_connection import dynamodb
from common.repositories.dynamo.santander_webhook_event import (
    SantanderWebhookEventRepository,
)
from simple_common.utils import serialize_dynamo_item
from api import app
from .pdf_mock_data import not_mapped_pdf_data, santander_1_pdf_data


@pytest.fixture(scope="function")
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(scope="session", autouse=True)
def create_postgres_db() -> None:
    for base, engine in engines_by_base.items():
        base.metadata.drop_all(bind=engine)
        base.metadata.create_all(bind=engine)


def create_dynamo_tables_and_data() -> None:
    file_dir = os.path.join(
        os.path.realpath(__file__), *["..", "dynamo_tables_definitions.json"]
    )
    file_dir = os.path.normpath(file_dir)

    with open(file_dir, "r") as dynamo_tables:
        tables_definitions: dict = json.loads(dynamo_tables.read())

    for table_key, table_definitions in tables_definitions.items():
        table_name = table_definitions["creation"]["TableName"]
        try:
            table = dynamodb.Table(table_name)
            table.delete()
        except ClientError as client_error:
            if client_error.response["Error"]["Code"] != "ResourceNotFoundException":
                raise client_error

        table = dynamodb.create_table(**table_definitions["creation"])

        with table.batch_writer() as batch:
            for item in table_definitions["data"]:
                item = serialize_dynamo_item(item)
                batch.put_item(Item=item)


def pytest_sessionstart(session) -> None:
    print(
        f"Executando criação das tabelas do Dynamo antes de coletar testes. "
        f"Recebida sessão dos testes: {type(session)}"
    )
    create_dynamo_tables_and_data()


@pytest.fixture(scope="class", autouse=True)
def dynamo_tables_teardown() -> None:
    santander_webhook_event_repository = SantanderWebhookEventRepository()
    santander_webhook_event_items = santander_webhook_event_repository.scan_all()
    item_keys = []

    for item in santander_webhook_event_items:
        item_keys.append(
            {"share_id": item["share_id"], "event_date": item["event_date"]}
        )

    santander_webhook_event_repository.batch_delete(item_keys)


@pytest.fixture(scope="class", autouse=True)
def tables_teardown() -> None:
    db_session.query(QuotasAPIModel).delete()
    db_session.query(BeeReaderModel).delete()
    db_session.query(QuotasSantanderPreModel).delete()
    db_session.query(GroupsSantanderPreModel).delete()
    db_session.query(BidsSantanderPreModel).delete()
    db_session.query(PrizeDrawSantanderPreModel).delete()
    db_session.query(QuotasGMACPreModel).delete()
    db_session.query(GroupsGMACModel).delete()
    db_session.query(CustomersGMACPreModel).delete()
    db_session.query(QuotasVolksPreModel).delete()
    db_session.query(GroupsVolksPreModel).delete()
    db_session.query(AssetsVolksPreModel).delete()
    db_session.query(BidsVolksPreModel).delete()
    db_session.query(CustomersVolksPreModel).delete()

    db_session.query(QuotaHistoryDetailModel).delete()
    db_session.query(QuotaOwnerModel).delete()
    db_session.query(QuotaStatusModel).delete()
    db_session.query(QuotaModel).delete()
    db_session.query(GroupModel).delete()
    db_session.query(CorrectionFactorTypeModel).delete()
    db_session.query(QuotaViewModel).delete()

    db_session.commit()


@pytest.fixture(scope="session", autouse=True)
def create_essential_tables(create_postgres_db) -> None:
    db_session.query(AdministratorModel).delete()
    db_session.query(AssetTypeModel).delete()
    db_session.query(QuotaOriginModel).delete()
    db_session.query(QuotaPersonTypeModel).delete()
    db_session.query(QuotaStatusTypeModel).delete()
    db_session.query(QuotaStatusCatModel).delete()

    db_session.commit()

    administrator_1 = {
        "administrator_id": 1,
        "administrator_code": "0000000155",
        "administrator_desc": "ITAÚ ADM DE CONSÓRCIOS LTDA",
    }
    administrator_2 = {
        "administrator_id": 2,
        "administrator_code": "0000000234",
        "administrator_desc": "SANTANDER ADM. CONS. LTDA",
    }

    db_session.bulk_insert_mappings(
        AdministratorModel, [administrator_1, administrator_2]
    )

    asset_type_1 = {
        "asset_type_code": "REAEST",
        "asset_type_code_ext": "1",
        "asset_type_desc": "REAL ESTATE",
    }
    asset_type_2 = {
        "asset_type_code": "CAR",
        "asset_type_code_ext": "3",
        "asset_type_desc": "CAR",
    }
    asset_type_3 = {
        "asset_type_code": "HVEHIC",
        "asset_type_code_ext": "2",
        "asset_type_desc": "HEAVY VEHICLE",
    }
    asset_type_4 = {
        "asset_type_code": "MOTCLE",
        "asset_type_code_ext": "4",
        "asset_type_desc": "MOTORCYCLE",
    }
    asset_type_5 = {
        "asset_type_code": "SRVICS",
        "asset_type_code_ext": "6",
        "asset_type_desc": "SERVICES",
    }
    asset_type_6 = {
        "asset_type_code": "OUMOBAS",
        "asset_type_code_ext": "5",
        "asset_type_desc": "OTHER MOBILE ASSETS",
    }
    asset_type_7 = {
        "asset_type_code": "ND",
        "asset_type_code_ext": "ND",
        "asset_type_desc": "NOT DEFINED",
    }

    db_session.bulk_insert_mappings(
        AssetTypeModel,
        [
            asset_type_1,
            asset_type_2,
            asset_type_3,
            asset_type_4,
            asset_type_5,
            asset_type_6,
            asset_type_7,
        ],
    )

    quota_origin_1 = {
        "quota_origin_code": "ADMORGN",
        "quota_origin_desc": "SEND BY ADMINISTRATOR",
    }
    quota_origin_2 = {
        "quota_origin_code": "CUSORGN",
        "quota_origin_desc": "SEND BY CLIENT",
    }
    quota_origin_3 = {"quota_origin_code": "UND", "quota_origin_desc": "UNDEFINED"}

    db_session.bulk_insert_mappings(
        QuotaOriginModel, [quota_origin_1, quota_origin_2, quota_origin_3]
    )

    quota_person_type_1 = {
        "quota_person_type_code": "NP",
        "quota_person_type_desc": "NATURAL PERSON QUOTA",
    }
    quota_person_type_2 = {
        "quota_person_type_code": "LP",
        "quota_person_type_desc": "LEGAL PERSON QUOTA",
    }

    db_session.bulk_insert_mappings(
        QuotaPersonTypeModel, [quota_person_type_1, quota_person_type_2]
    )

    quota_status_cat_1 = {
        "quota_status_cat_id": 1,
        "quota_status_cat_code": "ACTIVE",
        "quota_status_cat_desc": "ACTIVE QUOTA",
    }
    quota_status_cat_2 = {
        "quota_status_cat_id": 2,
        "quota_status_cat_code": "NOT_ACTIVE",
        "quota_status_cat_desc": "NOT ACTIVE",
    }
    quota_status_cat_3 = {
        "quota_status_cat_id": 3,
        "quota_status_cat_code": "UND",
        "quota_status_cat_desc": "UNDEFINED",
    }

    db_session.bulk_insert_mappings(
        QuotaStatusCatModel,
        [quota_status_cat_1, quota_status_cat_2, quota_status_cat_3],
    )

    quota_status_type_1 = {
        "quota_status_type_code": "ACTIVE",
        "quota_status_type_desc": "ACTIVE QUOTA",
        "quota_status_cat_id": 1,
    }
    quota_status_type_2 = {
        "quota_status_type_code": "CANCELLED",
        "quota_status_type_desc": "CANCELLED QUOTA",
        "quota_status_cat_id": 2,
    }
    quota_status_type_3 = {
        "quota_status_type_code": "DELAYED",
        "quota_status_type_desc": "DELAY IN INSTALLMENT PAYMENTS",
        "quota_status_cat_id": 1,
    }
    quota_status_type_4 = {
        "quota_status_type_code": "DROP-OUT",
        "quota_status_type_desc": "DROP-OUT",
        "quota_status_cat_id": 2,
    }
    quota_status_type_5 = {
        "quota_status_type_code": "UND",
        "quota_status_type_desc": "UNDEFINED",
        "quota_status_cat_id": 3,
    }

    db_session.bulk_insert_mappings(
        QuotaStatusTypeModel,
        [
            quota_status_type_1,
            quota_status_type_2,
            quota_status_type_3,
            quota_status_type_4,
            quota_status_type_5,
        ],
    )

    db_session.commit()


@pytest.fixture(scope="function")
def context():
    class LambdaContext:
        function_name: str = "test"
        aws_request_id: str = "88888888-4444-4444-4444-121212121212"
        invoked_function_arn: str = (
            "arn:aws:lambda:eu-west-1:123456789101:function:test"
        )

    return LambdaContext()


@pytest.fixture(scope="function")
def not_mapped_pdf_content() -> bytes:
    return not_mapped_pdf_data()


@pytest.fixture(scope="function")
def santander_1_pdf_content() -> bytes:
    return santander_1_pdf_data()
