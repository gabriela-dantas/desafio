# imports do glue

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

import requests
from datetime import datetime, timedelta
from dateutil import relativedelta
from enum import Enum
import json
import sys

# imports for connection
from sqlalchemy import create_engine, func
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.exc import OperationalError, DatabaseError

# imports for models:
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Boolean,
    BigInteger,
    ForeignKey,
    Numeric,
)

from sqlalchemy.ext.declarative import declarative_base

# imports loggers:
import logging
from typing import Literal


class Logger:
    def __init__(
        self, name: str = "etl-beereader", level: Literal[20] = logging.INFO
    ) -> None:
        self.log = self.logger(name, level)

    @classmethod
    def logger(cls, name: str, level: Literal[20] = logging.INFO) -> logging.Logger:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        logging.basicConfig(format=log_format)
        loggers = logging.getLogger(name)
        loggers.setLevel(level)
        return loggers


logger = Logger().log


# logger.info("Mensagem de informações")
# logger.warning("Mensagem de aviso")
# logger.error("Mensagem de error")


class DbInfo(Enum):
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "db-host", "db-port", "db-name", "db-user", "db-pass"]
    )
    DB_HOST = args["db_host"]
    DB_PORT = args["db_port"]
    DB_NAME = args["db_name"]
    DB_USER = args["db_user"]
    DB_PASSWORD = args["db_pass"]


class Connection:
    def __init__(self, schema) -> None:
        try:
            url = (
                f"postgresql://{DbInfo.DB_USER.value}:{DbInfo.DB_PASSWORD.value}@"
                f"{DbInfo.DB_HOST.value}:{DbInfo.DB_PORT.value}/{DbInfo.DB_NAME.value}"
            )

            self.__engine = create_engine(url)
            self.__db_session = scoped_session(
                sessionmaker(autocommit=False, bind=self.__engine)
            )
            self.__db_session.execute(f"SET search_path TO {schema}")
            logger.info("Database connection successfully established")
        except OperationalError as error:
            logger.error(
                "Connection not established with the database".format(str(error))
            )

    def get_session(self):
        return self.__db_session


# Código utilizado para as colunas padrão dos modelos: created_by, modified_by, deleted_by,
# created_by_app, modified_by_app, deleted_by_app
GLUE_DEFAULT_CODE = 2

Base = declarative_base()


def get_default_datetime() -> datetime:
    return datetime.now()


class BasicFieldsSchema:
    created_at = Column(DateTime, nullable=False, default=get_default_datetime())
    modified_at = Column(DateTime, nullable=False, default=get_default_datetime())


class BasicFields(BasicFieldsSchema):
    deleted_at = Column(DateTime)
    created_by = Column(Integer, nullable=False, default=GLUE_DEFAULT_CODE)
    modified_by = Column(Integer, nullable=False, default=GLUE_DEFAULT_CODE)
    deleted_by = Column(Integer)
    is_deleted = Column(Boolean, nullable=False, default=False)


class PlDataSource(Base, BasicFields):
    __tablename__ = "pl_data_source"

    data_source_id = Column(BigInteger, primary_key=True, index=True)
    data_source_code = Column(String(20), nullable=False)
    data_source_desc = Column(String(255), nullable=False)


class PlQuotaPersonType(Base, BasicFields):
    __tablename__ = "pl_quota_person_type"
    quota_person_type_id = Column(BigInteger, primary_key=True, index=True)
    quota_person_type_code = Column(String(20), nullable=False)
    quota_person_type_desc = Column(String(255), nullable=False)


class PlQuotaStatusCat(Base, BasicFields):
    __tablename__ = "pl_quota_status_cat"
    quota_status_cat_id = Column(BigInteger, primary_key=True, index=True)
    quota_status_cat_code = Column(String(10), nullable=False)
    quota_status_cat_desc = Column(String(255), nullable=False)


class PlQuotaStatusType(Base, BasicFields):
    __tablename__ = "pl_quota_status_type"
    quota_status_type_id = Column(BigInteger, primary_key=True, index=True)
    quota_status_type_code = Column(String(10), nullable=False)
    quota_status_type_desc = Column(String(255), nullable=False)
    quota_status_cat_id = Column(
        Integer, ForeignKey(PlQuotaStatusCat.quota_status_cat_id)
    )


class PlAdministrator(Base, BasicFields):
    __tablename__ = "pl_administrator"
    administrator_id = Column(BigInteger, primary_key=True, index=True)
    administrator_code = Column(String(20), nullable=False)
    administrator_desc = Column(String(255), nullable=False)


class PlCorrectionFactorType(Base, BasicFields):
    __tablename__ = "pl_correction_factor_type"
    correction_factor_type_id = Column(BigInteger, primary_key=True, index=True)
    correction_factor_type_code = Column(String(50), nullable=False)
    correction_factor_type_desc = Column(String(255), nullable=False)


class PlGroup(Base, BasicFields):
    __tablename__ = "pl_group"
    group_id = Column(BigInteger, primary_key=True, index=True)
    group_code = Column(String(255), nullable=False)
    group_deadline = Column(Integer, nullable=True)
    group_start_date = Column(DateTime, nullable=True)
    group_closing_date = Column(DateTime, nullable=True)
    per_max_embedded_bid = Column(Numeric(precision=12, scale=2), nullable=True)
    next_adjustment_date = Column(DateTime, nullable=True)
    current_assembly_date = Column(DateTime, nullable=True)
    current_assembly_number = Column(Integer, nullable=True)
    administrator_id = Column(Integer, ForeignKey(PlAdministrator.administrator_id))
    correction_factor_type_id = Column(
        Integer, ForeignKey(PlCorrectionFactorType.correction_factor_type_id)
    )


class PlQuotaOrigin(Base, BasicFields):
    __tablename__ = "pl_quota_origin"
    quota_origin_id = Column(BigInteger, primary_key=True, index=True)
    quota_origin_code = Column(String(20), nullable=False)
    quota_origin_desc = Column(String(255), nullable=False)


class PlQuota(Base, BasicFields):
    __tablename__ = "pl_quota"
    quota_id = Column(BigInteger, primary_key=True, index=True)
    quota_code = Column(String(255), nullable=False)
    quota_number = Column(String(255), nullable=True)
    check_digit = Column(String(255), nullable=True)
    external_reference = Column(String(255), nullable=False)
    total_installments = Column(Integer, nullable=True)
    version_id = Column(String(70), nullable=True)
    contract_number = Column(String(70), nullable=True)
    is_contemplated = Column(Boolean, nullable=False)
    is_multiple_ownership = Column(Boolean, nullable=False)
    contemplation_date = Column(DateTime, nullable=True)
    administrator_fee = Column(Numeric(precision=12, scale=2), nullable=True)
    insurance_fee = Column(Numeric(precision=12, scale=2), nullable=True)
    fund_reservation_fee = Column(Numeric(precision=12, scale=2), nullable=True)
    info_date = Column(DateTime, nullable=False)
    quota_status_type_id = Column(
        Integer, ForeignKey(PlQuotaStatusType.quota_status_type_id)
    )
    administrator_id = Column(Integer, ForeignKey(PlAdministrator.administrator_id))
    group_id = Column(Integer, ForeignKey(PlGroup.group_id))
    quota_origin_id = Column(Integer, ForeignKey(PlQuotaOrigin.quota_origin_id))
    quota_person_type_id = Column(
        Integer, ForeignKey(PlQuotaPersonType.quota_person_type_id)
    )
    quota_plan = Column(String(255), nullable=True)
    acquisition_date = Column(DateTime, nullable=True)
    cancel_date = Column(DateTime, nullable=True)


class PlQuotaHistoryField(Base, BasicFields):
    __tablename__ = "pl_quota_history_field"
    quota_history_field_id = Column(BigInteger, primary_key=True, index=True)
    quota_history_field_code = Column(String(50), nullable=False)


class PlQuotaFieldUpdateDate(Base, BasicFields):
    __tablename__ = "pl_quota_field_update_date"
    quota_field_update_date_id = Column(BigInteger, primary_key=True, index=True)
    update_date = Column(DateTime, nullable=False)
    quota_history_field_id = Column(
        Integer, ForeignKey(PlQuotaHistoryField.quota_history_field_id)
    )
    data_source_id = Column(Integer, ForeignKey(PlDataSource.data_source_id))
    quota_id = Column(Integer, ForeignKey(PlQuota.quota_id))


class PlAssetType(Base, BasicFields):
    __tablename__ = "pl_assset_type"
    asset_type_id = Column(BigInteger, primary_key=True, index=True)
    asset_type_code = Column(String(20), nullable=False)
    asset_type_code_ext = Column(String(10), nullable=True)
    asset_type_desc = Column(String(255), nullable=False)


class PlQuotaHistoryDetail(Base, BasicFields):
    __tablename__ = "pl_quota_history_detail"
    quota_history_detail_id = Column(BigInteger, primary_key=True, index=True)
    quota_id = Column(Integer, ForeignKey(PlQuota.quota_id))
    old_quota_number = Column(Integer, nullable=True)
    old_digit = Column(Integer, nullable=True)
    quota_plan = Column(String(70), nullable=True)
    installments_paid_number = Column(Integer, nullable=True)
    overdue_installments_number = Column(Integer, nullable=True)
    overdue_percentage = Column(Numeric(precision=12, scale=2), nullable=True)
    per_amount_paid = Column(Numeric(precision=12, scale=2), nullable=True)
    per_mutual_fund_paid = Column(Numeric(precision=19, scale=4), nullable=True)
    per_reserve_fund_paid = Column(Numeric(precision=19, scale=4), nullable=True)
    per_adm_paid = Column(Numeric(precision=19, scale=4), nullable=True)
    per_subscription_paid = Column(Numeric(precision=19, scale=4), nullable=True)
    per_mutual_fund_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    per_reserve_fund_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    per_adm_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    per_subscription_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    per_insurance_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    per_install_diff_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    per_total_amount_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    amnt_mutual_fund_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    amnt_reserve_fund_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    amnt_adm_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    amnt_subscription_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    amnt_insurance_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    amnt_fine_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    amnt_interest_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    amnt_others_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    amnt_install_diff_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    amnt_to_pay = Column(Numeric(precision=19, scale=4), nullable=True)
    quitter_assembly_number = Column(Integer, nullable=True)
    cancelled_assembly_number = Column(Integer, nullable=True)
    adjustment_date = Column(DateTime, nullable=True)
    current_assembly_date = Column(DateTime, nullable=True)
    current_assembly_number = Column(Integer, nullable=True)
    asset_adm_code = Column(String(255), nullable=True)
    asset_description = Column(String(255), nullable=True)
    asset_value = Column(Numeric(precision=19, scale=4), nullable=False)
    asset_type_id = Column(Integer, ForeignKey(PlAssetType.asset_type_id))
    info_date = Column(DateTime, nullable=False)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime, nullable=True)


class PlQuotaOwner(Base, BasicFields):
    __tablename__ = "pl_quota_owner"
    quota_owner_id = Column(BigInteger, primary_key=True, index=True)
    ownership_percent = Column(Numeric(precision=12, scale=4))
    quota_id = Column(Integer, ForeignKey(PlQuota.quota_id))
    main_owner = Column(Boolean)
    person_code = Column(String(255), nullable=False)
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime, nullable=True)


class PlQuotaStatus(Base, BasicFields):
    __tablename__ = "pl_quota_status"
    quota_status_id = Column(BigInteger, primary_key=True, index=True)
    quota_id = Column(Integer, ForeignKey(PlQuota.quota_id))
    quota_status_type_id = Column(
        Integer, ForeignKey(PlQuotaStatusType.quota_status_type_id)
    )
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime, nullable=True)


class StageRaw(Base):
    __tablename__ = "tb_beereader"
    id_beereader = Column(BigInteger, primary_key=True, index=True)
    file_name = Column(String(255))
    bpm_quota_id = Column(Integer)
    adm = Column(String(255))
    quota_data = Column(String)
    s3_path = Column(String(255))
    attachment_date = Column(DateTime)
    created_at = Column(DateTime)
    is_processed = Column(Boolean)


# criação da repository
class TypeSchemaEnum(Enum):
    MD_QUOTA = "md_cota"
    STAGE_RAW = "stage_raw"


class ConnectionAbstractRepository:
    def __init__(self, type_schema: str) -> None:
        self._session = Connection(type_schema).get_session()


class PlDataSourceRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.MD_QUOTA.value)

    def get_data_source_id(self, data_source_desc: str) -> int:
        source_id = (
            self._session.query(PlDataSource.data_source_id)
            .filter_by(data_source_desc=data_source_desc)
            .scalar()
        )
        logger.info("Get data_source_desc in PlDataSource success!")
        return source_id


class PlQuotaStatusRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.MD_QUOTA.value)

    def insert_quota_status(self, quota_status_insert: dict) -> Column:
        try:
            quota_status_pl_quota_status = PlQuotaStatus(
                quota_id=quota_status_insert["quota_id"],
                quota_status_type_id=quota_status_insert["quota_status_type_id"],
                valid_from=get_default_datetime(),
                created_at=get_default_datetime(),
                modified_at=get_default_datetime(),
                created_by=GLUE_DEFAULT_CODE,
                modified_by=GLUE_DEFAULT_CODE,
                is_deleted=False,
            )
            self._session.add(quota_status_pl_quota_status)
            self._session.commit()
            logger.info("Quota status inserted with sucess in PLQuotaStatus!")
            return quota_status_pl_quota_status.quota_status_id
        except DatabaseError as error:
            logger.error(
                "Error: Quota status not inserted in PLQuotaStatus:{}".format(error)
            )

    def update_quota_status(self, quota_status_to_update: dict) -> None:
        try:
            quota_status_update = (
                self._session.query(PlQuotaStatus)
                .filter_by(
                    quota_id=quota_status_to_update["quota_id"],
                    valid_to=None,
                    is_deleted=False,
                )
                .first()
            )

            quota_status_update.valid_to = (get_default_datetime(),)
            quota_status_update.modified_at = (get_default_datetime(),)
            quota_status_update.modified_by = GLUE_DEFAULT_CODE

            self._session.add(quota_status_update)
            self._session.commit()
            logger.info("Update date quota in PlQuotas success!")
        except OperationalError as error:
            logger.error("Error when search/update data in PlQuota:{}".format(error))


class PlAdministratorRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.MD_QUOTA.value)

    def read_adm(self, administrator_desc: str) -> int:
        id_adm = (
            self._session.query(PlAdministrator.administrator_id)
            .filter_by(administrator_desc=administrator_desc)
            .scalar()
        )
        logger.info("Get id_adm in PlAdministrator success!")
        return id_adm


class PlGroupRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.MD_QUOTA.value)

    def read_groups(self, adm_id: int) -> list:
        groups_md_quota = self._session.query(PlGroup).filter_by(
            administrator_id=adm_id, is_deleted=False
        )
        groups = []
        for data in groups_md_quota:
            groups.append(data.__dict__)
        logger.info("Search groups in PlGroup success!")
        return groups

    def insert_new_group(self, group_data: dict) -> Column:
        try:
            group_pl_group = PlGroup(
                group_code=group_data["group_code"],
                group_deadline=group_data["group_deadline"],
                administrator_id=group_data["administrator_id"],
                group_closing_date=group_data["group_closing_date"],
                created_at=get_default_datetime(),
                modified_at=get_default_datetime(),
                created_by=GLUE_DEFAULT_CODE,
                modified_by=GLUE_DEFAULT_CODE,
                is_deleted=False,
            )
            self._session.add(group_pl_group)
            self._session.commit()
            self._session.refresh(group_pl_group)
            logger.info("Group inserted with success in PlGroup!")
            return group_pl_group.__dict__

        except DatabaseError as error:
            logger.error("Error: Group not inserted in PlGroup:{}".format(error))


class PlQuotaRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.MD_QUOTA.value)

    def quotas_all(self, adm_id: int) -> list:
        try:
            quotas_md_quota = (
                self._session.query(PlQuota)
                .filter_by(administrator_id=adm_id, is_deleted=False)
                .all()
            )
            quotas = []
            for quota in quotas_md_quota:
                quotas.append(quota.__dict__)
            logger.info("Search all quotas in PlQuota success!")
            return quotas

        except Exception as error:
            logger.error("Error of connection with database:{}".format(error))

    def quota_code(self) -> int | None:
        try:
            max_quota_code = self._session.query(func.max(PlQuota.quota_id)).scalar()
            logger.info("Search max_quota_code in PlQuota success!")
            return max_quota_code

        except DatabaseError as error:
            logger.error("Error fetching quota code:{}".format(error))
            return None

    def insert_new_quota(self, new_quota: dict) -> Column:
        try:
            quota_pl_quota = PlQuota(
                quota_code=new_quota["quota_code"],
                external_reference=new_quota["external_reference"],
                total_installments=new_quota["total_installments"],
                is_contemplated=new_quota["is_contemplated"],
                is_multiple_ownership=new_quota["is_multiple_ownership"],
                administrator_fee=new_quota["administrator_fee"],
                fund_reservation_fee=new_quota["fund_reservation_fee"],
                info_date=new_quota["info_date"],
                quota_status_type_id=new_quota["quota_status_type_id"],
                administrator_id=new_quota["administrator_id"],
                group_id=new_quota["group_id"],
                quota_origin_id=new_quota["quota_origin_id"],
                created_at=get_default_datetime(),
                modified_at=get_default_datetime(),
                created_by=GLUE_DEFAULT_CODE,
                modified_by=GLUE_DEFAULT_CODE,
                is_deleted=False,
            )
            self._session.add(quota_pl_quota)
            self._session.commit()
            logger.info("Quota inserted in PlQuota with success!")
            return quota_pl_quota.quota_id

        except DatabaseError as error:
            logger.error("Error when inserting in PlQuota:{}".format(error))

    def insert_new_quota_from_santander_1(self, new_quota: dict) -> Column:
        try:
            quota_pl_quota = PlQuota(
                quota_code=new_quota["quota_code"],
                external_reference=new_quota["external_reference"],
                total_installments=new_quota["total_installments"],
                is_contemplated=new_quota["is_contemplated"],
                is_multiple_ownership=new_quota["is_multiple_ownership"],
                administrator_fee=new_quota["administrator_fee"],
                fund_reservation_fee=new_quota["fund_reservation_fee"],
                info_date=new_quota["info_date"],
                quota_status_type_id=new_quota["quota_status_type_id"],
                administrator_id=new_quota["administrator_id"],
                group_id=new_quota["group_id"],
                quota_origin_id=new_quota["quota_origin_id"],
                quota_number=new_quota["quota_number"],
                contract_number=new_quota["contract_number"],
                quota_person_type_id=new_quota["quota_person_type_id"],
                check_digit=new_quota["check_digit"],
                created_at=get_default_datetime(),
                modified_at=get_default_datetime(),
                created_by=GLUE_DEFAULT_CODE,
                modified_by=GLUE_DEFAULT_CODE,
                is_deleted=False,
            )
            self._session.add(quota_pl_quota)
            self._session.commit()
            self._session.refresh(quota_pl_quota)
            logger.info("Quota inserted in PlQuota with success!")
            return quota_pl_quota.__dict__

        except DatabaseError as error:
            logger.error("Error when inserting in PlQuota:{}".format(error))

    def update_quota(self, quota_to_update: dict) -> None:
        try:
            quota_update = (
                self._session.query(PlQuota)
                .filter_by(quota_id=quota_to_update["quota_id"], is_deleted=False)
                .first()
            )

            quota_update.quota_status_type_id = (
                quota_to_update["quota_status_type_id"],
            )
            quota_update.modified_at = (get_default_datetime(),)
            quota_update.modified_by = GLUE_DEFAULT_CODE

            self._session.add(quota_update)
            self._session.commit()

        except OperationalError as error:
            logger.error("Error when search/update data in PlQuota:{}".format(error))

    def update_quota_santander(self, quota_to_update: dict) -> None:
        try:
            quota_update = (
                self._session.query(PlQuota)
                .filter_by(quota_id=quota_to_update["quota_id"], is_deleted=False)
                .first()
            )

            quota_update.quota_status_type_id = (
                quota_to_update["quota_status_type_id"],
            )
            quota_update.quota_number = (quota_to_update["quota_number"],)
            quota_update.contract_number = (quota_to_update["contract_number"],)
            quota_update.check_digit = (quota_to_update["check_digit"],)
            quota_update.total_installments = (quota_to_update["total_installments"],)
            quota_update.quota_person_type_id = (
                quota_to_update["quota_person_type_id"],
            )
            quota_update.modified_at = (get_default_datetime(),)
            quota_update.modified_by = GLUE_DEFAULT_CODE

            self._session.add(quota_update)
            self._session.commit()
            logger.info("Update quota quotas in PlQuota success!")
        except OperationalError as error:
            logger.error("Error when search/update data in PlQuota:{}".format(error))

    def update_quota_code(self, quota_to_update: dict) -> None:
        try:
            quota_update = (
                self._session.query(PlQuota)
                .filter_by(quota_id=quota_to_update["quota_id"], is_deleted=False)
                .first()
            )

            quota_update.quota_code = (quota_to_update["quota_code"],)

            self._session.add(quota_update)
            self._session.commit()

        except OperationalError as error:
            logger.error("Error when search/update data in PlQuota:{}".format(error))


class PlQuotaFieldUpdateDateRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.MD_QUOTA.value)

    def insert_quota_field_update_date(
        self, quota_field_update_date_to_insert: dict
    ) -> Column:
        try:
            quota_field_update_date_pl_quota_field_update = PlQuotaFieldUpdateDate(
                update_date=quota_field_update_date_to_insert.get("update_date"),
                quota_history_field_id=quota_field_update_date_to_insert.get(
                    "quota_history_field_id"
                ),
                data_source_id=quota_field_update_date_to_insert.get("data_source_id"),
                quota_id=quota_field_update_date_to_insert.get("quota_id"),
                created_at=get_default_datetime(),
                modified_at=get_default_datetime(),
                created_by=GLUE_DEFAULT_CODE,
                modified_by=GLUE_DEFAULT_CODE,
                is_deleted=False,
            )
            self._session.add(quota_field_update_date_pl_quota_field_update)
            self._session.commit()
            logger.info(
                "Quota field update date inserted in PlQuotaFieldUpdateDate with success!"
            )
            return (
                quota_field_update_date_pl_quota_field_update.quota_field_update_date_id
            )

        except DatabaseError as error:
            logger.error(
                "Error when inserting in PlQuotaFieldUpdateDate:{}".format(error)
            )

    def update_quota_field_update_date(
        self, quota_field_update_date_to_update: dict
    ) -> None:
        try:
            quota_field_update_date_update = (
                self._session.query(PlQuotaFieldUpdateDate)
                .filter_by(
                    quota_id=quota_field_update_date_to_update["quota_id"],
                    quota_history_field_id=quota_field_update_date_to_update[
                        "quota_history_field_id"
                    ],
                    is_deleted=False,
                )
                .first()
            )

            quota_field_update_date_update.update_date = (
                quota_field_update_date_to_update["update_date"],
            )
            quota_field_update_date_update.data_source_id = (
                quota_field_update_date_to_update["data_source_id"],
            )
            quota_field_update_date_update.modified_at = (get_default_datetime(),)
            quota_field_update_date_update.modified_by = GLUE_DEFAULT_CODE

            self._session.add(quota_field_update_date_update)
            self._session.commit()
            logger.info("Update date in PlQuotaFieldUpdateDate success!")
        except OperationalError as error:
            logger.error("Error when search/update data in PlQuota:{}".format(error))
        except AttributeError:
            self.insert_quota_field_update_date(quota_field_update_date_to_update)

    def quota_field_update_date_all(self, quota_id: int) -> list:
        try:
            quotas_field_update_date_md_quota = (
                self._session.query(PlQuotaFieldUpdateDate)
                .filter_by(quota_id=quota_id, is_deleted=False)
                .all()
            )
            quotas = []
            for quota in quotas_field_update_date_md_quota:
                quotas.append(quota.__dict__)
            logger.info(
                "Search all quota history field update date in PlQuotaHistoryFieldUpdateDate success!"
            )
            return quotas

        except Exception as error:
            logger.error("Error of connection with database:{}".format(error))


class PlQuotaHistoryDetailRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.MD_QUOTA.value)

    def insert_new_quota_history(self, quota_history: dict) -> None:
        try:
            history_quota_detail = PlQuotaHistoryDetail(
                quota_history_detail_id=quota_history.get("quota_history_detail_id"),
                quota_id=quota_history.get("quota_id"),
                old_quota_number=quota_history.get("old_quota_number"),
                old_digit=quota_history.get("old_digit"),
                quota_plan=quota_history.get("quota_plan"),
                installments_paid_number=quota_history.get("installments_paid_number"),
                overdue_installments_number=quota_history.get(
                    "overdue_installments_number"
                ),
                overdue_percentage=quota_history.get("overdue_percentage"),
                per_amount_paid=quota_history.get("per_amount_paid"),
                per_mutual_fund_paid=quota_history.get("per_mutual_fund_paid"),
                per_reserve_fund_paid=quota_history.get("per_reserve_fund_paid"),
                per_adm_paid=quota_history.get("per_adm_paid"),
                per_subscription_paid=quota_history.get("per_subscription_paid"),
                per_mutual_fund_to_pay=quota_history.get("per_mutual_fund_to_pay"),
                per_reserve_fund_to_pay=quota_history.get("per_reserve_fund_to_pay"),
                per_adm_to_pay=quota_history.get("per_adm_to_pay"),
                per_subscription_to_pay=quota_history.get("per_subscription_to_pay"),
                per_insurance_to_pay=quota_history.get("per_subscription_to_pay"),
                per_install_diff_to_pay=quota_history.get("per_install_diff_to_pay"),
                per_total_amount_to_pay=quota_history.get("per_total_amount_to_pay"),
                amnt_mutual_fund_to_pay=quota_history.get("amnt_mutual_fund_to_pay"),
                amnt_reserve_fund_to_pay=quota_history.get("amnt_reserve_fund_to_pay"),
                amnt_adm_to_pay=quota_history.get("amnt_adm_to_pay"),
                amnt_subscription_to_pay=quota_history.get("amnt_subscription_to_pay"),
                amnt_insurance_to_pay=quota_history.get("amnt_insurance_to_pay"),
                amnt_fine_to_pay=quota_history.get("amnt_fine_to_pay"),
                amnt_interest_to_pay=quota_history.get("amnt_interest_to_pay"),
                amnt_others_to_pay=quota_history.get("amnt_others_to_pay"),
                amnt_install_diff_to_pay=quota_history.get("amnt_install_diff_to_pay"),
                amnt_to_pay=quota_history.get("amnt_to_pay"),
                quitter_assembly_number=quota_history.get("quitter_assembly_number"),
                cancelled_assembly_number=quota_history.get(
                    "cancelled_assembly_number"
                ),
                adjustment_date=quota_history.get("adjustment_date"),
                current_assembly_date=quota_history.get("current_assembly_date"),
                current_assembly_number=quota_history.get("current_assembly_number"),
                asset_adm_code=quota_history.get("asset_adm_code"),
                asset_description=quota_history.get("asset_description"),
                asset_value=quota_history.get("asset_value"),
                asset_type_id=quota_history.get("asset_type_id"),
                info_date=quota_history.get("info_date"),
                valid_from=quota_history.get("valid_from"),
                valid_to=quota_history.get("valid_to"),
            )
            self._session.add(history_quota_detail)
            self._session.commit()
            logger.info("Add quota detail in PlQuotaHistoryDetail success!")
        except DatabaseError as error:
            logger.error(
                "Data quota not inserted in PlQuotaHistoryDetail:{}".format(error)
            )

    def search_quota_history_detail(self, quota_id: int) -> dict:
        try:
            quota_detail = (
                self._session.query(PlQuotaHistoryDetail)
                .filter_by(quota_id=quota_id, valid_to=None, is_deleted=False)
                .first()
            )
            logger.info("Search quota_detail in PlQuotaHistoryDetail success!")
            return quota_detail.__dict__
        except DatabaseError as error:
            logger.error("Error when search data in StageRaw:{}".format(error))

    def update_valid_to(self, quota_id: int) -> None:
        try:
            quota_history_detail = (
                self._session.query(PlQuotaHistoryDetail)
                .filter_by(quota_id=quota_id, valid_to=None, is_deleted=False)
                .first()
            )

            quota_history_detail.valid_to = (get_default_datetime(),)
            quota_history_detail.modified_at = (get_default_datetime(),)
            quota_history_detail.modified_by = GLUE_DEFAULT_CODE

            self._session.add(quota_history_detail)
            self._session.commit()
            logger.info("Update valid_to in PlQuotaHistory success!")
        except OperationalError as error:
            logger.error(
                "Error when search data in PlQuotaHistoryDetail:{}".format(error)
            )


class PlQuotaOwnerRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.MD_QUOTA.value)

    def insert_new_quota_owner(self, new_quota_owner: dict) -> Column:
        try:
            quota_owner_pl_quota_owner = PlQuotaOwner(
                quota_id=new_quota_owner["quota_id"],
                ownership_percent=new_quota_owner["ownership_percent"],
                person_code=new_quota_owner["person_code"],
                main_owner=new_quota_owner["main_owner"],
                valid_from=new_quota_owner["valid_from"],
                created_at=get_default_datetime(),
                modified_at=get_default_datetime(),
                created_by=GLUE_DEFAULT_CODE,
                modified_by=GLUE_DEFAULT_CODE,
                is_deleted=False,
            )
            self._session.add(quota_owner_pl_quota_owner)
            self._session.commit()
            logger.info("Quota owner inserted in PlQuotaOwner with success!")
            return quota_owner_pl_quota_owner.quota_id
        except OperationalError as error:
            logger.error(
                "Error when trying to insert new data in PlQuotaOwner:{}".format(error)
            )

    def update_valid_to(self, quota_id: int) -> None:
        try:
            quota_owner = (
                self._session.query(PlQuotaOwner)
                .filter_by(quota_id=quota_id, valid_to=None, is_deleted=False)
                .first()
            )

            quota_owner.valid_to = (get_default_datetime(),)
            quota_owner.modified_at = (get_default_datetime(),)
            quota_owner.modified_by = GLUE_DEFAULT_CODE

            self._session.add(quota_owner)
            self._session.commit()
            logger.info("Update valid_to in quota in PlQuotaOwner!")
        except OperationalError as error:
            logger.error(
                "Error when trying to update data in PlQuotaOwner:{}".format(error)
            )


class StageRawRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.STAGE_RAW.value)

    def read_data_stage_raw(self, adm: str) -> list:
        try:
            stage_raw_beereader = (
                self._session.query(StageRaw)
                .filter_by(adm=adm, is_processed=False)
                .all()
            )
            beereader = []
            for data in stage_raw_beereader:
                beereader.append(data.__dict__)
            logger.info("Read data StageRaw success! data: {}".format(beereader))
            return beereader
        except DatabaseError as error:
            logger.error("Error when search data in StageRaw:{}".format(error))

    def update_is_processed(self, id_beereader: int) -> None:
        try:
            beereader_processed = (
                self._session.query(StageRaw)
                .filter_by(id_beereader=id_beereader)
                .first()
            )

            beereader_processed.is_processed = True
            self._session.add(beereader_processed)
            self._session.commit()
            logger.info("Update beereader is_processed=true!")
        except OperationalError as error:
            logger.error("Error when search/update data in beereader:{}".format(error))


def generate_luhn_check_digit(sequence):
    sequence = str(sequence)
    # Reverse the sequence
    reversed_sequence = sequence[::-1]

    # Double every other digit, starting with the second-to-last digit
    doubled_sequence = ""
    for i in range(len(reversed_sequence)):
        if i % 2 == 0:
            doubled_digit = int(reversed_sequence[i]) * 2
            if doubled_digit > 9:
                doubled_digit -= 9
            doubled_sequence += str(doubled_digit)
        else:
            doubled_sequence += reversed_sequence[i]

    # Add up all the digits in the doubled sequence
    sum_of_digits = sum(int(digit) for digit in doubled_sequence)

    # Calculate the Luhn check digit
    check_digit = (10 - (sum_of_digits % 10)) % 10

    return check_digit


class Etl:
    class Constants(Enum):
        CASE_DEFAULT_TYPES = 5
        CASE_DEFAULT_ASSET_TYPES = 7
        CASE_DEFAULT_HISTORY_DETAIL_FIELD = 0
        QUOTA_ORIGIN_ADM = 1
        QUOTA_ORIGIN_CUSTOMER = 2

    class QuotaHistoryFieldIdEnum(Enum):
        OLD_QUOTA_NUMBER = 1
        OLD_DIGIT = 2
        QUOTA_PLAN = 3
        INSTALLMENTS_PAID_NUMBER = 4
        OVERDUE_INSTALLMENTS_NUMBER = 5
        OVERDUE_PERCENTAGE = 6
        PER_AMOUNT_PAID = 7
        PER_MUTUAL_FUND_PAID = 8
        PER_RESERVE_FUND_PAID = 9
        PER_ADM_PAID = 10
        PER_SUBSCRIPTION_PAID = 11
        PER_MUTUAL_FUND_TO_PAY = 12
        PER_RESERVE_FUND_TO_PAY = 13
        PER_ADM_TO_PAY = 14
        PER_SUBSCRIPTION_TO_PAY = 15
        PER_INSURANCE_TO_PAY = 16
        PER_INSTALL_DIFF_TO_PAY = 17
        PER_TOTAL_AMOUNT_TO_PAY = 18
        AMNT_MUTUAL_FUND_TO_PAY = 19
        AMNT_RESERVE_FUND_TO_PAY = 20
        AMNT_ADM_TO_PAY = 21
        AMNT_SUBSCRIPTION_TO_PAY = 22
        AMNT_INSURANCE_TO_PAY = 23
        AMNT_FINE_TO_PAY = 24
        AMNT_INTEREST_TO_PAY = 25
        AMNT_OTHERS_TO_PAY = 26
        AMNT_INSTALL_DIFF_TO_PAY = 27
        AMNT_TO_PAY = 28
        QUITTER_ASSEMBLY_NUMBER = 29
        CANCELLED_ASSEMBLY_NUMBER = 30
        ADJUSTMENT_DATE = 31
        CURRENT_ASSEMBLY_DATE = 32
        CURRENT_ASSEMBLY_NUMBER = 33
        ASSET_ADM_CODE = 34
        ASSET_DESCRIPTION = 35
        ASSET_VALUE = 36
        ASSET_TYPE_ID = 37

    class EtlInfo(Enum):
        SANTANDER = "SANTANDER ADM. CONS. LTDA"
        GMAC = "GMAC ADM CONS  LTDA"
        QUOTA_CODE_PREFIX = "BZ"
        SANTANDER_1 = "SANTANDER_1"
        SANTANDER_2 = "SANTANDER_2"
        GMAC_1 = "GMAC_1"

    class StatusTypeEnum(Enum):
        EXCLUDED = 2
        DESISTENTES = 4
        ATIVOS = 1
        EM_ATRASO = 3

    class AssetTypeEnum(Enum):
        VEICULOS_PESADOS = 3
        VEICULOS_LEVES = 2
        IMOVEIS = 1
        MOTOCICLETAS = 4
        MOBILE = 5
        SERVICES = 6
        ND = 7

    switch_quota_history_field = {
        "old_quota_number": QuotaHistoryFieldIdEnum.OLD_QUOTA_NUMBER.value,
        "old_digit": QuotaHistoryFieldIdEnum.OLD_DIGIT.value,
        "quota_plan": QuotaHistoryFieldIdEnum.QUOTA_PLAN.value,
        "installments_paid_number": QuotaHistoryFieldIdEnum.INSTALLMENTS_PAID_NUMBER.value,
        "overdue_installments_number": QuotaHistoryFieldIdEnum.OVERDUE_INSTALLMENTS_NUMBER.value,
        "overdue_percentage": QuotaHistoryFieldIdEnum.OVERDUE_PERCENTAGE.value,
        "per_amount_paid": QuotaHistoryFieldIdEnum.PER_AMOUNT_PAID.value,
        "per_mutual_fund_paid": QuotaHistoryFieldIdEnum.PER_MUTUAL_FUND_PAID.value,
        "per_reserve_fund_paid": QuotaHistoryFieldIdEnum.PER_RESERVE_FUND_PAID.value,
        "per_adm_paid": QuotaHistoryFieldIdEnum.PER_ADM_PAID.value,
        "per_subscription_paid": QuotaHistoryFieldIdEnum.PER_SUBSCRIPTION_PAID.value,
        "per_mutual_fund_to_pay": QuotaHistoryFieldIdEnum.PER_MUTUAL_FUND_TO_PAY.value,
        "per_reserve_fund_to_pay": QuotaHistoryFieldIdEnum.PER_RESERVE_FUND_TO_PAY.value,
        "per_adm_to_pay": QuotaHistoryFieldIdEnum.PER_ADM_TO_PAY.value,
        "per_subscription_to_pay": QuotaHistoryFieldIdEnum.PER_SUBSCRIPTION_TO_PAY.value,
        "per_insurance_to_pay": QuotaHistoryFieldIdEnum.PER_INSURANCE_TO_PAY.value,
        "per_install_diff_to_pay": QuotaHistoryFieldIdEnum.PER_INSTALL_DIFF_TO_PAY.value,
        "per_total_amount_to_pay": QuotaHistoryFieldIdEnum.PER_TOTAL_AMOUNT_TO_PAY.value,
        "amnt_mutual_fund_to_pay": QuotaHistoryFieldIdEnum.AMNT_MUTUAL_FUND_TO_PAY.value,
        "amnt_reserve_fund_to_pay": QuotaHistoryFieldIdEnum.AMNT_RESERVE_FUND_TO_PAY.value,
        "amnt_adm_to_pay": QuotaHistoryFieldIdEnum.AMNT_ADM_TO_PAY.value,
        "amnt_subscription_to_pay": QuotaHistoryFieldIdEnum.AMNT_SUBSCRIPTION_TO_PAY.value,
        "amnt_insurance_to_pay": QuotaHistoryFieldIdEnum.AMNT_INSURANCE_TO_PAY.value,
        "amnt_fine_to_pay": QuotaHistoryFieldIdEnum.AMNT_FINE_TO_PAY.value,
        "amnt_interest_to_pay": QuotaHistoryFieldIdEnum.AMNT_INTEREST_TO_PAY.value,
        "amnt_others_to_pay": QuotaHistoryFieldIdEnum.AMNT_OTHERS_TO_PAY.value,
        "amnt_install_diff_to_pay": QuotaHistoryFieldIdEnum.AMNT_INSTALL_DIFF_TO_PAY.value,
        "amnt_to_pay": QuotaHistoryFieldIdEnum.AMNT_TO_PAY.value,
        "quitter_assembly_number": QuotaHistoryFieldIdEnum.QUITTER_ASSEMBLY_NUMBER.value,
        "cancelled_assembly_number": QuotaHistoryFieldIdEnum.CANCELLED_ASSEMBLY_NUMBER.value,
        "adjustment_date": QuotaHistoryFieldIdEnum.ADJUSTMENT_DATE.value,
        "current_assembly_date": QuotaHistoryFieldIdEnum.CURRENT_ASSEMBLY_DATE.value,
        "current_assembly_number": QuotaHistoryFieldIdEnum.CURRENT_ASSEMBLY_NUMBER.value,
        "asset_adm_code": QuotaHistoryFieldIdEnum.ASSET_ADM_CODE.value,
        "asset_description": QuotaHistoryFieldIdEnum.ASSET_DESCRIPTION.value,
        "asset_value": QuotaHistoryFieldIdEnum.ASSET_VALUE.value,
        "asset_type_id": QuotaHistoryFieldIdEnum.ASSET_TYPE_ID.value,
    }

    switch_asset_type_santander_1 = {
        "auto": AssetTypeEnum.VEICULOS_LEVES.value,
        "automóvelseguradora:356": AssetTypeEnum.VEICULOS_LEVES.value,
        "camin": AssetTypeEnum.VEICULOS_PESADOS.value,
        "eletro": AssetTypeEnum.ND.value,
        "imovel": AssetTypeEnum.IMOVEIS.value,
        "moto": AssetTypeEnum.MOTOCICLETAS.value,
        "servic": AssetTypeEnum.SERVICES.value,
    }

    switch_asset_type_santander_2 = {
        "auto  automóv": AssetTypeEnum.VEICULOS_LEVES.value,
        "auto  automóveis": AssetTypeEnum.VEICULOS_LEVES.value,
        "auto automóveis": AssetTypeEnum.VEICULOS_LEVES.value,
        "auto  automóveis / pesados": AssetTypeEnum.VEICULOS_PESADOS.value,
        "camin pesados / agro": AssetTypeEnum.VEICULOS_PESADOS.value,
        "eletroeletroeletrônicos": AssetTypeEnum.ND.value,
        "imovel imóveis": AssetTypeEnum.IMOVEIS.value,
        "imovelimóveis": AssetTypeEnum.IMOVEIS.value,
        "moto  motocicletas": AssetTypeEnum.MOTOCICLETAS.value,
        "servic servicos": AssetTypeEnum.SERVICES.value,
        "servicservicos": AssetTypeEnum.SERVICES.value,
        "servicserviços": AssetTypeEnum.SERVICES.value,
    }

    switch_status_santander_1 = {
        "cobradora funchal": StatusTypeEnum.EXCLUDED.value,
        "contemplado s-bem 1 p atraso": StatusTypeEnum.EM_ATRASO.value,
        "contemplado s-bem 2 p atraso": StatusTypeEnum.EM_ATRASO.value,
        "contemplado s-bem 3 p atraso": StatusTypeEnum.EM_ATRASO.value,
        "escritorio funchal": StatusTypeEnum.EXCLUDED.value,
        "excluido": StatusTypeEnum.EXCLUDED.value,
        "excluido -dev bloq judicialmen": StatusTypeEnum.EXCLUDED.value,
        "não contemplado 1 p atraso": StatusTypeEnum.EM_ATRASO.value,
        "não contemplado 2 p atraso": StatusTypeEnum.EM_ATRASO.value,
        "não contemplado 3 p atraso": StatusTypeEnum.EM_ATRASO.value,
        "não contemplado 4 p atraso": StatusTypeEnum.EM_ATRASO.value,
        "normal": StatusTypeEnum.ATIVOS.value,
        "quitado": StatusTypeEnum.ATIVOS.value,
    }

    switch_status_santander_2 = {
        "contemplado s-bem 1 p atraso": StatusTypeEnum.EM_ATRASO.value,
        "contemplado s-bem 2 p atraso": StatusTypeEnum.EM_ATRASO.value,
        "contemplado s-bem 3 p atraso": StatusTypeEnum.EM_ATRASO.value,
        "contemplado s-bem 4 p atraso": StatusTypeEnum.EM_ATRASO.value,
        "excluido": StatusTypeEnum.EXCLUDED.value,
        "não contemplado 1 p atraso": StatusTypeEnum.EM_ATRASO.value,
        "não contemplado 2 p atraso": StatusTypeEnum.EM_ATRASO.value,
        "não contemplado 3 p atraso": StatusTypeEnum.EM_ATRASO.value,
        "não contemplado 4 p atraso": StatusTypeEnum.EM_ATRASO.value,
        "normal": StatusTypeEnum.ATIVOS.value,
        "quitado": StatusTypeEnum.ATIVOS.value,
    }

    switch_status_gmac_1 = {
        "carta de cobrança emitida": StatusTypeEnum.EM_ATRASO.value,
        "cota sendo recuperada": StatusTypeEnum.EM_ATRASO.value,
        "desistente": StatusTypeEnum.DESISTENTES.value,
        "excluido": StatusTypeEnum.EXCLUDED.value,
        "normal": StatusTypeEnum.ATIVOS.value,
    }

    switch_status = {
        "ATIVOS": StatusTypeEnum.ATIVOS.value,
        "DESISTENTES": StatusTypeEnum.DESISTENTES.value,
        "EXCLUIDOS": StatusTypeEnum.EXCLUDED.value,
        "EM ATRASO": StatusTypeEnum.EM_ATRASO.value,
    }

    switch_history_field = {
        "per_adm_paid": "pago_perc_adm",
        "per_adm_to_pay": "a_pagar_perc_adm",
        "per_amount_paid": "pago_perc_total",
        "per_mutual_fund_paid": "pago_perc_fc_1",
        "per_reserve_fund_paid": "pago_perc_fr",
        "per_mutual_fund_to_pay": "a_pagar_perc_fc",
        "per_reserve_fund_to_pay": "a_pagar_perc_fr",
        "per_install_diff_to_pay": "a_pagar_perc_dif_parcela",
        "per_total_amount_to_pay": "a_pagar_perc_total",
        "amnt_mutual_fund_to_pay": "a_pagar_vl_fc",
        "amnt_reserve_fund_to_pay": "a_pagar_vl_fr",
        "amnt_adm_to_pay": "a_pagar_vl_adm",
        "amnt_install_diff_to_pay": "a_pagar_vl_dif_parcela",
        "amnt_to_pay": "a_pagar_vl_total",
        "old_quota_number": "cota",
        "old_digit": "digito",
        "adjustment_date": "dt_reajuste_em",
        "asset_description": "bem",
        "asset_adm_code": "cd_bem",
        "asset_value": "vl_bem",
        "asset_type_id": "asset_type",
        "info_date": "dt_extrato",
        "amnt_fine_to_pay": "a_pagar_vl_multa",
        "amnt_interest_to_pay": "a_pagar_vl_juros",
        "amnt_others_to_pay": "a_pagar_vl_outros",
        "per_insurance_to_pay": "a_pagar_perc_seguros",
        "amnt_insurance_to_pay": "a_pagar_vl_seguros",
        "per_subscription_to_pay": "a_pagar_perc_adesao",
        "amnt_subscription_to_pay": "a_pagar_vl_adesao",
    }

    def __init__(self):
        self.pl_administrator = PlAdministratorRepository()
        self.pl_group = PlGroupRepository()
        self.pl_quota = PlQuotaRepository()
        self.stage_raw = StageRawRepository()
        self.pl_quota_status = PlQuotaStatusRepository()
        self.pl_quota_history_detail = PlQuotaHistoryDetailRepository()
        self.pl_quota_field_update_date = PlQuotaFieldUpdateDateRepository()
        self.pl_data_source = PlDataSourceRepository()
        self.pl_quota_owner = PlQuotaOwnerRepository()

        self.id_adm_gmac = self.pl_administrator.read_adm(self.EtlInfo.GMAC.value)
        self.id_adm_santander = self.pl_administrator.read_adm(
            self.EtlInfo.SANTANDER.value
        )
        self.groups_md_quota_gmac = self.pl_group.read_groups(self.id_adm_gmac)
        self.groups_md_quota_santander = self.pl_group.read_groups(
            self.id_adm_santander
        )
        self.quota_code_md_quota = self.pl_quota.quota_code()
        self.quotas_gmac_md_quota = self.pl_quota.quotas_all(self.id_adm_gmac)
        self.quotas_santander_md_quota = self.pl_quota.quotas_all(self.id_adm_santander)
        self.statement_santander_1 = self.stage_raw.read_data_stage_raw(
            self.EtlInfo.SANTANDER_1.value
        )
        self.statement_santander_2 = self.stage_raw.read_data_stage_raw(
            self.EtlInfo.SANTANDER_2.value
        )
        self.statement_gmac_1 = self.stage_raw.read_data_stage_raw(
            self.EtlInfo.GMAC_1.value
        )
        self.data_source_id = self.pl_data_source.get_data_source_id(
            "ACCOUNT STATEMENT"
        )

    def start(self):
        self.santander_1_flow()
        self.santander_2_flow()
        self.gmac_1_flow()

    @staticmethod
    def get_dict_by_id(id_item: str, data_list: list, field_name: str) -> dict | None:
        for item_list in data_list:
            if str(item_list[field_name]) == id_item:
                return item_list

        return None

    @staticmethod
    def string_right_justified(group: str) -> str:
        if len(str(group)) == 5:
            code_group = str(group)
        else:
            code_group = str(group).rjust(5, "0")
        return code_group

    def santander_1_flow(self):
        for row in self.statement_santander_1:
            request_body = json.loads(row["quota_data"])
            code_group = self.string_right_justified(request_body["grupo"])
            md_quota_group = self.get_dict_by_id(
                code_group, self.groups_md_quota_santander, "group_code"
            )
            md_quota_quota = self.get_dict_by_id(
                str(request_body["contrato"]),
                self.quotas_santander_md_quota,
                "external_reference",
            )

            if md_quota_quota is None:
                quota_code_tb = str(self.quota_code_md_quota + 1).rjust(6, "0")
                self.quota_code_md_quota = self.quota_code_md_quota + 1
                quota_code_suffix = str(generate_luhn_check_digit(quota_code_tb))
                quota_code_final = (
                    self.EtlInfo.QUOTA_CODE_PREFIX.value
                    + quota_code_tb
                    + quota_code_suffix
                )

                if md_quota_group is None:
                    assembly_date = datetime.strptime(
                        request_body["dt_assembleia_atual"], "%Y-%m-%d"
                    ).date()
                    today = datetime.today()

                    assembly_since_statement = relativedelta.relativedelta(
                        today, assembly_date
                    ).months
                    total_assembly = (
                        request_body["nr_assembleia_atual"] + assembly_since_statement
                    )
                    group_deadline = request_body["plano_basico"]

                    assembly_to_end = group_deadline - total_assembly

                    group_end_date = today + relativedelta.relativedelta(
                        months=assembly_to_end
                    )

                    group_to_insert = {
                        "group_code": code_group,
                        "group_deadline": group_deadline,
                        "administrator_id": self.id_adm_santander,
                        "group_closing_date": group_end_date,
                    }
                    group_md_quota = self.pl_group.insert_new_group(group_to_insert)
                    group_id_md_quota = group_md_quota["group_id"]
                    self.groups_md_quota_santander.append(group_md_quota)

                else:
                    group_id_md_quota = md_quota_group["group_id"]

                person_code = request_body["person_code"]
                person_quota_type = None
                endpoint_url = "https://cubees.bazar-sandbox.technology/customer"
                header = {"x-api-key": "bDtuPmlYL65tP4lmVxWob6gygrBhO22vD4Cv5F1h"}
                params = {"person_code": person_code}
                try:
                    response = requests.get(
                        url=endpoint_url, params=params, headers=header
                    )
                    status_code = response.status_code
                    if status_code == 200:
                        customer = response.json()["person_type_data"]
                        if customer["natural_person_id"]:
                            person_quota_type = 1
                        else:
                            person_quota_type = 2

                except requests.exceptions.HTTPError as error:
                    logger.error("Error when call cubees api:{}".format(error))

                status_type = self.switch_status_santander_1.get(
                    request_body["situacao"], self.Constants.CASE_DEFAULT_TYPES.value
                )
                quota_to_insert = {
                    "quota_code": quota_code_final,
                    "external_reference": request_body["contrato"],
                    "total_installments": request_body["plano_basico"],
                    "is_contemplated": False,
                    "is_multiple_ownership": False,
                    "administrator_fee": request_body["tx_adm"] * 100,
                    "fund_reservation_fee": request_body["tx_fr"] * 100,
                    "info_date": request_body["dt_extrato"],
                    "quota_status_type_id": status_type,
                    "administrator_id": self.id_adm_santander,
                    "group_id": group_id_md_quota,
                    "quota_origin_id": self.Constants.QUOTA_ORIGIN_CUSTOMER.value,
                    "contract_number": request_body["contrato"],
                    "quota_number": request_body["cota"],
                    "check_digit": request_body["digito"],
                    "quota_person_type_id": person_quota_type,
                }

                quota_md_quota = self.pl_quota.insert_new_quota_from_santander_1(
                    quota_to_insert
                )
                quota_id_md_quota = quota_md_quota["quota_id"]
                self.quotas_santander_md_quota.append(quota_md_quota)

                quota_code_insert = str(quota_id_md_quota).rjust(6, "0")
                quota_code_update = str(generate_luhn_check_digit(quota_code_insert))
                quota_code_final_update = (
                    self.EtlInfo.QUOTA_CODE_PREFIX.value
                    + quota_code_insert
                    + quota_code_update
                )
                quota_code_update = {
                    "quota_id": quota_id_md_quota,
                    "quota_code": quota_code_final_update,
                }

                self.pl_quota.update_quota_code(quota_code_update)

                quota_status_to_insert = {
                    "quota_id": quota_id_md_quota,
                    "quota_status_type_id": status_type,
                }

                self.pl_quota_status.insert_quota_status(quota_status_to_insert)

                asset_type = self.switch_asset_type_santander_1.get(
                    request_body["tipo_bem"],
                    self.Constants.CASE_DEFAULT_ASSET_TYPES.value,
                )
                quota_history_detail_to_insert = {}
                for keyword in self.switch_quota_history_field:
                    quota_history_detail_to_insert[keyword] = None

                quota_history_detail_to_insert["quota_id"] = quota_id_md_quota
                quota_history_detail_to_insert["per_adm_paid"] = (
                    request_body["pago_perc_adm"] * 100
                )
                quota_history_detail_to_insert["per_adm_to_pay"] = (
                    request_body["a_pagar_perc_adm"] * 100
                )
                quota_history_detail_to_insert["per_amount_paid"] = (
                    request_body["pago_perc_total"] * 100
                )
                quota_history_detail_to_insert["per_mutual_fund_paid"] = (
                    request_body["pago_perc_fc_1"] * 100
                )
                quota_history_detail_to_insert["per_reserve_fund_paid"] = (
                    request_body["pago_perc_fr"] * 100
                )
                quota_history_detail_to_insert["per_mutual_fund_to_pay"] = (
                    request_body["a_pagar_perc_fc"] * 100
                )
                quota_history_detail_to_insert["per_reserve_fund_to_pay"] = (
                    request_body["a_pagar_perc_fr"] * 100
                )
                quota_history_detail_to_insert["per_install_diff_to_pay"] = (
                    request_body["a_pagar_perc_dif_parcela"] * 100
                )
                quota_history_detail_to_insert["per_total_amount_to_pay"] = (
                    request_body["a_pagar_perc_total"] * 100
                )
                quota_history_detail_to_insert[
                    "amnt_mutual_fund_to_pay"
                ] = request_body["a_pagar_vl_fc"]
                quota_history_detail_to_insert[
                    "amnt_reserve_fund_to_pay"
                ] = request_body["a_pagar_vl_fr"]
                quota_history_detail_to_insert["amnt_adm_to_pay"] = request_body[
                    "a_pagar_vl_adm"
                ]
                quota_history_detail_to_insert[
                    "amnt_install_diff_to_pay"
                ] = request_body["a_pagar_vl_dif_parcela"]
                quota_history_detail_to_insert["amnt_to_pay"] = request_body[
                    "a_pagar_vl_total"
                ]
                quota_history_detail_to_insert["old_quota_number"] = request_body[
                    "cota"
                ]
                quota_history_detail_to_insert["old_digit"] = request_body["digito"]
                quota_history_detail_to_insert["adjustment_date"] = request_body[
                    "dt_reajuste_em"
                ]
                quota_history_detail_to_insert["asset_description"] = request_body[
                    "bem"
                ]
                quota_history_detail_to_insert["asset_adm_code"] = request_body[
                    "cd_bem"
                ]
                quota_history_detail_to_insert["asset_value"] = request_body["vl_bem"]
                quota_history_detail_to_insert["asset_type_id"] = asset_type
                quota_history_detail_to_insert["info_date"] = request_body["dt_extrato"]
                quota_history_detail_to_insert["amnt_fine_to_pay"] = request_body[
                    "a_pagar_vl_multa"
                ]
                quota_history_detail_to_insert["amnt_interest_to_pay"] = request_body[
                    "a_pagar_vl_juros"
                ]
                quota_history_detail_to_insert["amnt_others_to_pay"] = request_body[
                    "a_pagar_vl_outros"
                ]
                quota_history_detail_to_insert["per_insurance_to_pay"] = request_body[
                    "a_pagar_perc_seguros"
                ]
                quota_history_detail_to_insert["amnt_insurance_to_pay"] = request_body[
                    "a_pagar_vl_seguros"
                ]
                quota_history_detail_to_insert[
                    "per_subscription_to_pay"
                ] = request_body["a_pagar_perc_adesao"]
                quota_history_detail_to_insert[
                    "amnt_subscription_to_pay"
                ] = request_body["a_pagar_vl_adesao"]
                quota_history_detail_to_insert["valid_from"] = get_default_datetime()
                quota_history_detail_to_insert["valid_to"] = None

                self.pl_quota_history_detail.insert_new_quota_history(
                    quota_history_detail_to_insert
                )

                fields_inserted_quota_history = [
                    "amnt_fine_to_pay",
                    "amnt_interest_to_pay",
                    "amnt_others_to_pay",
                    "per_insurance_to_pay",
                    "amnt_insurance_to_pay",
                    "per_subscription_to_pay",
                    "amnt_subscription_to_pay",
                    "old_quota_number",
                    "old_digit",
                    "per_amount_paid",
                    "per_mutual_fund_paid",
                    "per_reserve_fund_paid",
                    "per_adm_paid",
                    "per_mutual_fund_to_pay",
                    "per_reserve_fund_to_pay",
                    "per_install_diff_to_pay",
                    "per_adm_to_pay",
                    "per_total_amount_to_pay",
                    "amnt_mutual_fund_to_pay",
                    "amnt_reserve_fund_to_pay",
                    "amnt_adm_to_pay",
                    "amnt_install_diff_to_pay",
                    "amnt_to_pay",
                    "adjustment_date",
                    "asset_adm_code",
                    "asset_description",
                    "asset_value",
                    "asset_type_id",
                ]

                for field in fields_inserted_quota_history:
                    history_field_id = self.switch_quota_history_field.get(
                        field, self.Constants.CASE_DEFAULT_HISTORY_DETAIL_FIELD.value
                    )
                    quota_field_update_date_insert = {
                        "update_date": request_body["dt_extrato"],
                        "quota_history_field_id": history_field_id,
                        "data_source_id": self.data_source_id,
                        "quota_id": quota_id_md_quota,
                    }

                    self.pl_quota_field_update_date.insert_quota_field_update_date(
                        quota_field_update_date_insert
                    )

                quota_owner_to_insert = {
                    "ownership_percent": 1,
                    "quota_id": quota_id_md_quota,
                    "person_code": person_code,
                    "main_owner": True,
                    "valid_from": get_default_datetime(),
                }

                self.pl_quota_owner.insert_new_quota_owner(quota_owner_to_insert)

            else:
                quota_id_md_quota = md_quota_quota["quota_id"]
                date_str = request_body["dt_extrato"]
                date_format = "%Y-%m-%d"
                data_extrato = datetime.strptime(date_str, date_format)
                status_type = self.switch_status_santander_1.get(
                    request_body["situacao"], self.Constants.CASE_DEFAULT_TYPES.value
                )
                person_code = request_body["person_code"]
                person_quota_type = None
                endpoint_url = "https://cubees.bazar-sandbox.technology/customer"
                header = {"x-api-key": "bDtuPmlYL65tP4lmVxWob6gygrBhO22vD4Cv5F1h"}
                params = {"person_code": person_code}
                try:
                    response = requests.get(
                        url=endpoint_url, params=params, headers=header
                    )
                    if response.status_code == 200:
                        customer = response.json()["person_type_data"]
                        if customer["natural_person_id"]:
                            person_quota_type = 1
                        else:
                            person_quota_type = 2

                except requests.exceptions.HTTPError as error:
                    logger.error("Error when call cubees api:{}".format(error))

                if md_quota_quota["info_date"] < data_extrato:
                    quota_update = {
                        "quota_id": quota_id_md_quota,
                        "external_reference": request_body["contrato"],
                        "total_installments": request_body["plano_basico"],
                        "is_contemplated": False,
                        "is_multiple_ownership": False,
                        "administrator_fee": (request_body["tx_adm"] * 100),
                        "fund_reservation_fee": (request_body["tx_fr"] * 100),
                        "info_date": request_body["dt_extrato"],
                        "quota_status_type_id": status_type,
                        "contract_number": request_body["contrato"],
                        "quota_number": request_body["cota"],
                        "check_digit": request_body["digito"],
                        "quota_person_type_id": person_quota_type,
                    }

                    self.pl_quota.update_quota_santander(quota_update)

                    if md_quota_quota["quota_status_type_id"] != status_type:
                        quota_status_to_update = {"quota_id": quota_id_md_quota}

                        self.pl_quota_status.update_quota_status(quota_status_to_update)

                        quota_status_to_insert = {
                            "quota_id": quota_id_md_quota,
                            "quota_status_type_id": status_type,
                        }

                        self.pl_quota_status.insert_quota_status(quota_status_to_insert)

                    self.pl_quota_owner.update_valid_to(quota_id_md_quota)

                    quota_owner_to_insert = {
                        "ownership_percent": 1,
                        "quota_id": quota_id_md_quota,
                        "person_code": person_code,
                        "main_owner": True,
                        "valid_from": get_default_datetime(),
                    }

                    self.pl_quota_owner.insert_new_quota_owner(quota_owner_to_insert)

                quota_history_detail_md_quota = (
                    self.pl_quota_history_detail.search_quota_history_detail(
                        quota_id_md_quota
                    )
                )

                if quota_history_detail_md_quota["info_date"] < data_extrato:
                    logger.info("Fluxo de atualização de informação")

                    self.pl_quota_history_detail.update_valid_to(quota_id_md_quota)

                    asset_type = self.switch_asset_type_santander_1.get(
                        request_body["tipo_bem"],
                        self.Constants.CASE_DEFAULT_ASSET_TYPES.value,
                    )

                    quota_history_detail_to_insert = {}
                    for keyword in self.switch_quota_history_field:
                        logger.info(keyword)
                        logger.info(quota_history_detail_md_quota)
                        quota_history_detail_to_insert[
                            keyword
                        ] = quota_history_detail_md_quota[keyword]

                    quota_history_detail_to_insert["quota_id"] = quota_id_md_quota
                    quota_history_detail_to_insert["per_adm_paid"] = (
                        request_body["pago_perc_adm"] * 100
                    )
                    quota_history_detail_to_insert["per_adm_to_pay"] = (
                        request_body["a_pagar_perc_adm"] * 100
                    )
                    quota_history_detail_to_insert["per_amount_paid"] = (
                        request_body["pago_perc_total"] * 100
                    )
                    quota_history_detail_to_insert["per_mutual_fund_paid"] = (
                        request_body["pago_perc_fc_1"] * 100
                    )
                    quota_history_detail_to_insert["per_reserve_fund_paid"] = (
                        request_body["pago_perc_fr"] * 100
                    )
                    quota_history_detail_to_insert["per_mutual_fund_to_pay"] = (
                        request_body["a_pagar_perc_fc"] * 100
                    )
                    quota_history_detail_to_insert["per_reserve_fund_to_pay"] = (
                        request_body["a_pagar_perc_fr"] * 100
                    )
                    quota_history_detail_to_insert["per_install_diff_to_pay"] = (
                        request_body["a_pagar_perc_dif_parcela"] * 100
                    )
                    quota_history_detail_to_insert["per_total_amount_to_pay"] = (
                        request_body["a_pagar_perc_total"] * 100
                    )
                    quota_history_detail_to_insert[
                        "amnt_mutual_fund_to_pay"
                    ] = request_body["a_pagar_vl_fc"]
                    quota_history_detail_to_insert[
                        "amnt_reserve_fund_to_pay"
                    ] = request_body["a_pagar_vl_fr"]
                    quota_history_detail_to_insert["amnt_adm_to_pay"] = request_body[
                        "a_pagar_vl_adm"
                    ]
                    quota_history_detail_to_insert[
                        "amnt_install_diff_to_pay"
                    ] = request_body["a_pagar_vl_dif_parcela"]
                    quota_history_detail_to_insert["amnt_to_pay"] = request_body[
                        "a_pagar_vl_total"
                    ]
                    quota_history_detail_to_insert["old_quota_number"] = request_body[
                        "cota"
                    ]
                    quota_history_detail_to_insert["old_digit"] = request_body["digito"]
                    quota_history_detail_to_insert["adjustment_date"] = request_body[
                        "dt_reajuste_em"
                    ]
                    quota_history_detail_to_insert["asset_description"] = request_body[
                        "bem"
                    ]
                    quota_history_detail_to_insert["asset_adm_code"] = request_body[
                        "cd_bem"
                    ]
                    quota_history_detail_to_insert["asset_value"] = request_body[
                        "vl_bem"
                    ]
                    quota_history_detail_to_insert["asset_type_id"] = asset_type
                    quota_history_detail_to_insert["info_date"] = request_body[
                        "dt_extrato"
                    ]
                    quota_history_detail_to_insert["amnt_fine_to_pay"] = request_body[
                        "a_pagar_vl_multa"
                    ]
                    quota_history_detail_to_insert[
                        "amnt_interest_to_pay"
                    ] = request_body["a_pagar_vl_juros"]
                    quota_history_detail_to_insert["amnt_others_to_pay"] = request_body[
                        "a_pagar_vl_outros"
                    ]
                    quota_history_detail_to_insert[
                        "per_insurance_to_pay"
                    ] = request_body["a_pagar_perc_seguros"]
                    quota_history_detail_to_insert[
                        "amnt_insurance_to_pay"
                    ] = request_body["a_pagar_vl_seguros"]
                    quota_history_detail_to_insert[
                        "per_subscription_to_pay"
                    ] = request_body["a_pagar_perc_adesao"]
                    quota_history_detail_to_insert[
                        "amnt_subscription_to_pay"
                    ] = request_body["a_pagar_vl_adesao"]
                    quota_history_detail_to_insert[
                        "valid_from"
                    ] = get_default_datetime()
                    quota_history_detail_to_insert["valid_to"] = None

                    self.pl_quota_history_detail.insert_new_quota_history(
                        quota_history_detail_to_insert
                    )

                    fields_updated_quota_history = [
                        "amnt_fine_to_pay",
                        "amnt_interest_to_pay",
                        "amnt_others_to_pay",
                        "per_insurance_to_pay",
                        "amnt_insurance_to_pay",
                        "per_subscription_to_pay",
                        "amnt_subscription_to_pay",
                        "old_quota_number",
                        "old_digit",
                        "per_amount_paid",
                        "per_mutual_fund_paid",
                        "per_reserve_fund_paid",
                        "per_adm_paid",
                        "per_mutual_fund_to_pay",
                        "per_reserve_fund_to_pay",
                        "per_install_diff_to_pay",
                        "per_adm_to_pay",
                        "per_total_amount_to_pay",
                        "amnt_mutual_fund_to_pay",
                        "amnt_reserve_fund_to_pay",
                        "amnt_adm_to_pay",
                        "amnt_install_diff_to_pay",
                        "amnt_to_pay",
                        "adjustment_date",
                        "asset_adm_code",
                        "asset_description",
                        "asset_value",
                        "asset_type_id",
                    ]

                    for field in fields_updated_quota_history:
                        history_field_id = self.switch_quota_history_field.get(
                            field,
                            self.Constants.CASE_DEFAULT_HISTORY_DETAIL_FIELD.value,
                        )

                        quota_field_update_date_to_update = {
                            "update_date": request_body["dt_extrato"],
                            "quota_history_field_id": history_field_id,
                            "data_source_id": self.data_source_id,
                            "quota_id": quota_id_md_quota,
                        }

                        self.pl_quota_field_update_date.update_quota_field_update_date(
                            quota_field_update_date_to_update
                        )

                else:
                    logger.info(
                        "Fluxo de verificação de informações do extrato mesmo que antigas"
                    )
                    md_quota_history_field_update_date = (
                        self.pl_quota_field_update_date.quota_field_update_date_all(
                            quota_id_md_quota
                        )
                    )
                    fields_updated_quota_history = [
                        "amnt_fine_to_pay",
                        "amnt_interest_to_pay",
                        "amnt_others_to_pay",
                        "per_insurance_to_pay",
                        "amnt_insurance_to_pay",
                        "per_subscription_to_pay",
                        "amnt_subscription_to_pay",
                        "old_quota_number",
                        "old_digit",
                        "per_amount_paid",
                        "per_mutual_fund_paid",
                        "per_reserve_fund_paid",
                        "per_adm_paid",
                        "per_mutual_fund_to_pay",
                        "per_reserve_fund_to_pay",
                        "per_install_diff_to_pay",
                        "per_adm_to_pay",
                        "per_total_amount_to_pay",
                        "amnt_mutual_fund_to_pay",
                        "amnt_reserve_fund_to_pay",
                        "amnt_adm_to_pay",
                        "amnt_install_diff_to_pay",
                        "amnt_to_pay",
                        "adjustment_date",
                        "asset_adm_code",
                        "asset_description",
                        "asset_value",
                        "asset_type_id",
                    ]
                    asset_type = self.switch_asset_type_santander_1.get(
                        request_body["tipo_bem"],
                        self.Constants.CASE_DEFAULT_ASSET_TYPES.value,
                    )
                    quota_history_detail_md_quota["asset_type_id"] = asset_type

                    quota_history_detail_to_insert = {}
                    for keyword in self.switch_quota_history_field:
                        logger.info(keyword)
                        logger.info(quota_history_detail_md_quota)
                        quota_history_detail_to_insert[
                            keyword
                        ] = quota_history_detail_md_quota[keyword]
                    for field in fields_updated_quota_history:
                        history_field_id = self.switch_quota_history_field.get(
                            field,
                            self.Constants.CASE_DEFAULT_HISTORY_DETAIL_FIELD.value,
                        )
                        request_field = self.switch_history_field.get(field)
                        if quota_history_detail_md_quota[field] is None:
                            quota_history_detail_to_insert[field] = request_body[
                                request_field
                            ]
                            quota_field_update_date_to_update = {
                                "update_date": request_body["dt_extrato"],
                                "quota_history_field_id": history_field_id,
                                "data_source_id": self.data_source_id,
                                "quota_id": quota_id_md_quota,
                            }

                            self.pl_quota_field_update_date.update_quota_field_update_date(
                                quota_field_update_date_to_update
                            )

                        else:
                            history_field_update_date = self.get_dict_by_id(
                                str(history_field_id),
                                md_quota_history_field_update_date,
                                "quota_history_field_id",
                            )
                            logger.info(
                                "todos registros banco {}".format(
                                    md_quota_history_field_update_date
                                )
                            )
                            logger.info("history field id {}".format(history_field_id))
                            logger.info(
                                "registro banco {}".format(history_field_update_date)
                            )
                            if history_field_update_date["update_date"] < data_extrato:
                                quota_history_detail_to_insert[field] = request_body[
                                    request_field
                                ]
                                quota_field_update_date_to_update = {
                                    "update_date": request_body["dt_extrato"],
                                    "quota_history_field_id": history_field_id,
                                    "data_source_id": self.data_source_id,
                                    "quota_id": quota_id_md_quota,
                                }

                                self.pl_quota_field_update_date.update_quota_field_update_date(
                                    quota_field_update_date_to_update
                                )

            self.stage_raw.update_is_processed(row["id_beereader"])

    def santander_2_flow(self):
        for row in self.statement_santander_2:
            request_body = json.loads(row["quota_data"])
            code_group = self.string_right_justified(request_body["grupo"])
            md_quota_group = self.get_dict_by_id(
                code_group, self.groups_md_quota_santander, "group_code"
            )
            md_quota_quota = self.get_dict_by_id(
                str(request_body["contrato"]),
                self.quotas_santander_md_quota,
                "external_reference",
            )

            if md_quota_quota is None:
                quota_code_tb = str(self.quota_code_md_quota + 1).rjust(6, "0")
                self.quota_code_md_quota = self.quota_code_md_quota + 1
                quota_code_suffix = str(generate_luhn_check_digit(quota_code_tb))
                quota_code_final = (
                    self.EtlInfo.QUOTA_CODE_PREFIX.value
                    + quota_code_tb
                    + quota_code_suffix
                )

                if md_quota_group is None:
                    assembly_date = datetime.strptime(
                        request_body["dt_assembleia_atual"], "%Y-%m-%d"
                    ).date()
                    today = datetime.today()

                    assembly_since_statement = relativedelta.relativedelta(
                        today, assembly_date
                    ).months
                    total_assembly = (
                        request_body["nr_assembleia_atual"] + assembly_since_statement
                    )
                    group_deadline = request_body["plano_basico"]

                    assembly_to_end = group_deadline - total_assembly

                    group_end_date = today + relativedelta.relativedelta(
                        months=assembly_to_end
                    )

                    group_to_insert = {
                        "group_code": code_group,
                        "group_deadline": group_deadline,
                        "administrator_id": self.id_adm_santander,
                        "group_closing_date": group_end_date,
                    }
                    group_md_quota = self.pl_group.insert_new_group(group_to_insert)
                    group_id_md_quota = group_md_quota["group_id"]
                    self.groups_md_quota_santander.append(group_md_quota)

                else:
                    group_id_md_quota = md_quota_group["group_id"]

                person_code = request_body["person_code"]
                person_quota_type = None
                endpoint_url = "https://cubees.bazar-sandbox.technology/customer"
                header = {"x-api-key": "bDtuPmlYL65tP4lmVxWob6gygrBhO22vD4Cv5F1h"}
                params = {"person_code": person_code}
                try:
                    response = requests.get(
                        url=endpoint_url, params=params, headers=header
                    )
                    if response.status_code == 200:
                        customer = response.json()["person_type_data"]
                        if customer["natural_person_id"]:
                            person_quota_type = 1
                        else:
                            person_quota_type = 2

                except requests.exceptions.HTTPError as error:
                    logger.error("Error when call cubees api:{}".format(error))

                status_type = self.switch_status_santander_2.get(
                    request_body["situacao"], self.Constants.CASE_DEFAULT_TYPES.value
                )
                quota_to_insert = {
                    "quota_code": quota_code_final,
                    "external_reference": request_body["contrato"],
                    "total_installments": request_body["plano_basico"],
                    "is_contemplated": False,
                    "is_multiple_ownership": False,
                    "administrator_fee": request_body["tx_adm"] * 100,
                    "fund_reservation_fee": request_body["tx_fr"] * 100,
                    "info_date": row["dt_extrato"],
                    "quota_status_type_id": status_type,
                    "administrator_id": self.id_adm_santander,
                    "group_id": group_id_md_quota,
                    "quota_origin_id": self.Constants.QUOTA_ORIGIN_CUSTOMER.value,
                    "contract_number": request_body["contrato"],
                    "quota_number": request_body["cota"],
                    "check_digit": request_body["digito"],
                    "quota_person_type_id": person_quota_type,
                }
                quota_md_quota = self.pl_quota.insert_new_quota_from_santander_1(
                    quota_to_insert
                )
                quota_id_md_quota = quota_md_quota["quota_id"]
                self.quotas_santander_md_quota.append(quota_md_quota)

                quota_code_insert = str(quota_id_md_quota).rjust(6, "0")
                quota_code_update = str(generate_luhn_check_digit(quota_code_insert))
                quota_code_final_update = (
                    self.EtlInfo.QUOTA_CODE_PREFIX.value
                    + quota_code_insert
                    + quota_code_update
                )
                quota_code_update = {
                    "quota_id": quota_id_md_quota,
                    "quota_code": quota_code_final_update,
                }

                self.pl_quota.update_quota_code(quota_code_update)

                quota_status_to_insert = {
                    "quota_id": quota_id_md_quota,
                    "quota_status_type_id": status_type,
                }

                self.pl_quota_status.insert_quota_status(quota_status_to_insert)

                asset_type = self.switch_asset_type_santander_2.get(
                    request_body["tipo_bem"],
                    self.Constants.CASE_DEFAULT_ASSET_TYPES.value,
                )
                quota_history_detail_to_insert = {}
                for keyword in self.switch_quota_history_field:
                    quota_history_detail_to_insert[keyword] = None

                quota_history_detail_to_insert["quota_id"] = quota_id_md_quota
                quota_history_detail_to_insert["per_adm_paid"] = (
                    request_body["pago_perc_adm"] * 100
                )
                quota_history_detail_to_insert["per_adm_to_pay"] = (
                    request_body["a_pagar_perc_adm"] * 100
                )
                quota_history_detail_to_insert["per_amount_paid"] = (
                    request_body["pago_perc_total"] * 100
                )
                quota_history_detail_to_insert["per_mutual_fund_paid"] = (
                    request_body["pago_perc_fc_1"] * 100
                )
                quota_history_detail_to_insert["per_reserve_fund_paid"] = (
                    request_body["pago_perc_fr"] * 100
                )
                quota_history_detail_to_insert["per_mutual_fund_to_pay"] = (
                    request_body["a_pagar_perc_fc"] * 100
                )
                quota_history_detail_to_insert["per_reserve_fund_to_pay"] = (
                    request_body["a_pagar_perc_fr"] * 100
                )
                quota_history_detail_to_insert["per_install_diff_to_pay"] = (
                    request_body["a_pagar_perc_dif_parcela"] * 100
                )
                quota_history_detail_to_insert["per_total_amount_to_pay"] = (
                    request_body["a_pagar_perc_total"] * 100
                )
                quota_history_detail_to_insert[
                    "amnt_mutual_fund_to_pay"
                ] = request_body["a_pagar_vl_fc"]
                quota_history_detail_to_insert[
                    "amnt_reserve_fund_to_pay"
                ] = request_body["a_pagar_vl_fr"]
                quota_history_detail_to_insert["amnt_adm_to_pay"] = request_body[
                    "a_pagar_vl_adm"
                ]
                quota_history_detail_to_insert[
                    "amnt_install_diff_to_pay"
                ] = request_body["a_pagar_vl_dif_parcela"]
                quota_history_detail_to_insert["amnt_to_pay"] = request_body[
                    "a_pagar_vl_total"
                ]
                quota_history_detail_to_insert["old_quota_number"] = request_body[
                    "cota"
                ]
                quota_history_detail_to_insert["old_digit"] = request_body["digito"]
                quota_history_detail_to_insert["adjustment_date"] = request_body[
                    "dt_reajuste_em"
                ]
                quota_history_detail_to_insert["asset_description"] = request_body[
                    "bem"
                ]
                quota_history_detail_to_insert["asset_adm_code"] = request_body[
                    "cd_bem"
                ]
                quota_history_detail_to_insert["asset_value"] = request_body["vl_bem"]
                quota_history_detail_to_insert["asset_type_id"] = asset_type
                quota_history_detail_to_insert["info_date"] = request_body["dt_extrato"]
                quota_history_detail_to_insert["amnt_fine_to_pay"] = request_body[
                    "a_pagar_vl_multa"
                ]
                quota_history_detail_to_insert["amnt_interest_to_pay"] = request_body[
                    "a_pagar_vl_juros"
                ]
                quota_history_detail_to_insert["amnt_others_to_pay"] = request_body[
                    "a_pagar_vl_outros"
                ]
                quota_history_detail_to_insert["per_insurance_to_pay"] = request_body[
                    "a_pagar_perc_seguros"
                ]
                quota_history_detail_to_insert["amnt_insurance_to_pay"] = request_body[
                    "a_pagar_vl_seguros"
                ]
                quota_history_detail_to_insert[
                    "per_subscription_to_pay"
                ] = request_body["a_pagar_perc_adesao"]
                quota_history_detail_to_insert[
                    "amnt_subscription_to_pay"
                ] = request_body["a_pagar_vl_adesao"]
                quota_history_detail_to_insert["valid_from"] = get_default_datetime()
                quota_history_detail_to_insert["valid_to"] = None

                self.pl_quota_history_detail.insert_new_quota_history(
                    quota_history_detail_to_insert
                )

                fields_inserted_quota_history = [
                    "old_quota_number",
                    "old_digit",
                    "per_amount_paid",
                    "per_mutual_fund_paid",
                    "per_reserve_fund_paid",
                    "per_adm_paid",
                    "per_mutual_fund_to_pay",
                    "per_reserve_fund_to_pay",
                    "per_install_diff_to_pay",
                    "per_adm_to_pay",
                    "per_total_amount_to_pay",
                    "amnt_mutual_fund_to_pay",
                    "amnt_reserve_fund_to_pay",
                    "amnt_adm_to_pay",
                    "amnt_install_diff_to_pay",
                    "amnt_to_pay",
                    "adjustment_date",
                    "asset_adm_code",
                    "asset_description",
                    "asset_value",
                    "asset_type_id",
                ]

                for field in fields_inserted_quota_history:
                    history_field_id = self.switch_quota_history_field.get(
                        field, self.Constants.CASE_DEFAULT_HISTORY_DETAIL_FIELD.value
                    )
                    quota_field_update_date_insert = {
                        "update_date": row["dt_extrato"],
                        "quota_history_field_id": history_field_id,
                        "data_source_id": self.data_source_id,
                        "quota_id": quota_id_md_quota,
                    }

                    self.pl_quota_field_update_date.insert_quota_field_update_date(
                        quota_field_update_date_insert
                    )

                quota_owner_to_insert = {
                    "ownership_percent": 1,
                    "quota_id": quota_id_md_quota,
                    "person_code": person_code,
                    "main_owner": True,
                    "valid_from": get_default_datetime(),
                }

                self.pl_quota_owner.insert_new_quota_owner(quota_owner_to_insert)

            else:
                quota_id_md_quota = md_quota_quota["quota_id"]
                status_type = self.switch_status_santander_2.get(
                    request_body["situacao"], self.Constants.CASE_DEFAULT_TYPES.value
                )
                date_str = request_body["dt_extrato"]
                date_format = "%Y-%m-%d"
                data_extrato = datetime.strptime(date_str, date_format)
                person_code = request_body["person_code"]
                person_quota_type = None
                endpoint_url = "https://cubees.bazar-sandbox.technology/customer"
                header = {"x-api-key": "bDtuPmlYL65tP4lmVxWob6gygrBhO22vD4Cv5F1h"}
                params = {"person_code": person_code}
                try:
                    response = requests.get(
                        url=endpoint_url, params=params, headers=header
                    )
                    if response.status_code == 200:
                        customer = response.json()["person_type_data"]
                        if customer["natural_person_id"]:
                            person_quota_type = 1
                        else:
                            person_quota_type = 2

                except requests.exceptions.HTTPError as error:
                    logger.error("Error when call cubees api:{}".format(error))

                if md_quota_quota["info_date"] < data_extrato:
                    quota_update = {
                        "quota_id": quota_id_md_quota,
                        "external_reference": request_body["contrato"],
                        "total_installments": request_body["plano_basico"],
                        "is_contemplated": False,
                        "is_multiple_ownership": False,
                        "administrator_fee": (request_body["tx_adm"] * 100),
                        "fund_reservation_fee": (request_body["tx_fr"] * 100),
                        "info_date": request_body["dt_extrato"],
                        "quota_status_type_id": status_type,
                        "contract_number": request_body["contrato"],
                        "quota_number": request_body["cota"],
                        "check_digit": request_body["digito"],
                        "quota_person_type_id": person_quota_type,
                    }

                    self.pl_quota.update_quota_santander(quota_update)

                    if md_quota_quota["quota_status_type_id"] != status_type:
                        quota_status_to_update = {"quota_id": quota_id_md_quota}

                        self.pl_quota_status.update_quota_status(quota_status_to_update)

                        quota_status_to_insert = {
                            "quota_id": quota_id_md_quota,
                            "quota_status_type_id": status_type,
                        }

                        self.pl_quota_status.insert_quota_status(quota_status_to_insert)

                    self.pl_quota_owner.update_valid_to(quota_id_md_quota)

                    quota_owner_to_insert = {
                        "ownership_percent": 1,
                        "quota_id": quota_id_md_quota,
                        "person_code": person_code,
                        "main_owner": True,
                        "valid_from": get_default_datetime(),
                    }

                    self.pl_quota_owner.insert_new_quota_owner(quota_owner_to_insert)

                quota_history_detail_md_quota = (
                    self.pl_quota_history_detail.search_quota_history_detail(
                        quota_id_md_quota
                    )
                )

                if quota_history_detail_md_quota["info_date"] < data_extrato:
                    self.pl_quota_history_detail.update_valid_to(quota_id_md_quota)

                    asset_type = self.switch_asset_type_santander_2.get(
                        request_body["tipo_bem"],
                        self.Constants.CASE_DEFAULT_ASSET_TYPES.value,
                    )

                    quota_history_detail_to_insert = {}
                    for keyword in self.switch_quota_history_field:
                        logger.info(keyword)
                        logger.info(quota_history_detail_md_quota)
                        quota_history_detail_to_insert[
                            keyword
                        ] = quota_history_detail_md_quota[keyword]

                    quota_history_detail_to_insert["quota_id"] = quota_id_md_quota
                    quota_history_detail_to_insert["per_adm_paid"] = (
                        request_body["pago_perc_adm"] * 100
                    )
                    quota_history_detail_to_insert["per_adm_to_pay"] = (
                        request_body["a_pagar_perc_adm"] * 100
                    )
                    quota_history_detail_to_insert["per_amount_paid"] = (
                        request_body["pago_perc_total"] * 100
                    )
                    quota_history_detail_to_insert["per_mutual_fund_paid"] = (
                        request_body["pago_perc_fc_1"] * 100
                    )
                    quota_history_detail_to_insert["per_reserve_fund_paid"] = (
                        request_body["pago_perc_fr"] * 100
                    )
                    quota_history_detail_to_insert["per_mutual_fund_to_pay"] = (
                        request_body["a_pagar_perc_fc"] * 100
                    )
                    quota_history_detail_to_insert["per_reserve_fund_to_pay"] = (
                        request_body["a_pagar_perc_fr"] * 100
                    )
                    quota_history_detail_to_insert["per_install_diff_to_pay"] = (
                        request_body["a_pagar_perc_dif_parcela"] * 100
                    )
                    quota_history_detail_to_insert["per_total_amount_to_pay"] = (
                        request_body["a_pagar_perc_total"] * 100
                    )
                    quota_history_detail_to_insert[
                        "amnt_mutual_fund_to_pay"
                    ] = request_body["a_pagar_vl_fc"]
                    quota_history_detail_to_insert[
                        "amnt_reserve_fund_to_pay"
                    ] = request_body["a_pagar_vl_fr"]
                    quota_history_detail_to_insert["amnt_adm_to_pay"] = request_body[
                        "a_pagar_vl_adm"
                    ]
                    quota_history_detail_to_insert[
                        "amnt_install_diff_to_pay"
                    ] = request_body["a_pagar_vl_dif_parcela"]
                    quota_history_detail_to_insert["amnt_to_pay"] = request_body[
                        "a_pagar_vl_total"
                    ]
                    quota_history_detail_to_insert["old_quota_number"] = request_body[
                        "cota"
                    ]
                    quota_history_detail_to_insert["old_digit"] = request_body["digito"]
                    quota_history_detail_to_insert["adjustment_date"] = request_body[
                        "dt_reajuste_em"
                    ]
                    quota_history_detail_to_insert["asset_description"] = request_body[
                        "bem"
                    ]
                    quota_history_detail_to_insert["asset_adm_code"] = request_body[
                        "cd_bem"
                    ]
                    quota_history_detail_to_insert["asset_value"] = request_body[
                        "vl_bem"
                    ]
                    quota_history_detail_to_insert["asset_type_id"] = asset_type
                    quota_history_detail_to_insert["info_date"] = request_body[
                        "dt_extrato"
                    ]
                    quota_history_detail_to_insert["amnt_fine_to_pay"] = request_body[
                        "a_pagar_vl_multa"
                    ]
                    quota_history_detail_to_insert[
                        "amnt_interest_to_pay"
                    ] = request_body["a_pagar_vl_juros"]
                    quota_history_detail_to_insert["amnt_others_to_pay"] = request_body[
                        "a_pagar_vl_outros"
                    ]
                    quota_history_detail_to_insert[
                        "per_insurance_to_pay"
                    ] = request_body["a_pagar_perc_seguros"]
                    quota_history_detail_to_insert[
                        "amnt_insurance_to_pay"
                    ] = request_body["a_pagar_vl_seguros"]
                    quota_history_detail_to_insert[
                        "per_subscription_to_pay"
                    ] = request_body["a_pagar_perc_adesao"]
                    quota_history_detail_to_insert[
                        "amnt_subscription_to_pay"
                    ] = request_body["a_pagar_vl_adesao"]
                    quota_history_detail_to_insert[
                        "valid_from"
                    ] = get_default_datetime()
                    quota_history_detail_to_insert["valid_to"] = None

                    self.pl_quota_history_detail.insert_new_quota_history(
                        quota_history_detail_to_insert
                    )

                    fields_updated_quota_history = [
                        "amnt_fine_to_pay",
                        "amnt_interest_to_pay",
                        "amnt_others_to_pay",
                        "per_insurance_to_pay",
                        "amnt_insurance_to_pay",
                        "per_subscription_to_pay",
                        "amnt_subscription_to_pay",
                        "old_quota_number",
                        "old_digit",
                        "per_amount_paid",
                        "per_mutual_fund_paid",
                        "per_reserve_fund_paid",
                        "per_adm_paid",
                        "per_mutual_fund_to_pay",
                        "per_reserve_fund_to_pay",
                        "per_install_diff_to_pay",
                        "per_adm_to_pay",
                        "per_total_amount_to_pay",
                        "amnt_mutual_fund_to_pay",
                        "amnt_reserve_fund_to_pay",
                        "amnt_adm_to_pay",
                        "amnt_install_diff_to_pay",
                        "amnt_to_pay",
                        "adjustment_date",
                        "asset_adm_code",
                        "asset_description",
                        "asset_value",
                        "asset_type_id",
                    ]

                    for field in fields_updated_quota_history:
                        history_field_id = self.switch_quota_history_field.get(
                            field,
                            self.Constants.CASE_DEFAULT_HISTORY_DETAIL_FIELD.value,
                        )

                        quota_field_update_date_to_update = {
                            "update_date": request_body["dt_extrato"],
                            "quota_history_field_id": history_field_id,
                            "data_source_id": self.data_source_id,
                            "quota_id": quota_id_md_quota,
                        }

                        self.pl_quota_field_update_date.update_quota_field_update_date(
                            quota_field_update_date_to_update
                        )

                else:
                    md_quota_history_field_update_date = (
                        self.pl_quota_field_update_date.quota_field_update_date_all(
                            quota_id_md_quota
                        )
                    )
                    fields_updated_quota_history = [
                        "amnt_fine_to_pay",
                        "amnt_interest_to_pay",
                        "amnt_others_to_pay",
                        "per_insurance_to_pay",
                        "amnt_insurance_to_pay",
                        "per_subscription_to_pay",
                        "amnt_subscription_to_pay",
                        "old_quota_number",
                        "old_digit",
                        "per_amount_paid",
                        "per_mutual_fund_paid",
                        "per_reserve_fund_paid",
                        "per_adm_paid",
                        "per_mutual_fund_to_pay",
                        "per_reserve_fund_to_pay",
                        "per_install_diff_to_pay",
                        "per_adm_to_pay",
                        "per_total_amount_to_pay",
                        "amnt_mutual_fund_to_pay",
                        "amnt_reserve_fund_to_pay",
                        "amnt_adm_to_pay",
                        "amnt_install_diff_to_pay",
                        "amnt_to_pay",
                        "adjustment_date",
                        "asset_adm_code",
                        "asset_description",
                        "asset_value",
                        "asset_type_id",
                    ]
                    asset_type = self.switch_asset_type_santander_2.get(
                        request_body["tipo_bem"],
                        self.Constants.CASE_DEFAULT_ASSET_TYPES.value,
                    )
                    quota_history_detail_md_quota["asset_type_id"] = asset_type

                    quota_history_detail_to_insert = {}
                    for keyword in self.switch_quota_history_field:
                        logger.info(keyword)
                        logger.info(quota_history_detail_md_quota)
                        quota_history_detail_to_insert[
                            keyword
                        ] = quota_history_detail_md_quota[keyword]

                    for field in fields_updated_quota_history:
                        history_field_id = self.switch_quota_history_field.get(
                            field,
                            self.Constants.CASE_DEFAULT_HISTORY_DETAIL_FIELD.value,
                        )
                        request_field = self.switch_history_field.get(field)
                        if quota_history_detail_md_quota[field] is None:
                            quota_history_detail_to_insert[field] = request_body[
                                request_field
                            ]
                            quota_field_update_date_to_update = {
                                "update_date": request_body["dt_extrato"],
                                "quota_history_field_id": history_field_id,
                                "data_source_id": self.data_source_id,
                                "quota_id": quota_id_md_quota,
                            }

                            self.pl_quota_field_update_date.update_quota_field_update_date(
                                quota_field_update_date_to_update
                            )

                        else:
                            history_field_update_date = self.get_dict_by_id(
                                str(history_field_id),
                                md_quota_history_field_update_date,
                                "quota_history_field_id",
                            )
                            if history_field_update_date["update_date"] < data_extrato:
                                quota_history_detail_to_insert[field] = request_body[
                                    request_field
                                ]
                                quota_field_update_date_to_update = {
                                    "update_date": request_body["dt_extrato"],
                                    "quota_history_field_id": history_field_id,
                                    "data_source_id": self.data_source_id,
                                    "quota_id": quota_id_md_quota,
                                }

                                self.pl_quota_field_update_date.update_quota_field_update_date(
                                    quota_field_update_date_to_update
                                )

            self.stage_raw.update_is_processed(row["id_beereader"])

    def gmac_1_flow(self):
        for row in self.statement_gmac_1:
            request_body = json.loads(row["quota_data"])
            code_group = self.string_right_justified(request_body["grupo"])
            md_quota_group = self.get_dict_by_id(
                code_group, self.groups_md_quota_gmac, "group_code"
            )
            md_quota_quota = self.get_dict_by_id(
                str(request_body["contrato"]),
                self.quotas_gmac_md_quota,
                "external_reference",
            )

            if md_quota_quota is None:
                quota_code_tb = str(self.quota_code_md_quota + 1).rjust(6, "0")
                self.quota_code_md_quota = self.quota_code_md_quota + 1
                quota_code_suffix = str(generate_luhn_check_digit(quota_code_tb))
                quota_code_final = (
                    self.EtlInfo.QUOTA_CODE_PREFIX.value
                    + quota_code_tb
                    + quota_code_suffix
                )

                if md_quota_group is None:
                    assembly_date = datetime.strptime(
                        request_body["dt_assembleia_atual"], "%Y-%m-%d"
                    ).date()

                    group_deadline = 0
                    group_end_date = datetime.today()

                    if request_body["nr_assembleia_atual"] is not None:
                        today = datetime.today()

                        assembly_since_statement = relativedelta.relativedelta(
                            today, assembly_date
                        ).months
                        total_assembly = (
                            request_body["nr_assembleia_atual"]
                            + assembly_since_statement
                        )
                        group_deadline = request_body["plano_basico"]

                        assembly_to_end = group_deadline - total_assembly

                        group_end_date = today + relativedelta.relativedelta(
                            months=assembly_to_end
                        )

                    group_to_insert = {
                        "group_code": code_group,
                        "group_deadline": group_deadline,
                        "administrator_id": self.id_adm_santander,
                        "group_closing_date": group_end_date,
                    }
                    group_md_quota = self.pl_group.insert_new_group(group_to_insert)
                    group_id_md_quota = group_md_quota["group_id"]
                    self.groups_md_quota_gmac.append(group_md_quota)

                else:
                    group_id_md_quota = md_quota_group["group_id"]

                person_code = request_body["person_code"]
                person_quota_type = None
                endpoint_url = "https://cubees.bazar-sandbox.technology/customer"
                header = {"x-api-key": "bDtuPmlYL65tP4lmVxWob6gygrBhO22vD4Cv5F1h"}
                params = {"person_code": person_code}
                try:
                    response = requests.get(
                        url=endpoint_url, params=params, headers=header
                    )
                    if response.status_code == 200:
                        customer = response.json()["person_type_data"]
                        if customer["natural_person_id"]:
                            person_quota_type = 1
                        else:
                            person_quota_type = 2

                except requests.exceptions.HTTPError as error:
                    logger.error("Error when call cubees api:{}".format(error))

                status_type = self.Constants.CASE_DEFAULT_TYPES.value

                quota_to_insert = {
                    "quota_code": quota_code_final,
                    "external_reference": request_body["contrato"],
                    "total_installments": request_body["plano_basico"],
                    "is_contemplated": False,
                    "is_multiple_ownership": False,
                    "administrator_fee": request_body["tx_adm"] * 100,
                    "fund_reservation_fee": request_body["tx_fr"] * 100,
                    "info_date": row["dt_extrato"],
                    "quota_status_type_id": status_type,
                    "administrator_id": self.id_adm_gmac,
                    "group_id": group_id_md_quota,
                    "quota_origin_id": self.Constants.QUOTA_ORIGIN_CUSTOMER.value,
                    "contract_number": request_body["contrato"],
                    "quota_number": request_body["cota"],
                    "check_digit": request_body["digito"],
                    "quota_person_type_id": person_quota_type,
                }
                quota_md_quota = self.pl_quota.insert_new_quota_from_santander_1(
                    quota_to_insert
                )
                quota_id_md_quota = quota_md_quota["quota_id"]
                self.quotas_gmac_md_quota.append(quota_md_quota)

                quota_code_insert = str(quota_id_md_quota).rjust(6, "0")
                quota_code_update = str(generate_luhn_check_digit(quota_code_insert))
                quota_code_final_update = (
                    self.EtlInfo.QUOTA_CODE_PREFIX.value
                    + quota_code_insert
                    + quota_code_update
                )
                quota_code_update = {
                    "quota_id": quota_id_md_quota,
                    "quota_code": quota_code_final_update,
                }

                self.pl_quota.update_quota_code(quota_code_update)

                quota_status_to_insert = {
                    "quota_id": quota_id_md_quota,
                    "quota_status_type_id": status_type,
                }

                self.pl_quota_status.insert_quota_status(quota_status_to_insert)

                asset_type = 3

                quota_history_detail_to_insert = {}
                for keyword in self.switch_quota_history_field:
                    quota_history_detail_to_insert[keyword] = None

                quota_history_detail_to_insert["quota_id"] = quota_id_md_quota
                quota_history_detail_to_insert["per_adm_paid"] = (
                    request_body["pago_perc_adm"] * 100
                )
                quota_history_detail_to_insert["per_adm_to_pay"] = (
                    request_body["a_pagar_perc_adm"] * 100
                )
                quota_history_detail_to_insert["per_amount_paid"] = (
                    request_body["pago_perc_total"] * 100
                )
                quota_history_detail_to_insert["per_mutual_fund_paid"] = (
                    request_body["pago_perc_fc_1"] * 100
                )
                quota_history_detail_to_insert["per_reserve_fund_paid"] = (
                    request_body["pago_perc_fr"] * 100
                )
                quota_history_detail_to_insert["per_mutual_fund_to_pay"] = (
                    request_body["a_pagar_perc_fc"] * 100
                )
                quota_history_detail_to_insert["per_reserve_fund_to_pay"] = (
                    request_body["a_pagar_perc_fr"] * 100
                )
                quota_history_detail_to_insert["per_install_diff_to_pay"] = (
                    request_body["a_pagar_perc_dif_parcela"] * 100
                )
                quota_history_detail_to_insert["per_total_amount_to_pay"] = (
                    request_body["a_pagar_perc_total"] * 100
                )
                quota_history_detail_to_insert[
                    "amnt_mutual_fund_to_pay"
                ] = request_body["a_pagar_vl_fc"]
                quota_history_detail_to_insert[
                    "amnt_reserve_fund_to_pay"
                ] = request_body["a_pagar_vl_fr"]
                quota_history_detail_to_insert["amnt_adm_to_pay"] = request_body[
                    "a_pagar_vl_adm"
                ]
                quota_history_detail_to_insert[
                    "amnt_install_diff_to_pay"
                ] = request_body["a_pagar_vl_dif_parcela"]
                quota_history_detail_to_insert["amnt_to_pay"] = request_body[
                    "a_pagar_vl_total"
                ]
                quota_history_detail_to_insert["old_quota_number"] = request_body[
                    "cota"
                ]
                quota_history_detail_to_insert["old_digit"] = request_body["digito"]
                quota_history_detail_to_insert["adjustment_date"] = request_body[
                    "dt_reajuste_em"
                ]
                quota_history_detail_to_insert["asset_description"] = request_body[
                    "bem"
                ]
                quota_history_detail_to_insert["asset_adm_code"] = request_body[
                    "cd_bem"
                ]
                quota_history_detail_to_insert["asset_value"] = request_body["vl_bem"]
                quota_history_detail_to_insert["asset_type_id"] = asset_type
                quota_history_detail_to_insert["info_date"] = request_body["dt_extrato"]
                quota_history_detail_to_insert["amnt_fine_to_pay"] = request_body[
                    "a_pagar_vl_multa"
                ]
                quota_history_detail_to_insert["amnt_interest_to_pay"] = request_body[
                    "a_pagar_vl_juros"
                ]
                quota_history_detail_to_insert["amnt_others_to_pay"] = request_body[
                    "a_pagar_vl_outros"
                ]
                quota_history_detail_to_insert["per_insurance_to_pay"] = request_body[
                    "a_pagar_perc_seguros"
                ]
                quota_history_detail_to_insert["amnt_insurance_to_pay"] = request_body[
                    "a_pagar_vl_seguros"
                ]
                quota_history_detail_to_insert[
                    "per_subscription_to_pay"
                ] = request_body["a_pagar_perc_adesao"]
                quota_history_detail_to_insert[
                    "amnt_subscription_to_pay"
                ] = request_body["a_pagar_vl_adesao"]
                quota_history_detail_to_insert["valid_from"] = get_default_datetime()
                quota_history_detail_to_insert["valid_to"] = None

                self.pl_quota_history_detail.insert_new_quota_history(
                    quota_history_detail_to_insert
                )

                fields_inserted_quota_history = [
                    "old_quota_number",
                    "old_digit",
                    "per_amount_paid",
                    "per_mutual_fund_paid",
                    "per_reserve_fund_paid",
                    "per_adm_paid",
                    "per_mutual_fund_to_pay",
                    "per_reserve_fund_to_pay",
                    "per_install_diff_to_pay",
                    "per_adm_to_pay",
                    "per_total_amount_to_pay",
                    "amnt_mutual_fund_to_pay",
                    "amnt_reserve_fund_to_pay",
                    "amnt_adm_to_pay",
                    "amnt_install_diff_to_pay",
                    "amnt_to_pay",
                    "adjustment_date",
                    "asset_adm_code",
                    "asset_description",
                    "asset_value",
                    "asset_type_id",
                ]

                for field in fields_inserted_quota_history:
                    history_field_id = self.switch_quota_history_field.get(
                        field, self.Constants.CASE_DEFAULT_HISTORY_DETAIL_FIELD.value
                    )
                    quota_field_update_date_insert = {
                        "update_date": row["dt_extrato"],
                        "quota_history_field_id": history_field_id,
                        "data_source_id": self.data_source_id,
                        "quota_id": quota_id_md_quota,
                    }

                    self.pl_quota_field_update_date.insert_quota_field_update_date(
                        quota_field_update_date_insert
                    )

                quota_owner_to_insert = {
                    "ownership_percent": 1,
                    "quota_id": quota_id_md_quota,
                    "person_code": person_code,
                    "main_owner": True,
                    "valid_from": get_default_datetime(),
                }

                self.pl_quota_owner.insert_new_quota_owner(quota_owner_to_insert)

            else:
                quota_id_md_quota = md_quota_quota["quota_id"]
                status_type = self.Constants.CASE_DEFAULT_TYPES.value
                person_code = request_body["person_code"]
                date_str = request_body["dt_extrato"]
                date_format = "%Y-%m-%d"
                data_extrato = datetime.strptime(date_str, date_format)
                person_quota_type = None
                endpoint_url = "https://cubees.bazar-sandbox.technology/customer"
                header = {"x-api-key": "bDtuPmlYL65tP4lmVxWob6gygrBhO22vD4Cv5F1h"}
                params = {"person_code": person_code}
                try:
                    response = requests.get(
                        url=endpoint_url, params=params, headers=header
                    )
                    if response.status_code == 200:
                        customer = response.json()["person_type_data"]
                        if customer["natural_person_id"]:
                            person_quota_type = 1
                        else:
                            person_quota_type = 2

                except requests.exceptions.HTTPError as error:
                    logger.error("Error when call cubees api:{}".format(error))
                if md_quota_quota["info_date"] < data_extrato:
                    quota_update = {
                        "quota_id": quota_id_md_quota,
                        "external_reference": request_body["contrato"],
                        "total_installments": request_body["plano_basico"],
                        "is_contemplated": False,
                        "is_multiple_ownership": False,
                        "administrator_fee": request_body["tx_adm"] * 100,
                        "fund_reservation_fee": request_body["tx_fr"] * 100,
                        "info_date": row["dt_extrato"],
                        "quota_status_type_id": status_type,
                        "contract_number": request_body["contrato"],
                        "quota_number": request_body["cota"],
                        "check_digit": request_body["digito"],
                        "quota_person_type_id": person_quota_type,
                    }

                    self.pl_quota.update_quota_santander(quota_update)

                    if md_quota_quota["quota_status_type_id"] != status_type:
                        quota_status_to_update = {"quota_id": quota_id_md_quota}

                        self.pl_quota_status.update_quota_status(quota_status_to_update)

                        quota_status_to_insert = {
                            "quota_id": quota_id_md_quota,
                            "quota_status_type_id": status_type,
                        }

                        self.pl_quota_status.insert_quota_status(quota_status_to_insert)

                    self.pl_quota_owner.update_valid_to(quota_id_md_quota)

                    quota_owner_to_insert = {
                        "ownership_percent": 1,
                        "quota_id": quota_id_md_quota,
                        "person_code": person_code,
                        "main_owner": True,
                        "valid_from": get_default_datetime(),
                    }

                    self.pl_quota_owner.insert_new_quota_owner(quota_owner_to_insert)

                quota_history_detail_md_quota = (
                    self.pl_quota_history_detail.search_quota_history_detail(
                        quota_id_md_quota
                    )
                )

                asset_type = 3

                if quota_history_detail_md_quota["info_date"] < data_extrato:
                    self.pl_quota_history_detail.update_valid_to(quota_id_md_quota)

                    quota_history_detail_to_insert = {}
                    for keyword in self.switch_quota_history_field:
                        logger.info(keyword)
                        logger.info(quota_history_detail_md_quota)
                        quota_history_detail_to_insert[
                            keyword
                        ] = quota_history_detail_md_quota[keyword]

                    quota_history_detail_to_insert["quota_id"] = quota_id_md_quota
                    quota_history_detail_to_insert["per_adm_paid"] = (
                        request_body["pago_perc_adm"] * 100
                    )
                    quota_history_detail_to_insert["per_adm_to_pay"] = (
                        request_body["a_pagar_perc_adm"] * 100
                    )
                    quota_history_detail_to_insert["per_amount_paid"] = (
                        request_body["pago_perc_total"] * 100
                    )
                    quota_history_detail_to_insert["per_mutual_fund_paid"] = (
                        request_body["pago_perc_fc_1"] * 100
                    )
                    quota_history_detail_to_insert["per_reserve_fund_paid"] = (
                        request_body["pago_perc_fr"] * 100
                    )
                    quota_history_detail_to_insert["per_mutual_fund_to_pay"] = (
                        request_body["a_pagar_perc_fc"] * 100
                    )
                    quota_history_detail_to_insert["per_reserve_fund_to_pay"] = (
                        request_body["a_pagar_perc_fr"] * 100
                    )
                    quota_history_detail_to_insert["per_install_diff_to_pay"] = (
                        request_body["a_pagar_perc_dif_parcela"] * 100
                    )
                    quota_history_detail_to_insert["per_total_amount_to_pay"] = (
                        request_body["a_pagar_perc_total"] * 100
                    )
                    quota_history_detail_to_insert[
                        "amnt_mutual_fund_to_pay"
                    ] = request_body["a_pagar_vl_fc"]
                    quota_history_detail_to_insert[
                        "amnt_reserve_fund_to_pay"
                    ] = request_body["a_pagar_vl_fr"]
                    quota_history_detail_to_insert["amnt_adm_to_pay"] = request_body[
                        "a_pagar_vl_adm"
                    ]
                    quota_history_detail_to_insert[
                        "amnt_install_diff_to_pay"
                    ] = request_body["a_pagar_vl_dif_parcela"]
                    quota_history_detail_to_insert["amnt_to_pay"] = request_body[
                        "a_pagar_vl_total"
                    ]
                    quota_history_detail_to_insert["old_quota_number"] = request_body[
                        "cota"
                    ]
                    quota_history_detail_to_insert["old_digit"] = request_body["digito"]
                    quota_history_detail_to_insert["adjustment_date"] = request_body[
                        "dt_reajuste_em"
                    ]
                    quota_history_detail_to_insert["asset_description"] = request_body[
                        "bem"
                    ]
                    quota_history_detail_to_insert["asset_adm_code"] = request_body[
                        "cd_bem"
                    ]
                    quota_history_detail_to_insert["asset_value"] = request_body[
                        "vl_bem"
                    ]
                    quota_history_detail_to_insert["asset_type_id"] = asset_type
                    quota_history_detail_to_insert["info_date"] = request_body[
                        "dt_extrato"
                    ]
                    quota_history_detail_to_insert["amnt_fine_to_pay"] = request_body[
                        "a_pagar_vl_multa"
                    ]
                    quota_history_detail_to_insert[
                        "amnt_interest_to_pay"
                    ] = request_body["a_pagar_vl_juros"]
                    quota_history_detail_to_insert["amnt_others_to_pay"] = request_body[
                        "a_pagar_vl_outros"
                    ]
                    quota_history_detail_to_insert[
                        "per_insurance_to_pay"
                    ] = request_body["a_pagar_perc_seguros"]
                    quota_history_detail_to_insert[
                        "amnt_insurance_to_pay"
                    ] = request_body["a_pagar_vl_seguros"]
                    quota_history_detail_to_insert[
                        "per_subscription_to_pay"
                    ] = request_body["a_pagar_perc_adesao"]
                    quota_history_detail_to_insert[
                        "amnt_subscription_to_pay"
                    ] = request_body["a_pagar_vl_adesao"]
                    quota_history_detail_to_insert[
                        "valid_from"
                    ] = get_default_datetime()
                    quota_history_detail_to_insert["valid_to"] = None

                    self.pl_quota_history_detail.insert_new_quota_history(
                        quota_history_detail_to_insert
                    )

                    fields_updated_quota_history = [
                        "old_quota_number",
                        "old_digit",
                        "per_amount_paid",
                        "per_mutual_fund_paid",
                        "per_reserve_fund_paid",
                        "per_adm_paid",
                        "per_mutual_fund_to_pay",
                        "per_reserve_fund_to_pay",
                        "per_install_diff_to_pay",
                        "per_adm_to_pay",
                        "per_total_amount_to_pay",
                        "amnt_mutual_fund_to_pay",
                        "amnt_reserve_fund_to_pay",
                        "amnt_adm_to_pay",
                        "amnt_install_diff_to_pay",
                        "amnt_to_pay",
                        "adjustment_date",
                        "asset_adm_code",
                        "asset_description",
                        "asset_value",
                        "asset_type_id",
                    ]

                    for field in fields_updated_quota_history:
                        history_field_id = self.switch_quota_history_field.get(
                            field,
                            self.Constants.CASE_DEFAULT_HISTORY_DETAIL_FIELD.value,
                        )

                        quota_field_update_date_to_update = {
                            "update_date": row["dt_extrato"],
                            "quota_history_field_id": history_field_id,
                            "data_source_id": self.data_source_id,
                            "quota_id": quota_id_md_quota,
                        }

                        self.pl_quota_field_update_date.update_quota_field_update_date(
                            quota_field_update_date_to_update
                        )

                else:
                    md_quota_history_field_update_date = (
                        self.pl_quota_field_update_date.quota_field_update_date_all(
                            quota_id_md_quota
                        )
                    )
                    fields_updated_quota_history = [
                        "old_quota_number",
                        "old_digit",
                        "per_amount_paid",
                        "per_mutual_fund_paid",
                        "per_reserve_fund_paid",
                        "per_adm_paid",
                        "per_mutual_fund_to_pay",
                        "per_reserve_fund_to_pay",
                        "per_install_diff_to_pay",
                        "per_adm_to_pay",
                        "per_total_amount_to_pay",
                        "amnt_mutual_fund_to_pay",
                        "amnt_reserve_fund_to_pay",
                        "amnt_adm_to_pay",
                        "amnt_install_diff_to_pay",
                        "amnt_to_pay",
                        "adjustment_date",
                        "asset_adm_code",
                        "asset_description",
                        "asset_value",
                        "asset_type_id",
                    ]
                    quota_history_detail_md_quota["asset_type"] = asset_type

                    quota_history_detail_to_insert = {}
                    for keyword in self.switch_quota_history_field:
                        logger.info(keyword)
                        logger.info(quota_history_detail_md_quota)
                        quota_history_detail_to_insert[
                            keyword
                        ] = quota_history_detail_md_quota[keyword]

                    for field in fields_updated_quota_history:
                        history_field_id = self.switch_quota_history_field.get(
                            field,
                            self.Constants.CASE_DEFAULT_HISTORY_DETAIL_FIELD.value,
                        )
                        request_field = self.switch_history_field.get(field)
                        if quota_history_detail_md_quota[field] is None:
                            quota_history_detail_to_insert[field] = request_body[
                                request_field
                            ]
                            quota_field_update_date_to_update = {
                                "update_date": request_body["dt_extrato"],
                                "quota_history_field_id": history_field_id,
                                "data_source_id": self.data_source_id,
                                "quota_id": quota_id_md_quota,
                            }

                            self.pl_quota_field_update_date.update_quota_field_update_date(
                                quota_field_update_date_to_update
                            )

                        else:
                            history_field_update_date = self.get_dict_by_id(
                                str(history_field_id),
                                md_quota_history_field_update_date,
                                "quota_history_field_id",
                            )
                            if history_field_update_date["update_date"] < data_extrato:
                                quota_history_detail_to_insert[field] = request_body[
                                    request_field
                                ]
                                quota_field_update_date_to_update = {
                                    "update_date": request_body["dt_extrato"],
                                    "quota_history_field_id": history_field_id,
                                    "data_source_id": self.data_source_id,
                                    "quota_id": quota_id_md_quota,
                                }

                                self.pl_quota_field_update_date.update_quota_field_update_date(
                                    quota_field_update_date_to_update
                                )

            self.stage_raw.update_is_processed(row["id_beereader"])


if __name__ == "__main__":
    etl = Etl()
    etl.start()
    # job.commit()
