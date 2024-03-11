# imports do glue

"""
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
"""

from datetime import datetime

from dateutil import relativedelta
from enum import Enum
from typing import Union, Dict, Any

# imports for connection
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.exc import (
    OperationalError,
    DatabaseError,
    DBAPIError,
    IntegrityError,
    SQLAlchemyError,
)

# imports for models:
from sqlalchemy import (
    create_engine,
    func,
    Column,
    Integer,
    String,
    DateTime,
    Boolean,
    BigInteger,
    ForeignKey,
    Numeric,
    inspect,
    and_,
)

from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv
import os

# imports loggers:
import logging


class Logger:
    def __init__(self, name: str = "etl-satander", level: str = logging.INFO) -> None:
        self.log = self.logger(name, level)

    @classmethod
    def logger(cls, name: str, level: str = logging.INFO) -> logging.Logger:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        logging.basicConfig(format=log_format)
        loggers = logging.getLogger(name)
        loggers.setLevel(level)
        return loggers


logger = Logger().log

load_dotenv()


class DbInfo(Enum):
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    SCHEMA_STAGE = os.getenv("SCHEMA_STAGE")
    SCHEMA_MD = os.getenv("SCHEMA_MD")


Base = declarative_base()


class Connection:
    def __init__(self) -> None:
        try:
            url = (
                f"postgresql://{DbInfo.DB_USER.value}:{DbInfo.DB_PASSWORD.value}@"
                f"{DbInfo.DB_HOST.value}:{DbInfo.DB_PORT.value}/{DbInfo.DB_NAME.value}"
            )

            self.__engine = create_engine(url)
            self.__db_session = scoped_session(
                sessionmaker(autocommit=False, bind=self.__engine)
            )
            logger.info(f"Database connection successfully established")
            Base.metadata.create_all(bind=self.__engine)
        except OperationalError as error:
            logger.error(
                "Connection not established with the database {}".format(error)
            )
            raise Exception(f"Connection not established with the database: {error}")

    def get_session(self):
        return self.__db_session


session_md = Connection().get_session()

GLUE_DEFAULT_CODE = 2


def get_default_datetime() -> datetime:
    return datetime.now()


class BasicFieldsSchema:
    __table_args__ = {"schema": DbInfo.SCHEMA_MD.value}
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
    cancel_date = Column(DateTime, nullable=True)
    acquisition_date = Column(DateTime, nullable=True)


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


class BasicFieldsSchemaStage:
    __table_args__ = {"schema": DbInfo.SCHEMA_STAGE.value}
    created_at = Column(DateTime, nullable=False, default=get_default_datetime())


class TbQuotasSantanderPre(Base, BasicFieldsSchemaStage):
    __tablename__ = "tb_quotas_santander_pre"
    id_quotas_santander = Column(BigInteger, primary_key=True, index=True)
    cd_grupo = Column(String(10))
    cd_cota = Column(String(10))
    nr_contrato = Column(String(30))
    vl_devolver = Column(Numeric(precision=12, scale=2))
    vl_bem_atual = Column(Numeric(precision=12, scale=2))
    cd_produto = Column(String(10))
    pc_fc_pago = Column(Numeric(precision=12, scale=4))
    dt_canc = Column(DateTime)
    dt_venda = Column(DateTime)
    pz_restante_grupo = Column(Integer)
    qt_parcela_a_pagar = Column(Integer)
    nm_situ_entrega_bem = Column(String(255))
    pc_fr_pago = Column(Numeric(precision=12, scale=4))
    pc_tx_adm = Column(Numeric(precision=12, scale=2))
    pc_tx_pago = Column(Numeric(precision=12, scale=4))
    pz_contratado = Column(Integer)
    qt_parcela_paga = Column(Integer)
    pc_fundo_reserva = Column(Numeric(precision=12, scale=2))
    pz_decorrido_grupo = Column(Integer)
    is_processed = Column(Boolean)
    data_info = Column(DateTime)


class TbGroupsSantanderPre(Base, BasicFieldsSchemaStage):
    __tablename__ = "tb_grupos_santander_pre"
    id_grupos_santander = Column(BigInteger, primary_key=True, index=True)
    grupo = Column(String(10))
    cd_bem = Column(String(10))
    modalidade = Column(String(20))
    nm_bem = Column(String(255))
    vl_bem_atual = Column(Numeric(precision=12, scale=2))
    nm_situ_grupo = Column(String(30))
    is_processed = Column(Boolean)
    data_info = Column(DateTime)


# criação da repository


class ConnectionAbstractRepository:
    def __init__(self, session) -> None:
        self.session = session

    @staticmethod
    def update_modified(obj):
        obj.modified_at = datetime.now()
        obj.modified_by = GLUE_DEFAULT_CODE

    def add_and_commit(self, obj):
        self.session.add(obj)
        self.session.commit()


class AbstractRepository(ConnectionAbstractRepository):
    def __init__(self, model, session):
        super().__init__(session)
        self._model = model

    def commit(self) -> None:
        self.session.commit()

    def rollback(self) -> None:
        self.session.rollback()

    def flush_and_commit(
        self, new_entity: declarative_base = None, commit_at_the_end: bool = True
    ) -> None:
        try:
            if new_entity is not None:
                self.session.add(new_entity)

            self.session.flush()
        except IntegrityError as exception1:
            self.session.rollback()
            logger.exception(f"integrity_error: {str(exception1.detail)}")
            raise Exception(
                f"Erro de integridade ao tentar criar ou atualizar a entidade \
                {self._model.__tablename__}, error:{exception1}"
            )
        except SQLAlchemyError as exception2:
            self.session.rollback()
            logger.exception(f"generic_error:{str(exception2)}")
            raise Exception(
                f"Comportamento inesperado ao tentar criar ou atualizar a entidade \
                 {self._model.__tablename__}, error:{exception2}"
            )

        if commit_at_the_end:
            self.session.commit()

    def create(
        self, attributes: Dict[str, Any], commit_at_the_end: bool = True
    ) -> declarative_base:
        new_entity = self._model(**attributes)

        self.flush_and_commit(new_entity, commit_at_the_end)
        return new_entity

    def update(self, filters, **data):
        session = self.session()
        try:
            query = session.query(self._model)
            for attr, value in filters.items():
                if hasattr(self._model, attr):
                    query = query.filter(getattr(self._model, attr) == value)
                else:
                    logger.exception(
                        f"{self._model.__name__} does not have attribute: {attr}"
                    )

            obj = query.first()
            if obj:
                mapper = inspect(self._model)
                for key, value in data.items():
                    if mapper.has_property(key):
                        setattr(obj, key, value)
                    else:
                        logger.exception(
                            f"{self._model.__name__} does not have property: {key}"
                        )
                session.commit()
            else:
                logger.info("No matching record found")

        except DBAPIError as error:
            logger.exception(f"Update {self._model.__name__} error: {error}")
            session.rollback()
            raise Exception(
                f"Comportamento inesperado ao tentar atualizar a entidade \
                {self._model.__tablename__}, error:{error}"
            )

    def get_data_by_filters_and_column(self, filters, response_column):
        session = self.session()
        try:
            mapper = inspect(self._model)
            query = session.query(getattr(self._model, response_column))

            for column_name, variable_value in filters.items():
                column = getattr(mapper.columns, column_name, None)
                if column is None:
                    logger.exception(f"{self._model.__name__} not have a column named")
                    raise Exception(
                        f"{self._model.__name__} does not have a column named: {column_name}"
                    )
                query = query.filter(column == variable_value)

            result = query.scalar()  # Obter apenas o valor único da consulta
            return result
        except OperationalError as error:
            logger.exception(f"Error {self._model.__name__} get data. Error {error}")
            raise Exception(
                f"Comportamento inesperado ao retorna uma entidade \
                {self._model.__tablename__}, error:{error}"
            )

    def get_rows(self, query_dict):
        session = self.session()
        try:
            query = session.query(self._model)

            filters = []
            for column_name, value in query_dict.items():
                column = getattr(self._model, column_name, None)
                if column is not None:
                    filters.append(column == value)

            if filters:
                query = query.filter(and_(*filters))

            result = query.all()
            return result
        except OperationalError as error:
            logger.exception(f"Error {self._model.__name__} get data. Error {error}")
            raise Exception(
                f"Comportamento inesperado ao retorna uma linha \
                 {self._model.__tablename__}, error:{error}"
            )


class PlDataSourceRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(PlDataSource, session_md)

    def get_data_source_id(self, data_source_desc: str) -> int:
        try:
            source_id = (
                self.session.query(PlDataSource.data_source_id)
                .filter_by(data_source_desc=data_source_desc)
                .scalar()
            )
            logger.info("Get data_source_desc in PlDataSource success!")
            return source_id
        except SQLAlchemyError as exception2:
            self.session.rollback()
            logger.exception(f"generic_error:{str(exception2)}")
            raise Exception(
                f"Comportamento inesperado ao tentar obter o sourc_id \
                 {self._model.__tablename__}, error:{exception2}",
            )


class PlQuotaStatusRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(PlQuotaStatus, session_md)

    def insert_quota_status(self, quota_id_md_quota: int, status_type: int):
        quota_status = {
            "quota_id": quota_id_md_quota,
            "quota_status_type_id": status_type,
            "valid_from": get_default_datetime(),
            "created_at": get_default_datetime(),
            "modified_at": get_default_datetime(),
            "created_by": GLUE_DEFAULT_CODE,
            "modified_by": GLUE_DEFAULT_CODE,
            "is_deleted": False,
        }
        logger.info("Quota status inserted with sucess in PLQuotaStatus!")
        self.create(quota_status)
        name_column = "quota_status_id"
        quota_id_md = {"quota_id": quota_id_md_quota}
        quota_status_id = self.get_data_by_filters_and_column(quota_id_md, name_column)
        return quota_status_id

    def update_quota_status(self, quota_id_md_quota) -> None:
        quota_status_to_update = {"quota_id": quota_id_md_quota}
        try:
            quota_status_update = (
                self.session.query(PlQuotaStatus)
                .filter_by(
                    quota_id=quota_status_to_update["quota_id"],
                    valid_to=None,
                    is_deleted=False,
                )
                .first()
            )

            quota_status_update.valid_to = (get_default_datetime(),)
            self.update_modified(quota_status_update)
            self.add_and_commit(quota_status_to_update)

            logger.info("Update date quota in PlQuotas success!")
        except SQLAlchemyError as exception2:
            self.session.rollback()
            logger.exception(
                "Error when search/update data in PlQuota:{}".format(exception2)
            )
            raise Exception(
                f"Comportamento inesperado ao tentar atualizar quotas_status \
                {self._model.__tablename__}, error:{exception2}"
            )


class PlAdministratorRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(PlAdministrator, session_md)

    def read_adm(self, administrator_desc: str):
        administrator_id = "administrator_id"
        administrator = {"administrator_desc": administrator_desc}
        id_adm = self.get_data_by_filters_and_column(administrator, administrator_id)
        logger.info("Get id_adm in PlAdministrator success!")
        return id_adm


class PlGroupRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(PlGroup, session_md)

    def read_groups(self, adm_id) -> list:
        groups_md_quota = self.session.query(PlGroup).filter_by(
            administrator_id=adm_id, is_deleted=False
        )
        groups = []
        for data in groups_md_quota:
            groups.append(data.__dict__)
        logger.info("Search groups in PlGroup success!")
        return groups

    def insert_new_group(
        self, code_group: str, row: dict, id_adm: int, group_end_date
    ) -> dict:
        group_to_insert = {
            "group_code": code_group,
            "group_deadline": row["pz_contratado"],
            "administrator_id": id_adm,
            "group_closing_date": group_end_date,
            "created_at": get_default_datetime(),
            "modified_at": get_default_datetime(),
            "created_by": GLUE_DEFAULT_CODE,
            "modified_by": GLUE_DEFAULT_CODE,
            "is_deleted": False,
        }

        self.create(group_to_insert)
        logger.info("Group inserted with success in PlGroup!")
        group_code = {"group_code": code_group}
        group_pl = self.get_rows(group_code)
        return group_pl[0].__dict__


class PlQuotaRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(PlQuota, session_md)

    def quotas_all(self, adm_id) -> list:
        try:
            quotas_md_quota = (
                self.session.query(PlQuota)
                .filter_by(administrator_id=adm_id, is_deleted=False)
                .all()
            )
            quotas = []
            for quota in quotas_md_quota:
                quotas.append(quota.__dict__)
            logger.info("Search all quotas in PlQuota success!")
            return quotas
        except SQLAlchemyError as exception:
            self.session.rollback()
            logger.exception("Error of connection with database:{}".format(exception))
            raise Exception(
                f"Comportamento inesperado ao tentar obter quotas \
                {self._model.__tablename__}, error:{exception}",
            )

    def quota_code(self) -> Union[int, None]:
        try:
            max_quota_code = self.session.query(func.max(PlQuota.quota_id)).scalar()
            logger.info("Search max_quota_code in PlQuota success!")
            return max_quota_code

        except DatabaseError as error:
            logger.warning("Error fetching quota code:{}".format(error))
            return None

    def insert_new_quota(
        self,
        quota_code_final: str,
        row: dict,
        id_adm: int,
        quota_origin: int,
        group_id_md_quota: int,
        status_type: int,
    ) -> dict:
        try:
            quota_to_insert = {
                "quota_code": quota_code_final,
                "external_reference": row["nr_contrato"],
                "quota_number": row["cd_cota"],
                "total_installments": row["pz_contratado"],
                "is_contemplated": False,
                "is_multiple_ownership": False,
                "administrator_fee": row["pc_tx_adm"],
                "fund_reservation_fee": row["pc_fundo_reserva"],
                "info_date": row["data_info"],
                "quota_status_type_id": status_type,
                "administrator_id": id_adm,
                "group_id": group_id_md_quota,
                "contract_number": row["nr_contrato"],
                "quota_origin_id": quota_origin,
                "cancel_date": row["dt_canc"],
                "acquisition_date": row["dt_venda"],
                "created_at": get_default_datetime(),
                "modified_at": get_default_datetime(),
                "created_by": GLUE_DEFAULT_CODE,
                "modified_by": GLUE_DEFAULT_CODE,
                "is_deleted": False,
            }
            self.create(quota_to_insert)
            logger.info("Quota inserted in PlQuota with success!")
            quota_code = {"quota_code": quota_code_final}
            quota_pl = self.get_rows(quota_code)
            return quota_pl[0].__dict__

        except SQLAlchemyError as exception:
            self.session.rollback()
            logger.exception("Error when inserting in PlQuota:{}".format(exception))
            raise Exception(
                f"Comportamento inesperado ao tentar obter o id \
                {self._model.__tablename__}, error:{exception}"
            )

    def update_quota(self, quota_id_md_quota: int, status_type: int, row: dict) -> None:
        quota = {
            "quota_id": quota_id_md_quota,
            "quota_status_type_id": status_type,
            "cancel_date": row["dt_canc"],
            "acquisition_date": row["dt_venda"],
        }
        try:
            quota_update = (
                self.session.query(PlQuota)
                .filter_by(quota_id=quota["quota_id"], is_deleted=False)
                .first()
            )

            quota_update.quota_status_type_id = (quota["quota_status_type_id"],)
            quota_update.cancel_date = quota["cancel_date"]
            quota_update.acquisition_date = quota["acquisition_date"]

            self.update_modified(quota_update)
            self.add_and_commit(quota_update)

        except SQLAlchemyError as exception:
            self.session.rollback()
            logger.exception(
                "Error when search/update data in PlQuota:{}".format(exception)
            )
            raise Exception(
                f"Comportamento inesperado ao atualizar na PlQuota \
                {self._model.__tablename__}, error:{exception}"
            )

    def update_quota_origin(self, quota_to_update: dict) -> None:
        try:
            quota_update = (
                self.session.query(PlQuota)
                .filter_by(quota_id=quota_to_update["quota_id"], is_deleted=False)
                .first()
            )
            quota_update.QUOTA_ORIGIN_ID = (quota_to_update["quota_origin_id"],)
            self.update_modified(quota_update)
            self.add_and_commit(quota_update)

        except SQLAlchemyError as exception:
            self.session.rollback()
            logger.exception(
                "Error when search/update data in PlQuota:{}".format(exception)
            )
            raise Exception(
                f"Comportamento inesperado ao atualizar na PlQuota \
                {self._model.__tablename__}, error:{exception}"
            )

    def update_quota_number(self, quota_to_update: dict) -> None:
        try:
            quota_update = (
                self.session.query(PlQuota)
                .filter_by(quota_id=quota_to_update["quota_id"], is_deleted=False)
                .first()
            )
            quota_update.quota_number = (quota_to_update["quota_number"],)
            quota_update.quota_status_type_id = (
                quota_to_update["quota_status_type_id"],
            )
            self.update_modified(quota_update)
            self.add_and_commit(quota_update)

        except SQLAlchemyError as exception:
            self.session.rollback()
            logger.exception("Error when /update data in PlQuota:{}".format(exception))
            raise Exception(
                f"Comportamento inesperado ao atualizar na PlQuota \
                {self._model.__tablename__}, error:{exception}",
            )

    def update_quota_quotas(self, quota_to_update: dict) -> None:
        try:
            quota_update = (
                self.session.query(PlQuota)
                .filter_by(quota_id=quota_to_update["quota_id"], is_deleted=False)
                .first()
            )

            quota_update.quota_status_type_id = (
                quota_to_update["quota_status_type_id"],
            )
            quota_update.quota_number = (quota_to_update["quota_number"],)
            quota_update.contract_number = (quota_to_update["contract_number"],)

            self.update_modified(quota_update)
            self.add_and_commit(quota_update)
            logger.info("Update quota quotas in PlQuota success!")
        except SQLAlchemyError as exception:
            self.session.rollback()
            logger.exception("Error when /update data in PlQuota:{}".format(exception))
            raise Exception(
                f"Comportamento inesperado ao atualizar na PlQuota \
                {self._model.__tablename__}, error:{exception}"
            )

    def update_quota_code(self, quota_id_md_quota: int, quota_code_prefix: str) -> None:
        quota_code_insert = str(quota_id_md_quota).rjust(6, "0")
        quota_code_update = str(generate_luhn_check_digit(quota_code_insert))
        quota_code_final_update = (
            quota_code_prefix + quota_code_insert + quota_code_update
        )
        quota_code_update = {
            "quota_id": quota_id_md_quota,
            "quota_code": quota_code_final_update,
        }
        try:
            quota_update = (
                self.session.query(PlQuota)
                .filter_by(quota_id=quota_code_update["quota_id"], is_deleted=False)
                .first()
            )

            quota_update.quota_code = (quota_code_update["quota_code"],)
            self.add_and_commit(quota_update)

        except SQLAlchemyError as exception:
            self.session.rollback()
            logger.exception("Error when /update data in PlQuota:{}".format(exception))
            raise Exception(
                f"Comportamento inesperado ao atualizar na PlQuota \
                {self._model.__tablename__}, error:{exception}"
            )


class PlQuotaFieldUpdateDateRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(PlQuotaFieldUpdateDate, session_md)

    def insert_quota_field_update_date(
        self,
        switch: dict,
        case_default,
        quota_id_md_quota: int,
        row: dict,
        data_source_id,
    ) -> None:
        fields_inserted_quota_history = [
            "installments_paid_number",
            "old_quota_number",
            "old_digit",
            "per_mutual_fund_paid",
            "asset_value",
            "asset_type_id",
            "per_adm_paid",
            "per_reserve_fund_paid",
            "current_assembly_number",
        ]

        for field in fields_inserted_quota_history:
            history_field_id = switch.get(field, case_default)
            quota = {
                "update_date": row["data_info"],
                "quota_history_field_id": history_field_id,
                "data_source_id": data_source_id,
                "quota_id": quota_id_md_quota,
                "created_at": get_default_datetime(),
                "modified_at": get_default_datetime(),
                "created_by": GLUE_DEFAULT_CODE,
                "modified_by": GLUE_DEFAULT_CODE,
                "is_deleted": False,
            }
            self.create(quota)
            logger.info(
                "Quota field update date inserted in PlQuotaFieldUpdateDate with success!"
            )

    def update_quota_field_update_date(
        self,
        switch: dict,
        case_default,
        quota_id_md_quota: int,
        row: dict,
        data_source_id: int,
    ) -> None:
        fields_inserted_quota_history = [
            "installments_paid_number",
            "old_quota_number",
            "old_digit",
            "per_mutual_fund_paid",
            "asset_value",
            "asset_type_id",
            "per_adm_paid",
            "per_reserve_fund_paid",
            "current_assembly_number",
        ]

        for field in fields_inserted_quota_history:
            history_field_id = switch.get(field, case_default)
            quota = {
                "update_date": row["created_at"],
                "quota_history_field_id": history_field_id,
                "data_source_id": data_source_id,
                "quota_id": quota_id_md_quota,
            }
            quota_field_update_date_update = (
                self.session.query(PlQuotaFieldUpdateDate)
                .filter_by(
                    quota_id=quota["quota_id"],
                    quota_history_field_id=quota["quota_history_field_id"],
                    is_deleted=False,
                )
                .first()
            )

            quota_field_update_date_update.update_date = (quota["update_date"],)
            quota_field_update_date_update.data_source_id = (quota["data_source_id"],)
            self.update_modified(quota_field_update_date_update)
            self.add_and_commit(quota_field_update_date_update)
            logger.info("Update date in PlQuotaFieldUpdateDate success!")


class PlQuotaHistoryDetailRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(PlQuotaHistoryDetail, session_md)

    def insert_new_quota_history(
        self,
        switch: dict,
        quota_id_md_quota: int,
        row: dict,
        total_assembly: int,
        default_asset,
        quota_history_md_quota,
    ) -> None:
        asset_type = switch.get(row["cd_produto"], default_asset)
        if quota_history_md_quota:
            quota = {}
            for keyword in switch:
                quota[keyword] = quota_history_md_quota[keyword]
        else:
            quota = {}
            for keyword in switch:
                quota[keyword] = None
        quota["quota_id"] = quota_id_md_quota
        quota["old_quota_number"] = row["cd_cota"]
        quota["installments_paid_number"] = row["qt_parcela_paga"]
        quota["per_mutual_fund_paid"] = row["pc_fc_pago"]
        quota["asset_value"] = row["vl_bem_atual"]
        quota["asset_type_id"] = asset_type
        quota["info_date"] = row["data_info"]
        quota["current_assembly_number"] = total_assembly
        quota["per_adm_paid"] = row["pc_tx_pago"]
        quota["per_reserve_fund_paid"] = row["pc_fr_pago"]
        quota["valid_from"] = get_default_datetime()
        quota["valid_to"] = None

        self.create(quota, True)
        logger.info("Add quota detail in PlQuotaHistoryDetail success!")

    def search_quota_history_detail(self, quota_id: int) -> Union[dict, None]:
        try:
            quota_detail = (
                self.session.query(PlQuotaHistoryDetail)
                .filter_by(quota_id=quota_id, valid_to=None, is_deleted=False)
                .first()
            )
            logger.info("Search quota_detail in PlQuotaHistoryDetail success!")
            if quota_detail is not None and quota_detail.__dict__:
                return quota_detail.__dict__
            else:
                return None
        except DatabaseError as error:
            self.session.rollback()
            logger.exception("Error when search data in StageRaw:{}".format(error))
            raise Exception(
                f"Comportamento inesperado ao atualizar na PlQuota \
                {self._model.__tablename__} error:{error}"
            )

    def update_valid_to(self, quota_id: int) -> None:
        try:
            quota_history_detail = (
                self.session.query(PlQuotaHistoryDetail)
                .filter_by(quota_id=quota_id, valid_to=None, is_deleted=False)
                .first()
            )

            quota_history_detail.valid_to = get_default_datetime()
            self.update_modified(quota_history_detail)
            self.add_and_commit(quota_history_detail)
            logger.info("Update valid_to in PlQuotaHistory success!")
        except OperationalError as error:
            self.session.rollback()
            logger.exception(
                "Error when search data in PlQuotaHistoryDetail:{}".format(error)
            )
            raise Exception(
                f"Comportamento inesperado ao atualizar na PlQuotaHistory \
                {self._model.__tablename__} error:{error}"
            )


class PlQuotaOwnerRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(PlQuotaOwner, session_md)

    def update_valid_to(self, quota_id: int) -> None:
        try:
            quota_owner = (
                self.session.query(PlQuotaOwner)
                .filter_by(quota_id=quota_id, valid_to=None, is_deleted=False)
                .first()
            )

            quota_owner.valid_to = get_default_datetime()
            self.update_modified(quota_owner)
            self.add_and_commit(quota_owner)
            logger.info("Update valid_to in quota in PlQuotaOwner!")
        except OperationalError as error:
            self.session.rollback()
            logger.exception(
                "Error when trying to update data in PlQuotaOwner:{}".format(error)
            )
            raise Exception(
                f"Comportamento inesperado ao atualizar na PlQuotaOwner \
                {self._model.__tablename__} error:{error}"
            )


class TbQuotasSantanderPreRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(TbQuotasSantanderPre, session_md)

    def read_data_stage_raw(self) -> list:
        try:
            stage_raw_tb_quotas_santander = (
                self.session.query(TbQuotasSantanderPre)
                .filter_by(is_processed=False)
                .all()
            )

            quotas_santander = []
            for data in stage_raw_tb_quotas_santander:
                quotas_santander.append(data.__dict__)
            logger.info("Read data StageRaw success! data: {}".format(quotas_santander))
            return quotas_santander
        except DatabaseError as error:
            self.session.rollback()
            logger.exception("Error when search data in StageRaw:{}".format(error))
            raise Exception(
                f"Comportamento inesperado ao atualizar na PlQuotaOwner \
                 {self._model.__tablename__}, error:{error}",
            )

    def update_is_processed(self, id_quotas_santander_pre: int) -> None:
        try:
            quotas_santander_pre = (
                self.session.query(TbQuotasSantanderPre)
                .filter_by(id_quotas_santander=id_quotas_santander_pre)
                .first()
            )

            quotas_santander_pre.is_processed = True
            self.add_and_commit(quotas_santander_pre)
            logger.info("Update tb_quotas_santander_pre is_processed=true!")
        except OperationalError as error:
            self.session.rollback()
            logger.exception("Error when update data in StageRaw:{}".format(error))
            raise Exception(
                f"Comportamento inesperado ao atualizar na PlQuotaOwner \
                 {self._model.__tablename__}, error:{error}",
            )


class TbGroupsSantanderPreRepository(AbstractRepository):
    def __init__(self) -> None:
        super().__init__(TbGroupsSantanderPre, session_md)

    def read_data_stage_raw(self) -> list:
        try:
            stage_raw_tb_groups_santander = (
                self.session.query(TbGroupsSantanderPre)
                .filter_by(is_processed=False)
                .all()
            )
            groups_santander = []
            for data in stage_raw_tb_groups_santander:
                groups_santander.append(data.__dict__)
            logger.info("Read data StageRaw success! data: {}".format(groups_santander))
            return groups_santander
        except DatabaseError as error:
            self.session.rollback()
            logger.exception("Error when search data in StageRaw:{}".format(error))
            raise Exception(
                f"Comportamento inesperado ao atualizar na PlQuotaOwner \
                {self._model.__tablename__}, error:{error}"
            )


def generate_luhn_check_digit(sequence):
    sequence = str(sequence)
    reversed_sequence = sequence[::-1]
    doubled_sequence = ""

    for i, digit in enumerate(reversed_sequence):
        if i % 2 == 0:
            doubled_digit = int(digit) * 2
            doubled_digit -= 9 if doubled_digit > 9 else 0
            doubled_sequence += str(doubled_digit)
        else:
            doubled_sequence += digit

    sum_of_digits = sum(int(digit) for digit in doubled_sequence)
    check_digit = (10 - (sum_of_digits % 10)) % 10

    return check_digit


class EtlAbstract:
    class Constants(Enum):
        CASE_DEFAULT_TYPES = 5
        CASE_DEFAULT_ASSET_TYPES = 7
        CASE_DEFAULT_HISTORY_DETAIL_FIELD = 0
        QUOTA_ORIGIN_ADM = 1
        QUOTA_ORIGIN_CUSTOMER = 2
        QUOTA_ORIGIN_UND = 3

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
        ADM_NAME = "SANTANDER ADM. CONS. LTDA"
        QUOTA_CODE_PREFIX = "BZ"

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
        SERVICOS = 5

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

    switch_asset_type = {
        "AUTO": AssetTypeEnum.VEICULOS_LEVES.value,
        "CAMIN": AssetTypeEnum.VEICULOS_PESADOS.value,
        "IMOVEL": AssetTypeEnum.IMOVEIS.value,
        "SERVIC": AssetTypeEnum.SERVICOS.value,
    }

    switch_status = {
        "Bem Pendente de Entrega": StatusTypeEnum.ATIVOS.value,
        "Bem a Contemplar": StatusTypeEnum.DESISTENTES.value,
        "Cota Cancelada": StatusTypeEnum.ATIVOS.value,
    }

    switch_multiple_owner = {"N": False, "S": True}

    def __init__(self):
        self.pl_administrator = PlAdministratorRepository()
        self.pl_group = PlGroupRepository()
        self.pl_quota = PlQuotaRepository()
        self.quotas_santander_pre = TbQuotasSantanderPreRepository()
        self.groups_santander_pre = TbGroupsSantanderPreRepository()
        self.pl_quota_status = PlQuotaStatusRepository()
        self.pl_quota_history_detail = PlQuotaHistoryDetailRepository()
        self.pl_quota_field_update_date = PlQuotaFieldUpdateDateRepository()
        self.pl_data_source = PlDataSourceRepository()
        self.pl_quota_owner = PlQuotaOwnerRepository()

        self.id_adm = self.pl_administrator.read_adm(self.EtlInfo.ADM_NAME.value)
        self.groups_md_quota = self.pl_group.read_groups(self.id_adm)
        self.quota_code_md_quota = self.pl_quota.quota_code()
        self.quotas_md_quota = self.pl_quota.quotas_all(self.id_adm)
        self.quotas_stage_raw = self.quotas_santander_pre.read_data_stage_raw()
        self.groups_stage_raw = self.groups_santander_pre.read_data_stage_raw()
        self.data_source_id = self.pl_data_source.get_data_source_id("FILE")

    @staticmethod
    def string_right_justified(group: str) -> str:
        if len(str(group)) == 5:
            code_group = str(group)
        else:
            code_group = str(group).rjust(5, "0")
        return code_group

    @staticmethod
    def get_dict_by_id(
        id_item: str, data_list: list, field_name: str
    ) -> Union[dict, None]:
        for item_list in data_list:
            if item_list[field_name] == id_item:
                return item_list
        return None

    def insert_quota_history_and_update_date(
        self, quota_id_md_quota, row, total_assembly
    ):
        self.pl_quota_history_detail.insert_new_quota_history(
            self.switch_quota_history_field,
            quota_id_md_quota,
            row,
            total_assembly,
            self.Constants.CASE_DEFAULT_ASSET_TYPES.value,
            None,
        )
        self.pl_quota_field_update_date.insert_quota_field_update_date(
            self.switch_quota_history_field,
            self.Constants.CASE_DEFAULT_HISTORY_DETAIL_FIELD.value,
            quota_id_md_quota,
            row,
            self.data_source_id,
        )

    def md_quota_quota_none(self, row, md_quota_group, code_group):
        quota_code_tb = str(self.quota_code_md_quota + 1).rjust(6, "0")
        self.quota_code_md_quota = self.quota_code_md_quota + 1
        quota_code_suffix = str(generate_luhn_check_digit(quota_code_tb))
        quota_code_final = (
            self.EtlInfo.QUOTA_CODE_PREFIX.value + quota_code_tb + quota_code_suffix
        )

        info_date = row["data_info"]
        today = datetime.today()

        assembly_since_statement = relativedelta.relativedelta(today, info_date).months
        total_assembly = row["pz_decorrido_grupo"] + assembly_since_statement
        assembly_to_end = row["pz_contratado"] - total_assembly

        group_end_date = today + relativedelta.relativedelta(months=assembly_to_end)

        if md_quota_group is None:
            group_md_quota = self.pl_group.insert_new_group(
                code_group, row, self.id_adm, group_end_date
            )
            group_id_md_quota = group_md_quota["group_id"]
            self.groups_md_quota.append(group_md_quota)

        else:
            group_id_md_quota = md_quota_group["group_id"]

        status_type = self.switch_status.get(
            row["nm_situ_entrega_bem"], self.Constants.CASE_DEFAULT_TYPES.value
        )

        quota_md_quota = self.pl_quota.insert_new_quota(
            quota_code_final,
            row,
            self.id_adm,
            self.Constants.QUOTA_ORIGIN_ADM.value,
            group_id_md_quota,
            status_type,
        )
        quota_id_md_quota = quota_md_quota["quota_id"]
        self.quotas_md_quota.append(quota_md_quota)
        self.pl_quota.update_quota_code(
            quota_id_md_quota, self.EtlInfo.QUOTA_CODE_PREFIX.value
        )
        self.pl_quota_status.insert_quota_status(quota_id_md_quota, status_type)
        self.insert_quota_history_and_update_date(
            quota_id_md_quota, row, total_assembly
        )

    def md_quota_quota_exist(self, row, md_quota_quota):
        quota_id_md_quota = md_quota_quota["quota_id"]
        status_type = self.switch_status.get(
            row["nm_situ_entrega_bem"], self.Constants.CASE_DEFAULT_TYPES.value
        )
        info_date = row["data_info"]
        today = datetime.today()

        assembly_since_statement = relativedelta.relativedelta(today, info_date).months
        total_assembly = row["pz_decorrido_grupo"] + assembly_since_statement

        if (
            md_quota_quota["info_date"] < row["data_info"]
            and md_quota_quota["quota_status_type_id"] != status_type
        ):
            self.pl_quota.update_quota(quota_id_md_quota, status_type, row)
            self.pl_quota_status.update_quota_status(quota_id_md_quota)
            self.pl_quota_status.insert_quota_status(quota_id_md_quota, status_type)
        quota_history_detail_md_quota = (
            self.pl_quota_history_detail.search_quota_history_detail(quota_id_md_quota)
        )
        if (
            quota_history_detail_md_quota
            and quota_history_detail_md_quota["info_date"] < row["data_info"]
        ):
            self.pl_quota_history_detail.update_valid_to(quota_id_md_quota)
            self.insert_quota_history_and_update_date(
                quota_id_md_quota, row, total_assembly
            )
        else:
            self.insert_quota_history_and_update_date(
                quota_id_md_quota, row, total_assembly
            )

    def santander_quotas_flow(self) -> None:
        for row in self.quotas_stage_raw:
            code_group = self.string_right_justified(row["cd_grupo"])
            md_quota_group = self.get_dict_by_id(
                code_group, self.groups_md_quota, "group_code"
            )

            md_quota_quota = self.get_dict_by_id(
                row["nr_contrato"], self.quotas_md_quota, "external_reference"
            )

            if md_quota_quota is None:
                self.md_quota_quota_none(row, md_quota_group, code_group)
            else:
                self.md_quota_quota_exist(row, md_quota_quota)

            self.quotas_santander_pre.update_is_processed(row["id_quotas_santander"])


class Etl(EtlAbstract):
    def __init__(self):
        super().__init__()

    def start(self):
        self.santander_quotas_flow()


if __name__ == "__main__":
    etl = Etl()
    etl.start()
    # job.commit()
