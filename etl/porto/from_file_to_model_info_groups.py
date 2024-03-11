# imports do glue

from awsglue.utils import getResolvedOptions
from bazartools.common.database.glueConnection import GlueConnection
from bazartools.common.database.assetCodeBuilder import build_asset_code
import requests
import boto3
from datetime import datetime
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
    Float,
)

from sqlalchemy.ext.declarative import declarative_base

# imports loggers:
import logging
from typing import Literal


from logging import StreamHandler, Formatter


class Logger:
    def __init__(
        self, name: str = "etl-group-infos-porto", level: Literal[20] = logging.INFO
    ) -> None:
        self.log = self.get_logger(name, level)

    @classmethod
    def get_logger(cls, name: str, level: Literal[20] = logging.INFO) -> logging.Logger:
        formatter = Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        stream_handler = StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        loggers = logging.getLogger(name)
        loggers.setLevel(level)
        loggers.addHandler(stream_handler)
        return loggers


logger = Logger().log


class DbInfo(Enum):
    args = getResolvedOptions(
        sys.argv, ["db-host", "db-port", "db-name", "db-user", "db-pass"]
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


class TbGruposPortoPre(Base):
    __tablename__ = "tb_grupos_porto_pre"
    id_grupos_porto = Column(BigInteger, primary_key=True, index=True)
    grupo = Column(String(255))
    end_group_m = Column(Integer)
    group_variances = Column(Integer)
    available_credit_in_group_0 = Column(Numeric(precision=12, scale=2))
    available_credit_in_group_1 = Column(Numeric(precision=12, scale=2))
    available_credit_in_group_2 = Column(Numeric(precision=12, scale=2))
    available_credit_in_group_3 = Column(Numeric(precision=12, scale=2))
    available_credit_in_group_4 = Column(Numeric(precision=12, scale=2))
    available_credit_in_group_5 = Column(Numeric(precision=12, scale=2))
    available_credit_in_group_6 = Column(Numeric(precision=12, scale=2))
    available_credit_in_group_7 = Column(Numeric(precision=12, scale=2))
    available_credit_in_group_8 = Column(Numeric(precision=12, scale=2))
    available_credit_in_group_9 = Column(Numeric(precision=12, scale=2))
    qtd_lances_maximos_1 = Column(Numeric(precision=12, scale=2))
    qtd_lances_maximos_2 = Column(Numeric(precision=12, scale=2))
    qtd_lances_maximos_3 = Column(Numeric(precision=12, scale=2))
    qtd_lances_contemplados_1 = Column(Numeric(precision=12, scale=2))
    qtd_lances_contemplados_2 = Column(Numeric(precision=12, scale=2))
    qtd_lances_contemplados_3 = Column(Numeric(precision=12, scale=2))
    pct_lances_contemplados_1_1 = Column(Numeric(precision=12, scale=2))
    pct_lances_contemplados_1_2 = Column(Numeric(precision=12, scale=2))
    pct_lances_contemplados_1_3 = Column(Numeric(precision=12, scale=2))
    pct_lances_contemplados_1_4 = Column(Numeric(precision=12, scale=2))
    pct_lances_contemplados_1_5 = Column(Numeric(precision=12, scale=2))
    pct_lances_contemplados_1_6 = Column(Numeric(precision=12, scale=2))
    end_of_group_months = Column(Integer)
    data_info = Column(DateTime)
    created_at = Column(DateTime)
    is_processed = Column(Boolean)


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


class PlBidType(Base, BasicFields):
    __tablename__ = "pl_bid_type"
    bid_type_id = Column(BigInteger, primary_key=True, index=True)
    bid_type_code = Column(String(10), nullable=False)
    bid_type_desc = Column(String(255), nullable=False)


class PlBidValueType(Base, BasicFields):
    __tablename__ = "pl_bid_value_type"
    bid_value_type_id = Column(BigInteger, primary_key=True, index=True)
    bid_value_type_code = Column(String(10), nullable=False)
    bid_value_type_desc = Column(String(255), nullable=False)


class PlVacancies(Base, BasicFields):
    __tablename__ = "pl_group_vacancies"
    group_vacancies_id = Column(BigInteger, primary_key=True, index=True)
    vacancies = Column(Integer, nullable=False)
    info_date = Column(DateTime, nullable=False)
    group_id = Column(Integer, ForeignKey(PlGroup.group_id))
    valid_from = Column(DateTime)
    valid_to = Column(DateTime)


class PlBid(Base, BasicFields):
    __tablename__ = "pl_bid"
    bid_id = Column(BigInteger, primary_key=True, index=True)
    value = Column(Float, nullable=False)
    assembly_date = Column(DateTime, nullable=True)
    assembly_order = Column(Integer, nullable=True)
    info_date = Column(DateTime, nullable=True)
    group_id = Column(Integer, ForeignKey(PlGroup.group_id))
    bid_type_id = Column(Integer, ForeignKey(PlBidType.bid_type_id))
    bid_value_type_id = Column(Integer, ForeignKey(PlBidValueType.bid_value_type_id))


class PlAssetType(Base, BasicFields):
    __tablename__ = "pl_assset_type"
    asset_type_id = Column(BigInteger, primary_key=True, index=True)
    asset_type_code = Column(String(20), nullable=False)
    asset_type_code_ext = Column(String(10), nullable=True)
    asset_type_desc = Column(String(255), nullable=False)


class PlAsset(Base, BasicFields):
    __tablename__ = "pl_asset"
    asset_id = Column(BigInteger, primary_key=True, index=True)
    asset_code = Column(String(20), nullable=True)
    asset_adm_code = Column(Integer, nullable=True)
    asset_desc = Column(String(255), nullable=False)
    asset_value = Column(Float, nullable=False)
    asset_type_id = Column(Integer, ForeignKey(PlAssetType.asset_type_id))
    PLAN = Column(String(255), nullable=False)
    administrator_fee = Column(Float, nullable=False)
    fund_reservation_fee = Column(Float, nullable=False)
    info_date = Column(DateTime)
    valid_from = Column(DateTime)
    valid_to = Column(DateTime)
    group_id = Column(Integer, ForeignKey(PlGroup.group_id))


# criação da repository
class TypeSchemaEnum(Enum):
    MD_QUOTA = "md_cota"
    STAGE_RAW = "stage_raw"


class ConnectionAbstractRepository:
    def __init__(self, type_schema: str) -> None:
        self._session = Connection(type_schema).get_session()


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
                administrator_id=group_data["administrator_id"],
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


class TbGruposPortoRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.STAGE_RAW.value)

    def read_data_stage_raw(self) -> list:
        try:
            stage_raw_tb_grupos_porto = (
                self._session.query(TbGruposPortoPre)
                .filter_by(is_processed=False)
                .limit(1000)
                .all()
            )
            grupos_porto = []
            for data in stage_raw_tb_grupos_porto:
                grupos_porto.append(data.__dict__)
            logger.info("Read data StageRaw success! data: {}".format(grupos_porto))
            return grupos_porto
        except DatabaseError as error:
            logger.error("Error when search data in StageRaw:{}".format(error))

    def update_is_processed(self, id_grupos_porto: int) -> None:
        try:
            groups_porto_processed = (
                self._session.query(TbGruposPortoPre)
                .filter_by(id_grupos_porto=id_grupos_porto)
                .first()
            )
            groups_porto_processed.is_processed = True
            self._session.add(groups_porto_processed)
            self._session.commit()
            logger.info("Update TbGruposPortoPre is_processed=true!")
        except OperationalError as error:
            logger.error(
                "Error when search/update data in TbGruposPortoPre:{}".format(error)
            )


class PlBidTypeRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.MD_QUOTA.value)

    def read_bid_type(self, bid_type_desc: str) -> int:
        bid_type_id = (
            self._session.query(PlBidType.bid_type_id)
            .filter_by(bid_type_desc=bid_type_desc)
            .scalar()
        )
        logger.info("Get bid_type_id in PlBidType success!")
        return bid_type_id


class PlBidValueTypeRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.MD_QUOTA.value)

    def read_bid_type(self, bid_value_type_desc: str) -> int:
        bid_value_type_id = (
            self._session.query(PlBidValueType.bid_value_type_id)
            .filter_by(bid_value_type_desc=bid_value_type_desc)
            .scalar()
        )
        logger.info("Get bid_type_id in PlBidValueType success!")
        return bid_value_type_id


class PlBidRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.MD_QUOTA.value)

    def insert_new_bid(self, bid_data: dict) -> Column:
        try:
            new_bid = PlBid(
                value=bid_data["value"],
                info_date=bid_data["info_date"],
                group_id=bid_data["group_id"],
                bid_type_id=bid_data["bid_type_id"],
                bid_value_type_id=bid_data["bid_value_type_id"],
                created_at=get_default_datetime(),
                modified_at=get_default_datetime(),
                created_by=GLUE_DEFAULT_CODE,
                modified_by=GLUE_DEFAULT_CODE,
                is_deleted=False,
            )
            self._session.add(new_bid)
            self._session.commit()
            self._session.refresh(new_bid)
            logger.info("Bid inserted with success in PlBid!")
            return new_bid.__dict__

        except DatabaseError as error:
            logger.error("Error: Bid not inserted in PlBid:{}".format(error))


class PlAssetRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.MD_QUOTA.value)

    def insert_new_asset(self, asset_data: dict) -> Column:
        try:
            new_asset = PlAsset(
                asset_desc=asset_data["asset_desc"],
                asset_code=asset_data["asset_code"],
                asset_value=asset_data["asset_value"],
                asset_type_id=asset_data["asset_type_id"],
                info_date=asset_data["info_date"],
                valid_from=asset_data["valid_from"],
                group_id=asset_data["group_id"],
                created_at=get_default_datetime(),
                modified_at=get_default_datetime(),
                created_by=GLUE_DEFAULT_CODE,
                modified_by=GLUE_DEFAULT_CODE,
                is_deleted=False,
            )
            self._session.add(new_asset)
            self._session.commit()
            self._session.refresh(new_asset)
            logger.info("Asset inserted with success in PlAsset!")
            return new_asset.__dict__

        except DatabaseError as error:
            logger.error("Error: Asset not inserted in PlAsset:{}".format(error))

    def update_asset_code(self, asset: dict):
        try:
            asset_update = (
                self._session.query(PlAsset)
                .filter_by(asset_id=asset["asset_id"], is_deleted=False)
                .first()
            )
            asset_update.asset_code = (asset["asset_code"],)
            self._session.add(asset_update)
            self._session.commit()
            logger.info("Update PlAsset asset_code=true!")
        except OperationalError as error:
            logger.error("Error when search/update data in PlAsset:{}".format(error))

    def update_valid_to(self, asset_id: int) -> None:
        try:
            assets_invalidate = (
                self._session.query(PlAsset).filter_by(asset_id=asset_id).first()
            )
            assets_invalidate.valid_to = get_default_datetime()
            self._session.add(assets_invalidate)
            self._session.commit()
            logger.info("Update PlAsset is_processed=true!")
        except OperationalError as error:
            logger.error("Error when search/update data in PlAsset:{}".format(error))

    def read_assets(self) -> list:
        try:
            assets_md_quota = (
                self._session.query(PlAsset)
                .filter_by(is_deleted=False, valid_to=None)
                .all()
            )
            assets = []
            for data in assets_md_quota:
                assets.append(data.__dict__)
            logger.info("Read data PlAsset success!")
            return assets
        except DatabaseError as error:
            logger.error("Error when search data in PlAsset:{}".format(error))


class PlGroupVacanciesRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.MD_QUOTA.value)

    def insert_new_group_vacancies(self, group_vacancies_data: dict) -> Column:
        try:
            new_group_vacancies = PlVacancies(
                vacancies=group_vacancies_data["vacancies"],
                info_date=group_vacancies_data["info_date"],
                group_id=group_vacancies_data["group_id"],
                valid_from=group_vacancies_data["valid_from"],
                created_at=get_default_datetime(),
                modified_at=get_default_datetime(),
                created_by=GLUE_DEFAULT_CODE,
                modified_by=GLUE_DEFAULT_CODE,
                is_deleted=False,
            )
            self._session.add(new_group_vacancies)
            self._session.commit()
            self._session.refresh(new_group_vacancies)
            logger.info("Asset inserted with success in PlAsset!")
            return new_group_vacancies.__dict__

        except DatabaseError as error:
            logger.error(
                "Error: Asset not inserted in PlGroupVacancies:{}".format(error)
            )

    def update_valid_to(self, group_vacancies_id: int) -> None:
        try:
            group_vacancies_invalidate = (
                self._session.query(PlVacancies)
                .filter_by(group_vacancies_id=group_vacancies_id)
                .first()
            )
            group_vacancies_invalidate.valid_to = get_default_datetime()
            self._session.add(group_vacancies_invalidate)
            self._session.commit()
            logger.info("Update PlGroupVacancies valid_to")
        except OperationalError as error:
            logger.error(
                "Error when search/update data in PlGroupVacancies:{}".format(error)
            )

    def read_group_vacancies(self) -> list:
        try:
            group_vacancies_md_quota = (
                self._session.query(PlVacancies)
                .filter_by(is_deleted=False, valid_to=None)
                .all()
            )
            group_vacancies = []
            for data in group_vacancies_md_quota:
                group_vacancies.append(data.__dict__)
            logger.info("Read data PlGroupVacancies success!")
            return group_vacancies
        except DatabaseError as error:
            logger.error("Error when search data in PlGroupVacancies:{}".format(error))


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

    class EtlInfo(Enum):
        ADM_NAME = "PORTO SEGURO ADM. CONS. LTDA"

    class AssetTypeEnum(Enum):
        VEICULOS_PESADOS = 3
        VEICULOS_LEVES = 2
        IMOVEIS = 1
        MOTOCICLETAS = 4
        SERVICOS = 5

    switch_asset_type = {
        "CAMIN": AssetTypeEnum.VEICULOS_PESADOS.value,
        "AUTO": AssetTypeEnum.VEICULOS_LEVES.value,
        "IMOVEL": AssetTypeEnum.IMOVEIS.value,
        "MOTO": AssetTypeEnum.MOTOCICLETAS.value,
        "SERVIC": AssetTypeEnum.SERVICOS.value,
    }

    def __init__(self):
        self.batch_size = 1000
        self.pl_administrator = PlAdministratorRepository()
        self.pl_group = PlGroupRepository()
        self.group_porto_repository = TbGruposPortoRepository()
        self.pl_bid_repository = PlBidRepository()
        self.pl_asset_repository = PlAssetRepository()
        self.group_vacancies_repository = PlGroupVacanciesRepository()

        self.id_adm = self.pl_administrator.read_adm(self.EtlInfo.ADM_NAME.value)
        self.groups_md_quota = self.pl_group.read_groups(self.id_adm)
        self.groups_porto = self.group_porto_repository.read_data_stage_raw()
        self.md_quota_assets = self.pl_asset_repository.read_assets()
        self.group_vacancies_md_quota = (
            self.group_vacancies_repository.read_group_vacancies()
        )
        self.md_cota_connection_factory = GlueConnection(connection_name="md-cota")
        self.md_cota_connection = self.md_cota_connection_factory.get_connection()

    def start(self):
        try:
            self.groups_infos_porto_flow()
        except Exception as error:
            logger.error(error)
            raise error

    @staticmethod
    def get_dict_by_id(id_item: str, data_list: list, field_name: str) -> dict:
        for item_list in data_list:
            if item_list[field_name] == id_item:
                return item_list
        return None

    @staticmethod
    def get_list_by_id(id_item: str, data_list: list, field_name: str) -> list:
        items = []
        for item_list in data_list:
            if item_list[field_name] == id_item:
                items.append(item_list)
        return items

    @staticmethod
    def string_right_justified(group: str) -> str:
        if len(str(group)) == 5:
            code_group = str(group)
        else:
            code_group = str(group).rjust(5, "0")
        return code_group

    def groups_infos_porto_flow(self):
        md_quota_assets = self.md_quota_assets
        md_quota_group_vacancies = self.group_vacancies_md_quota
        while True:
            self.groups_porto = self.group_porto_repository.read_data_stage_raw()
            if not self.groups_porto:
                break
            for row in self.groups_porto:
                if len(row["grupo"]) > 5:
                    code_group = row["grupo"][-5:]

                else:
                    code_group = self.string_right_justified(row["grupo"])

                logger.info(f"code_group: {code_group}")

                md_quota_group = self.get_dict_by_id(
                    code_group, self.groups_md_quota, "group_code"
                )

                logger.info(f"md_quota_group: {md_quota_group}")

                if md_quota_group is None:
                    group_to_insert = {
                        "group_code": code_group,
                        "administrator_id": self.id_adm,
                    }
                    group_md_quota = self.pl_group.insert_new_group(group_to_insert)
                    group_id_md_quota = group_md_quota["group_id"]
                    self.groups_md_quota.append(group_md_quota)

                else:
                    group_id_md_quota = md_quota_group["group_id"]

                # sessão de inserção de dados de bens

                md_quota_assets_group = self.get_list_by_id(
                    group_id_md_quota, md_quota_assets, "group_id"
                )

                asset_type = 7

                assets_to_insert = []

                asset_desc = "Bem Porto"

                if row["available_credit_in_group_0"] is not None:
                    asset_to_insert_0 = {
                        "asset_desc": asset_desc,
                        "asset_code": build_asset_code(self.md_cota_connection),
                        "asset_value": row["available_credit_in_group_0"],
                        "asset_type_id": asset_type,
                        "info_date": row["data_info"],
                        "valid_from": row["data_info"],
                        "group_id": group_id_md_quota,
                    }

                    assets_to_insert.append(asset_to_insert_0)

                if row["available_credit_in_group_1"] is not None:
                    asset_to_insert_1 = {
                        "asset_desc": asset_desc,
                        "asset_code": build_asset_code(self.md_cota_connection),
                        "asset_value": row["available_credit_in_group_1"],
                        "asset_type_id": asset_type,
                        "info_date": row["data_info"],
                        "valid_from": row["data_info"],
                        "group_id": group_id_md_quota,
                    }

                    assets_to_insert.append(asset_to_insert_1)

                if row["available_credit_in_group_2"] is not None:
                    asset_to_insert_2 = {
                        "asset_desc": asset_desc,
                        "asset_code": build_asset_code(self.md_cota_connection),
                        "asset_value": row["available_credit_in_group_2"],
                        "asset_type_id": asset_type,
                        "info_date": row["data_info"],
                        "valid_from": row["data_info"],
                        "group_id": group_id_md_quota,
                    }

                    assets_to_insert.append(asset_to_insert_2)

                if row["available_credit_in_group_3"] is not None:
                    asset_to_insert_3 = {
                        "asset_desc": asset_desc,
                        "asset_code": build_asset_code(self.md_cota_connection),
                        "asset_value": row["available_credit_in_group_3"],
                        "asset_type_id": asset_type,
                        "info_date": row["data_info"],
                        "valid_from": row["data_info"],
                        "group_id": group_id_md_quota,
                    }

                    assets_to_insert.append(asset_to_insert_3)

                if row["available_credit_in_group_4"] is not None:
                    asset_to_insert_4 = {
                        "asset_desc": asset_desc,
                        "asset_code": build_asset_code(self.md_cota_connection),
                        "asset_value": row["available_credit_in_group_4"],
                        "asset_type_id": asset_type,
                        "info_date": row["data_info"],
                        "valid_from": row["data_info"],
                        "group_id": group_id_md_quota,
                    }

                    assets_to_insert.append(asset_to_insert_4)

                if row["available_credit_in_group_5"] is not None:
                    asset_to_insert_5 = {
                        "asset_desc": asset_desc,
                        "asset_code": build_asset_code(self.md_cota_connection),
                        "asset_value": row["available_credit_in_group_5"],
                        "asset_type_id": asset_type,
                        "info_date": row["data_info"],
                        "valid_from": row["data_info"],
                        "group_id": group_id_md_quota,
                    }

                    assets_to_insert.append(asset_to_insert_5)

                if row["available_credit_in_group_6"] is not None:
                    asset_to_insert_6 = {
                        "asset_desc": asset_desc,
                        "asset_code": build_asset_code(self.md_cota_connection),
                        "asset_value": row["available_credit_in_group_6"],
                        "asset_type_id": asset_type,
                        "info_date": row["data_info"],
                        "valid_from": row["data_info"],
                        "group_id": group_id_md_quota,
                    }

                    assets_to_insert.append(asset_to_insert_6)

                if row["available_credit_in_group_7"] is not None:
                    asset_to_insert_7 = {
                        "asset_desc": asset_desc,
                        "asset_code": build_asset_code(self.md_cota_connection),
                        "asset_value": row["available_credit_in_group_7"],
                        "asset_type_id": asset_type,
                        "info_date": row["data_info"],
                        "valid_from": row["data_info"],
                        "group_id": group_id_md_quota,
                    }

                    assets_to_insert.append(asset_to_insert_7)

                if row["available_credit_in_group_8"] is not None:
                    asset_to_insert_8 = {
                        "asset_desc": asset_desc,
                        "asset_code": build_asset_code(self.md_cota_connection),
                        "asset_value": row["available_credit_in_group_8"],
                        "asset_type_id": asset_type,
                        "info_date": row["data_info"],
                        "valid_from": row["data_info"],
                        "group_id": group_id_md_quota,
                    }

                    assets_to_insert.append(asset_to_insert_8)
                if row["available_credit_in_group_9"] is not None:
                    asset_to_insert_9 = {
                        "asset_desc": asset_desc,
                        "asset_code": build_asset_code(self.md_cota_connection),
                        "asset_value": row["available_credit_in_group_9"],
                        "asset_type_id": asset_type,
                        "info_date": row["data_info"],
                        "valid_from": row["data_info"],
                        "group_id": group_id_md_quota,
                    }

                    assets_to_insert.append(asset_to_insert_9)
                    new_asset_to_insert = False

                for new_asset in assets_to_insert:
                    for asset in md_quota_assets_group:
                        if asset["info_date"] < row["data_info"]:
                            self.pl_asset_repository.update_valid_to(asset["asset_id"])
                            md_quota_assets.remove(asset)
                            new_asset_to_insert = True

                    if new_asset_to_insert:
                        new_asset = self.pl_asset_repository.insert_new_asset(new_asset)
                        md_quota_assets.append(new_asset)

                # sessão de inserção de dados de lances

                bids_to_insert = []
                if row["pct_lances_contemplados_1_1"] is not None:
                    bid_to_insert_1 = {
                        "value": row["pct_lances_contemplados_1_1"],
                        "info_date": row["data_info"],
                        "group_id": group_id_md_quota,
                        "bid_type_id": 1,
                        "bid_value_type_id": 1,
                    }

                    bids_to_insert.append(bid_to_insert_1)

                if row["pct_lances_contemplados_1_2"] is not None:
                    bid_to_insert_2 = {
                        "value": row["pct_lances_contemplados_1_2"],
                        "info_date": row["data_info"],
                        "group_id": group_id_md_quota,
                        "bid_type_id": 1,
                        "bid_value_type_id": 1,
                    }

                    bids_to_insert.append(bid_to_insert_2)

                if row["pct_lances_contemplados_1_3"] is not None:
                    bid_to_insert_3 = {
                        "value": row["pct_lances_contemplados_1_3"],
                        "info_date": row["data_info"],
                        "group_id": group_id_md_quota,
                        "bid_type_id": 1,
                        "bid_value_type_id": 1,
                    }

                    bids_to_insert.append(bid_to_insert_3)

                if row["pct_lances_contemplados_1_4"] is not None:
                    bid_to_insert_4 = {
                        "value": row["pct_lances_contemplados_1_4"],
                        "info_date": row["data_info"],
                        "group_id": group_id_md_quota,
                        "bid_type_id": 1,
                        "bid_value_type_id": 1,
                    }

                    bids_to_insert.append(bid_to_insert_4)

                if row["pct_lances_contemplados_1_5"] is not None:
                    bid_to_insert_5 = {
                        "value": row["pct_lances_contemplados_1_5"],
                        "info_date": row["data_info"],
                        "group_id": group_id_md_quota,
                        "bid_type_id": 1,
                        "bid_value_type_id": 1,
                    }

                    bids_to_insert.append(bid_to_insert_5)

                if row["pct_lances_contemplados_1_6"] is not None:
                    bid_to_insert_6 = {
                        "value": row["pct_lances_contemplados_1_6"],
                        "info_date": row["data_info"],
                        "group_id": group_id_md_quota,
                        "bid_type_id": 1,
                        "bid_value_type_id": 1,
                    }

                    bids_to_insert.append(bid_to_insert_6)

                for new_bid in bids_to_insert:
                    self.pl_bid_repository.insert_new_bid(new_bid)

                # sessão de inserção de dados de vagas

                md_quota_group_vacancies_by_group = self.get_list_by_id(
                    group_id_md_quota, md_quota_group_vacancies, "group_id"
                )
                logger.info(f"criando objeto para inserir na tabela pl_group_vacancies")

                group_vacancies_to_insert = {
                    "vacancies": row["group_variances"],
                    "info_date": row["data_info"],
                    "group_id": group_id_md_quota,
                    "valid_from": row["data_info"],
                }

                logger.info(f"objeto criado")

                for group_vacancies in md_quota_group_vacancies_by_group:
                    if group_vacancies["info_date"] < row["data_info"]:
                        self.group_vacancies_repository.update_valid_to(
                            group_vacancies["group_vacancies_id"]
                        )
                        new_group_vacancies = (
                            self.group_vacancies_repository.insert_new_group_vacancies(
                                group_vacancies_to_insert
                            )
                        )
                        md_quota_group_vacancies.remove(group_vacancies)
                        md_quota_group_vacancies.append(new_group_vacancies)

                self.group_porto_repository.update_is_processed(row["id_grupos_porto"])


if __name__ == "__main__":
    etl = Etl()
    etl.start()
    # job.commit()
