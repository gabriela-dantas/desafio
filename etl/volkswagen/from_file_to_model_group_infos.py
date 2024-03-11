# imports do glue

from awsglue.utils import getResolvedOptions
from bazartools.common.database.assetCodeBuilder import build_asset_code
from bazartools.common.database.glueConnection import GlueConnection

from datetime import datetime
from enum import Enum
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
        self, name: str = "etl-group-infos", level: Literal[20] = logging.INFO
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


class PlVacancies(Base, BasicFields):
    __tablename__ = "pl_group_vacancies"
    group_vacancies_id = Column(BigInteger, primary_key=True, index=True)
    vacancies = Column(Integer, nullable=False)
    info_date = Column(DateTime, nullable=False)
    group_id = Column(Integer, ForeignKey(PlGroup.group_id))
    valid_from = Column(DateTime)
    valid_to = Column(DateTime)


class BidsVolksPreModel(Base):
    __tablename__ = "tb_lances_volks_pre"

    id_lances_volks = Column(BigInteger, primary_key=True, index=True)
    atsf_stgrupo = Column(String(10), nullable=False)
    pkni_grupo = Column(String(10), nullable=False)
    pkni_cota = Column(String(10), nullable=False)
    pkni_subst = Column(String(10), nullable=False)
    pkni_digcontr = Column(String(10), nullable=False)
    pkni_assembleia = Column(String(10), nullable=False)
    atnd_lancebruto = Column(Float, nullable=False)
    atnd_lanceliqdo = Column(Float, nullable=False)
    atnd_percamortz = Column(Float, nullable=False)
    atsf_origem = Column(String(10), nullable=True)
    atdt_captacao = Column(DateTime, nullable=False)
    atni_horacapta = Column(String(10), nullable=False)
    atsf_stlcvenc = Column(String(10), nullable=False)
    is_processed = Column(Boolean)
    created_at = Column(DateTime)
    data_info = Column(DateTime)


class AssetsVolksPreModel(Base):
    __tablename__ = "tb_bens_volks_pre"

    id_bens_volks = Column(BigInteger, primary_key=True, index=True)
    atsf_stgrupo = Column(String(10), nullable=False)
    pkni_grupo = Column(String(10), nullable=False)
    pkni_plano = Column(String(10), nullable=False)
    fkni_codbem = Column(String(10), nullable=False)
    atnd_taxafr = Column(Float, nullable=False)
    atnd_taxadm = Column(Float, nullable=False)
    valor_do_bem = Column(Float, nullable=False)
    valor_da_categoria = Column(Float, nullable=False)
    atsv_descrbem = Column(String(255), nullable=False)
    atdt_descont = Column(String(10), nullable=True)
    is_processed = Column(Boolean)
    created_at = Column(DateTime)
    data_info = Column(DateTime)


class GroupsVolksPreModel(Base):
    __tablename__ = "tb_grupos_volks_pre"

    id_grupos_volks = Column(BigInteger, primary_key=True, index=True)
    grupo = Column(String(10), nullable=False)
    status = Column(String(20), nullable=True)
    dt_formacao = Column(DateTime, nullable=True)
    serie = Column(String(10), nullable=True)
    primeira_ass = Column(DateTime, nullable=False)
    prazo = Column(Integer, nullable=False)
    ult_ass = Column(DateTime, nullable=False)
    participantes = Column(Integer, nullable=True)
    nro_ass_atual = Column(Integer, nullable=False)
    data_ass_atual = Column(DateTime, nullable=True)
    contemplados = Column(Integer, nullable=False)
    a_contemplar = Column(Integer, nullable=False)
    vagas = Column(Integer, nullable=False)
    cancelados = Column(Integer, nullable=False)
    desistentes = Column(Integer, nullable=False)
    inativos = Column(Integer, nullable=False)
    inadimplentes = Column(Integer, nullable=False)
    data_encerramento = Column(DateTime, nullable=True)
    is_processed = Column(Boolean)
    created_at = Column(DateTime)
    data_info = Column(DateTime)


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


class AssetsVolksPreRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.STAGE_RAW.value)

    def read_data_stage_raw(self) -> list:
        try:
            stage_raw_assets_volks_pre = (
                self._session.query(AssetsVolksPreModel)
                .filter_by(is_processed=False)
                .all()
            )
            assets_volks_pre = []
            for data in stage_raw_assets_volks_pre:
                assets_volks_pre.append(data.__dict__)
            logger.info("Read data AssetsVolksPre success!")
            return assets_volks_pre
        except DatabaseError as error:
            logger.error("Error when search data in AssetsVolksPre:{}".format(error))

    def update_is_processed(self, id_bens_volks: int) -> None:
        try:
            assets_volks_processed = (
                self._session.query(AssetsVolksPreModel)
                .filter_by(id_bens_volks=id_bens_volks)
                .first()
            )
            assets_volks_processed.is_processed = True
            self._session.add(assets_volks_processed)
            self._session.commit()
            logger.info("Update AssetsVolksPre is_processed=true!")
        except OperationalError as error:
            logger.error(
                "Error when search/update data in AssetsVolksPre:{}".format(error)
            )


class BidsVolksPreRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.STAGE_RAW.value)

    def read_data_stage_raw(self) -> list:
        try:
            stage_raw_bids_volks_pre = (
                self._session.query(BidsVolksPreModel)
                .filter_by(is_processed=False)
                .all()
            )
            bids_volks_pre = []
            for data in stage_raw_bids_volks_pre:
                bids_volks_pre.append(data.__dict__)
            logger.info("Read data BidsVolksPreModel success!")
            return bids_volks_pre
        except DatabaseError as error:
            logger.error("Error when search data in BidsVolksPreModel:{}".format(error))

    def update_is_processed(self, id_lances_volks: int) -> None:
        try:
            bids_volks_processed = (
                self._session.query(BidsVolksPreModel)
                .filter_by(id_lances_volks=id_lances_volks)
                .first()
            )
            bids_volks_processed.is_processed = True
            self._session.add(bids_volks_processed)
            self._session.commit()
            logger.info("Update BidsVolksPre is_processed=true!")
        except OperationalError as error:
            logger.error(
                "Error when search/update data in BidsVolksPre:{}".format(error)
            )


class GroupsVolksPreRepository(ConnectionAbstractRepository):
    def __init__(self) -> None:
        super().__init__(TypeSchemaEnum.STAGE_RAW.value)

    def read_data_stage_raw(self) -> list:
        try:
            stage_raw_groups_volks_pre = (
                self._session.query(GroupsVolksPreModel)
                .filter_by(is_processed=False)
                .all()
            )
            groups_volks_pre = []
            for data in stage_raw_groups_volks_pre:
                groups_volks_pre.append(data.__dict__)
            logger.info("Read data GroupsVolksPreModel success!")
            return groups_volks_pre
        except DatabaseError as error:
            logger.error(
                "Error when search data in GroupsVolksPreModel:{}".format(error)
            )

    def update_is_processed(self, id_grupos_volks: int) -> None:
        try:
            groups_volks_processed = (
                self._session.query(GroupsVolksPreModel)
                .filter_by(id_grupos_volks=id_grupos_volks)
                .first()
            )
            groups_volks_processed.is_processed = True
            self._session.add(groups_volks_processed)
            self._session.commit()
            logger.info("Update GroupsVolksPreModel is_processed=true!")
        except OperationalError as error:
            logger.error(
                "Error when search/update data in GroupsVolksPreRepository:{}".format(
                    error
                )
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
                assembly_date=bid_data["assembly_date"],
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
            return new_asset.asset_id

        except DatabaseError as error:
            logger.error("Error: Asset not inserted in PlAsset:{}".format(error))

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
            logger.info("Vacancies inserted with success in PlGroupVacancies!")
            return new_group_vacancies.group_vacancies_id

        except DatabaseError as error:
            logger.error(
                "Error: Vacancies not inserted in PlGroupVacancies:{}".format(error)
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
        ADM_NAME = "CONS. NACIONAL VOLKSWAGEN LTDA."

    class AssetTypeEnum(Enum):
        VEICULOS_PESADOS = 3
        VEICULOS_LEVES = 2
        IMOVEIS = 1
        MOTOCICLETAS = 4

    switch_asset_type = {
        "VEÍCULOS PESADOS": AssetTypeEnum.VEICULOS_PESADOS.value,
        "VEÍCULOS LEVES": AssetTypeEnum.VEICULOS_LEVES.value,
        "IMÓVEIS": AssetTypeEnum.IMOVEIS.value,
        "MOTOCICLETAS": AssetTypeEnum.MOTOCICLETAS.value,
    }

    def __init__(self):
        self.pl_administrator = PlAdministratorRepository()
        self.pl_group = PlGroupRepository()
        self.asset_repository = AssetsVolksPreRepository()
        self.bid_repository = BidsVolksPreRepository()
        self.pl_bid_repository = PlBidRepository()
        self.pl_asset_repository = PlAssetRepository()
        self.vacancies_repository = PlGroupVacanciesRepository()
        self.group_repository = GroupsVolksPreRepository()
        self.md_cota_connection_factory = GlueConnection(connection_name="md-cota")

        self.md_cota_connection = self.md_cota_connection_factory.get_connection()
        self.id_adm = self.pl_administrator.read_adm(self.EtlInfo.ADM_NAME.value)
        self.groups_md_quota = self.pl_group.read_groups(self.id_adm)
        self.assets_volks = self.asset_repository.read_data_stage_raw()
        self.bids_volks = self.bid_repository.read_data_stage_raw()
        self.md_quota_assets = self.pl_asset_repository.read_assets()
        self.group_vacancies_md_quota = self.vacancies_repository.read_group_vacancies()
        self.groups_stage = self.group_repository.read_data_stage_raw()

    def start(self):
        self.assets_volks_flow()
        self.bids_volks_flow()
        self.vacancies_volks_flow()

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

    def assets_volks_flow(self):
        md_quota_assets = self.md_quota_assets
        for row in self.assets_volks:
            logger.info(f"Assets md-cota {md_quota_assets}")

            if len(row["pkni_grupo"]) > 5:
                code_group = row["pkni_grupo"][-5:]

            else:
                code_group = self.string_right_justified(row["pkni_grupo"])

            md_quota_group = self.get_dict_by_id(
                code_group, self.groups_md_quota, "group_code"
            )

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

            md_quota_assets_group = self.get_list_by_id(
                group_id_md_quota, md_quota_assets, "group_id"
            )

            asset_code = build_asset_code(self.md_cota_connection)

            asset_to_insert = {
                "asset_desc": row["atsv_descrbem"],
                "asset_code": asset_code,
                "asset_value": row["valor_do_bem"],
                "asset_type_id": 2,
                "info_date": row["data_info"],
                "valid_from": row["data_info"],
                "group_id": group_id_md_quota,
            }

            new_asset_to_insert = False

            for asset in md_quota_assets_group:
                if asset["info_date"] < row["data_info"]:
                    self.pl_asset_repository.update_valid_to(asset["asset_id"])
                    new_asset_to_insert = True

            if new_asset_to_insert:
                new_asset_id = self.pl_asset_repository.insert_new_asset(
                    asset_to_insert
                )

            self.asset_repository.update_is_processed(row["id_bens_volks"])

    def bids_volks_flow(self):
        for row in self.bids_volks:
            if len(row["pkni_grupo"]) > 5:
                code_group = row["pkni_grupo"][-5:]

            else:
                code_group = self.string_right_justified(row["pkni_grupo"])

            md_quota_group = self.get_dict_by_id(
                code_group, self.groups_md_quota, "group_code"
            )

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

            bid_to_insert = {
                "value": row["atnd_lancebruto"],
                "assembly_date": row["atdt_captacao"],
                "info_date": row["data_info"],
                "group_id": group_id_md_quota,
                "bid_type_id": 1,
                "bid_value_type_id": 1,
            }

            new_bid = self.pl_bid_repository.insert_new_bid(bid_to_insert)

            self.bid_repository.update_is_processed(row["id_lances_volks"])

    def vacancies_volks_flow(self):
        md_quota_group_vacancies = self.group_vacancies_md_quota
        for row in self.groups_stage:
            logger.info("iniciando processamento de vagas de grupo...")

            if len(row["grupo"]) > 5:
                code_group = row["grupo"][-5:]

            else:
                code_group = self.string_right_justified(row["grupo"])

            logger.info(f"code_group: {code_group}")

            md_quota_group = self.get_dict_by_id(
                code_group, self.groups_md_quota, "group_code"
            )
            logger.info(f"md_quota_group: {md_quota_group}")

            logger.info("group_code formado...")

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

            logger.info(f"group_id: {group_id_md_quota}")

            md_quota_group_vacancies_by_group = self.get_list_by_id(
                group_id_md_quota, md_quota_group_vacancies, "group_id"
            )
            logger.info(f"criando objeto para inserir na tabela")

            group_vacancies_to_insert = {
                "vacancies": row["vagas"],
                "info_date": row["data_info"],
                "group_id": group_id_md_quota,
                "valid_from": row["data_info"],
            }

            logger.info(f"objeto criado")
            insert_new_vacancy = False
            for group_vacancies in md_quota_group_vacancies_by_group:
                if group_vacancies["info_date"] < row["data_info"]:
                    self.vacancies_repository.update_valid_to(
                        group_vacancies["group_vacancies_id"]
                    )
                    insert_new_vacancy = True

            if insert_new_vacancy:
                new_group_vacancies_id = (
                    self.vacancies_repository.insert_new_group_vacancies(
                        group_vacancies_to_insert
                    )
                )

            self.group_repository.update_is_processed(row["id_grupos_volks"])


if __name__ == "__main__":
    etl = Etl()
    etl.start()
    # job.commit()
