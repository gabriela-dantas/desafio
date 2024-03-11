from datetime import datetime
from enum import Enum

from bazartools.common.database.assetCodeBuilder import build_asset_code
from bazartools.common.database.glueConnection import GlueConnection
from bazartools.common.logger import get_logger

GLUE_DEFAULT_CODE = 2
logger = get_logger()


class Constants(Enum):
    CASE_DEFAULT_TYPES = 5
    CASE_DEFAULT_ASSET_TYPES = 7
    CASE_DEFAULT_HISTORY_DETAIL_FIELD = 0
    QUOTA_ORIGIN_ADM = 1
    QUOTA_ORIGIN_CUSTOMER = 2


def get_list_by_id(id_item: str, data_list: list, field_name: str) -> list:
    items = []
    for item_list in data_list:
        if item_list[field_name] == id_item:
            items.append(item_list)
    return items


def asset_type_dict(key, value):
    asset = {"CAMIN": 3, "AUTO": 2, "IMOVEL": 1, "MOTO": 4, "SERVIC": 5}
    return asset.get(key, value)


def get_dict_by_id(id_item: str, data_list: list, field_name: str):
    for item_list in data_list:
        if item_list[field_name] == id_item:
            return item_list
    return None


def get_table_dict(cursor, rows):
    column_names = [desc[0] for desc in cursor.description]
    return [dict(zip(column_names, row)) for row in rows]


def string_right_justified(group: str) -> str:
    code_group = str(group) if len(str(group)) == 5 else str(group).rjust(5, "0")
    return code_group


def get_default_datetime() -> datetime:
    return datetime.now()


def read_adm_id(md_quota_select_cursor):
    try:
        logger.info("Buscando id da adm...")
        query_select_adm = """
            SELECT administrator_id
            FROM md_cota.pl_administrator
            WHERE administrator_code = '0000000131'
            AND is_deleted is false;
            """
        md_quota_select_cursor.execute(query_select_adm)
        query_result_adm = md_quota_select_cursor.fetchall()
        adm_dict = get_table_dict(md_quota_select_cursor, query_result_adm)
        logger.info("Busca do id da adm efetuada com sucesso.")
        return adm_dict[0]["administrator_id"]
    except Exception as error:
        logger.error(f"Error ao buscar id_adm no banco,error:{error}")
        raise error


def read_groups_pl_group(id_adm, md_quota_select_cursor):
    try:
        logger.info("Buscando informações de grupo na pl_group")
        query_groups_pl_group = f"""
            SELECT * 
            FROM md_cota.pl_group
            WHERE administrator_id = {id_adm} AND is_deleted is FALSE;
            """
        md_quota_select_cursor.execute(query_groups_pl_group)
        query_result_groups = md_quota_select_cursor.fetchall()
        logger.info("Informações de grupos recuperadas com sucesso.")
        return get_table_dict(md_quota_select_cursor, query_result_groups)
    except Exception as error:
        logger.error(f"Erro ao buscas informações de grupos, error:{error}")
        raise error


def gmac_read_data_stage_raw(md_quota_select_cursor):
    try:
        logger.info("Lendo dados do stage raw")
        stage_raw_groups_pre = """
        SELECT *
        FROM stage_raw.tb_grupos_gmac
        WHERE is_processed is FALSE
        """
        md_quota_select_cursor.execute(stage_raw_groups_pre)
        query_result = md_quota_select_cursor.fetchall()
        logger.info("Dados recuperados da tb_grupos_gmac")
        return get_table_dict(md_quota_select_cursor, query_result)
    except Exception as error:
        logger.error(
            f"Erro ao buscas informações adicionais na tb_grupos_gmac, error:{error}"
        )
        raise error


def read_assets(md_quota_select_cursor):
    try:
        assets_md_quota = """
        SELECT *
        FROM md_cota.pl_asset
        WHERE is_deleted is FALSE
        AND
        valid_to is null
        """
        md_quota_select_cursor.execute(assets_md_quota)
        query_result = md_quota_select_cursor.fetchall()
        logger.info("Dados recuperados da pl_asset")
        return get_table_dict(md_quota_select_cursor, query_result)
    except Exception as error:
        logger.error(
            f"Erro ao buscas informações adicionais na pl_asset, error:{error}"
        )
        raise error


def pl_group_insert_new_group(md_quota_insert_cursor, group_to_insert):
    try:
        logger.info("Inserindo dados na pl_group")
        group_pl_group = """
        INSERT INTO md_cota.pl_group
        (
            group_code,
            administrator_id,
            created_at,
            modified_at,
            created_by,
            modified_by,
            is_deleted
        )
        VALUES(%s, %s, %s, %s, %s, %s, %s)
        RETURNING *;
        """
        params = (
            group_to_insert["group_code"],
            group_to_insert["administrator_id"],
            get_default_datetime(),
            get_default_datetime(),
            GLUE_DEFAULT_CODE,
            GLUE_DEFAULT_CODE,
            False,
        )
        md_quota_insert_cursor.execute(group_pl_group, params)
        query_result_client = md_quota_insert_cursor.fetchall()
        logger.info("Dados inseridos na pl_group")
        return get_table_dict(md_quota_insert_cursor, query_result_client)[0]
    except Exception as error:
        logger.error(f"Erro ao inserir dados na pl_group. Error:{error}")
        raise error


def pl_asset_insert_new_asset(md_quota_insert_cursor, asset_data):
    try:
        logger.info("Inserindo dado na pl_asset")
        new_asset = """
            INSERT INTO md_cota.pl_asset (
                asset_desc,
                asset_code,
                asset_adm_code,
                asset_value,
                asset_type_id,
                info_date,
                valid_from,
                group_id,
                created_at,
                modified_at,
                created_by,
                modified_by,
                is_deleted
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            RETURNING *;
        """

        params = (
            asset_data["asset_desc"],
            asset_data["asset_code"],
            asset_data["asset_adm_code"],
            asset_data["asset_value"],
            asset_data["asset_type_id"],
            asset_data["info_date"],
            get_default_datetime(),
            asset_data["group_id"],
            get_default_datetime(),
            get_default_datetime(),
            GLUE_DEFAULT_CODE,
            GLUE_DEFAULT_CODE,
            False,
        )
        md_quota_insert_cursor.execute(new_asset, params)
        query_result_client = md_quota_insert_cursor.fetchall()
        logger.info("Dados Inserindos na pl_asset")
        return get_table_dict(md_quota_insert_cursor, query_result_client)[0][
            "asset_id"
        ]
    except Exception as error:
        logger.error(f"Erro ao inserir dados na pl_asset. Error: {error}")
        raise error


def update_valid_to(md_quota_update_cursor, asset_id):
    try:
        logger.info("Fazendo update na pl_asset")
        query_update_valid_to = f"""
            UPDATE md_cota.pl_asset
            SET asset_id = {asset_id},
                valid_to = {get_default_datetime()},
            """
        md_quota_update_cursor.execute(query_update_valid_to)
        logger.info("Update efetudado na pl_asset")
    except Exception as error:
        logger.error(f"Erro ao atualizar informações na pl_asset, error:{error}")
        raise error


def update_is_processed(md_quota_update_cursor, id_group_gmac):
    try:
        groups_gmac_processed = f"""
        UPDATE stage_raw.tb_grupos_gmac
        SET is_processed = {True}
        WHERE id_grupo_gmac = {id_group_gmac}
        """
        md_quota_update_cursor.execute(groups_gmac_processed)
        logger.info("Update na tb_grupos_gmac")
    except Exception as error:
        logger.error(f"Erro ao atualizar informações na tb_grupos_gmac, error:{error}")
        raise error


def process_row(
    row,
    groups_md_quota,
    id_adm,
    md_quota_insert_cursor,
    md_quota_assets,
    md_quota_update_cursor,
    md_quota_connection,
):
    code_group = process_code_group(row)
    logger.info(f"code_group: {code_group}")

    md_quota_group = get_dict_by_id(code_group, groups_md_quota, "group_code")
    logger.info(f"md_quota_group: {md_quota_group}")

    group_id_md_quota = (
        md_quota_group["group_id"]
        if md_quota_group
        else insert_new_group(
            md_quota_insert_cursor, code_group, id_adm, groups_md_quota
        )
    )

    process_assets(
        row,
        md_quota_insert_cursor,
        md_quota_update_cursor,
        group_id_md_quota,
        md_quota_assets,
        md_quota_connection,
    )


def process_code_group(row):
    return (
        row["codigo_grupo"][-5:]
        if len(row["codigo_grupo"]) > 5
        else string_right_justified(row["codigo_grupo"])
    )


def insert_new_group(md_quota_insert_cursor, code_group, id_adm, groups_md_quota):
    group_to_insert = {
        "group_code": code_group,
        "administrator_id": id_adm,
    }
    group_md_quota = pl_group_insert_new_group(md_quota_insert_cursor, group_to_insert)
    group_id_md_quota = group_md_quota["group_id"]
    groups_md_quota.append(group_md_quota)
    return group_id_md_quota


def process_assets(
    row,
    md_quota_insert_cursor,
    md_quota_update_cursor,
    group_id_md_quota,
    md_quota_assets,
    md_quota_connection,
):
    asset_type = 2
    asset_code = build_asset_code(md_quota_connection)
    asset_to_insert = {
        "asset_desc": row["descricao"],
        "asset_code": asset_code,
        "asset_adm_code": row["codigo_bem"],
        "asset_value": row["valor_bem"],
        "asset_type_id": asset_type,
        "info_date": row["data_info"],
        "valid_from": row["data_info"],
        "group_id": group_id_md_quota,
    }

    new_asset_id = insert_new_asset(md_quota_insert_cursor, asset_to_insert)

    update_assets_valid_to(row, md_quota_assets, md_quota_update_cursor, new_asset_id)


def insert_new_asset(md_quota_insert_cursor, asset_to_insert):
    return pl_asset_insert_new_asset(md_quota_insert_cursor, asset_to_insert)


def update_assets_valid_to(row, md_quota_assets, md_quota_update_cursor, new_asset_id):
    md_quota_assets_group = get_list_by_id(new_asset_id, md_quota_assets, "group_id")

    for asset in md_quota_assets_group:
        if asset["info_date"] < row["data_info"]:
            update_valid_to(md_quota_update_cursor, asset["asset_id"])

        if asset["info_date"] > row["data_info"]:
            update_valid_to(md_quota_update_cursor, new_asset_id)

    update_is_processed(md_quota_update_cursor, row["id_grupo_gmac"])


def process_all(
    groups_md_quota,
    id_adm,
    md_quota_insert_cursor,
    md_quota_assets,
    md_quota_update_cursor,
    assets_gmac,
    md_quota_connection,
):
    md_quota_assets = md_quota_assets
    for row in assets_gmac:
        logger.info(f"Assets md-cota")
        process_row(
            row,
            groups_md_quota,
            id_adm,
            md_quota_insert_cursor,
            md_quota_assets,
            md_quota_update_cursor,
            md_quota_connection,
        )


def from_file_to_model_info_groups():
    md_quota_connection_factory = GlueConnection(connection_name="md-cota")
    md_quota_connection = md_quota_connection_factory.get_connection()
    md_quota_insert_cursor = md_quota_connection.cursor()
    md_quota_select_cursor = md_quota_connection.cursor()
    md_quota_update_cursor = md_quota_connection.cursor()

    try:
        id_adm = read_adm_id(md_quota_select_cursor)
        groups_md_quota = read_groups_pl_group(id_adm, md_quota_select_cursor)
        asset_gmac = gmac_read_data_stage_raw(md_quota_select_cursor)
        md_quota_assets = read_assets(md_quota_select_cursor)

        process_all(
            groups_md_quota,
            id_adm,
            md_quota_insert_cursor,
            md_quota_assets,
            md_quota_update_cursor,
            asset_gmac,
            md_quota_connection,
        )
        md_quota_connection.commit()
        logger.info("Dados processado com sucesso.")

    except Exception as error:
        logger.error(
            f"Erro ao processar dados.Rollback da operação efetuado devido ao Error:{error}"
        )
        md_quota_connection.rollback()
        raise error


if __name__ == "__main__":
    from_file_to_model_info_groups()
