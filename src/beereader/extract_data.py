import pandas as pd
import numpy as np
import os
import json

from typing import Dict, Any, Tuple

from simple_common.logger import logger
from common.repositories.beereader import BeeReaderRepository
from beereader.file_handler import FileHandler
from beereader.data_type_treatment import DataTypeTreatment
from common.event_schemas.extract_event_schema import ExtractEventSchema
from common.exceptions import UnprocessableEntity


class ExtractData:
    def __init__(self) -> None:
        self.__logger = logger
        self.__file_handler = FileHandler()
        self.__data_type_treatment = DataTypeTreatment()
        self.__beereader_repository = BeeReaderRepository()

        self.__structure_path = ""
        self.__df_types: pd.DataFrame
        self.__df_types_script: pd.DataFrame
        self.__df_types_keys: pd.DataFrame
        self.__df_fields: pd.DataFrame
        self.__df_fields_script: pd.DataFrame
        self.__df_fixed_fields: pd.DataFrame

        self.__extraction_result: Dict[str, Any] = {}

        self.__set_structure_file_path()
        self.__set_structure_dfs()

    def __set_structure_file_path(self) -> None:
        file_path = os.path.join(os.path.realpath(__file__), *["..", "structure3.xlsx"])
        self.__structure_path = os.path.normpath(file_path)

    def __set_structure_dfs(self) -> None:
        self.__logger.debug(
            f"Iniciada leitura de planilhas auxiliares "
            f"contidas em {self.__structure_path}..."
        )

        self.__df_types = pd.read_excel(self.__structure_path, sheet_name="tipos")
        self.__df_types_script = pd.read_excel(
            self.__structure_path, sheet_name="tipos_script"
        )
        self.__df_types_keys = pd.read_excel(
            self.__structure_path, sheet_name="tipos_keys"
        )
        self.__df_fields = pd.read_excel(self.__structure_path, sheet_name="campos")
        self.__df_fields_script = pd.read_excel(
            self.__structure_path, sheet_name="campos_script"
        )
        self.__df_fixed_fields = pd.read_excel(
            self.__structure_path, sheet_name="campos_fixos"
        )

        self.__logger.debug(f"Finalizada leitura de {self.__structure_path}...")

    def __detect_extract_type(self, file_text: str) -> str:
        self.__logger.debug("Iniciada detecção de tipo do extrato...")

        for index, row in self.__df_types.iterrows():
            extract_type = row["tipo"]
            start_pos = -1
            end_pos = -1

            for index_2, row_2 in self.__df_types_script[
                self.__df_types_script.tipo == extract_type
            ].iterrows():
                if row_2["posicao"] == "comeco":
                    start_pos = file_text.find(row_2["key"], start_pos + 1)
                    end_pos = start_pos
                elif row_2["posicao"] == "fim":
                    end_pos = file_text.find(row_2["key"], end_pos + 1)

            target_text = file_text[start_pos:end_pos]
            found_positions = []

            for index_3, row_3 in self.__df_types_keys[
                self.__df_types_keys.tipo == extract_type
            ].iterrows():
                position = target_text.find(str(row_3["key"]))
                found_positions.append(position)

            if -1 not in found_positions and len(found_positions) != 0:
                self.__logger.debug(f"Tipo de extrato detectado: {extract_type}")
                return extract_type

        message = "Extrato informadp possui formato não mapeado no Beereader."
        self.__logger.error(message)
        raise UnprocessableEntity(detail=message)

    def __collect_fixed_fields(self) -> None:
        self.__logger.debug("Iniciando coleta de campos fixos...")

        for index, row in self.__df_fixed_fields[
            self.__df_fixed_fields.tipo == self.__extraction_result["tipo"]
        ].iterrows():
            collected = {row["campo"]: row["valor"]}
            self.__logger.debug(collected)
            self.__extraction_result.update(**collected)

        self.__logger.debug("Finalizada coleta de campos fixos.")

    def __get_filed_position(self, text: str, field_name: str) -> Tuple[bool, int, int]:
        start = -1
        end = -1
        error = False

        for index, row in self.__df_fields_script[
            (self.__df_fields_script.tipo == self.__extraction_result["tipo"])
            & (self.__df_fields_script.campo == field_name)
        ].iterrows():
            if row["posicao"] == "comeco":
                if text.find(str(row["key"]), start + 1) == -1:
                    error = True

                start = text.find(str(row["key"]), start + 1) + int(row["add_comeco"])
                end = start + int(row["add_fim"])
            elif row["posicao"] == "fim":
                if text.find(str(row["key"]), end + 1) == -1:
                    error = True

                end = text.find(str(row["key"]), end + 1) + int(row["add_fim"])
                start += int(row["add_comeco"])

        return error, start, end

    def __collect_specific_fields(self, text: str) -> None:
        self.__logger.debug("Iniciando coleta de campos específicos...")

        for index, row in self.__df_fields[
            self.__df_fields.tipo == self.__extraction_result["tipo"]
        ].iterrows():
            self.__logger.debug(
                f"Obtido tipo do campo: {row['campo']} - {row['dtype']}"
            )

            eliminate = row["eliminate"] if row["eliminate"] is not np.nan else ""
            error, start, end = self.__get_filed_position(text, row["campo"])

            if not error:
                placeholder = text[start:end]
                self.__logger.debug(
                    f"Obtido valor do campo: {row['campo']} - {placeholder}"
                )

                placeholder = self.__data_type_treatment.fix_data_type(
                    placeholder, row["dtype"], eliminate
                )
                self.__logger.debug(
                    f"Feito tratamento do campo: {row['campo']} - {placeholder}"
                )

                self.__extraction_result[row["campo"]] = placeholder
            else:
                self.__logger.debug(f"Valor do cmapo não encontrado: {row['campo']}")

        self.__logger.debug("Finalizada coleta de campos específicos.")

    def __extract_to_json(self, file_text: str) -> None:
        self.__logger.debug(
            "Iniciando operações com Dataframes para coleta de dados do extrato..."
        )

        file_text = file_text.lower()

        self.__extraction_result["tipo"] = self.__detect_extract_type(file_text)
        self.__collect_fixed_fields()
        self.__collect_specific_fields(file_text)

        self.__logger.debug("Dados do extrato coletados com sucesso.")

    def __save_result(self, extract_event: ExtractEventSchema) -> None:
        self.__logger.debug("A salvar dados coletados no banco...")

        self.__extraction_result["person_code"] = extract_event.person_code
        quota_data = json.dumps(
            self.__extraction_result, default=str, ensure_ascii=False
        )
        adm = str(self.__extraction_result["tipo"]).upper()

        beereader_entity = {
            "file_name": extract_event.extract_filename,
            "bpm_quota_id": extract_event.quota_id,
            "adm": adm,
            "quota_data": quota_data,
            "s3_path": extract_event.extract_s3_path,
            "attachment_date": extract_event.extract_created_at,
        }

        self.__beereader_repository.create(beereader_entity, True)
        self.__logger.debug("Dados do extrato salvos com sucesso.")

    def extract_and_save(self, extract_event: ExtractEventSchema) -> Dict[str, int]:
        file_text = self.__file_handler.get_file_text(extract_event.extract_url)
        self.__extract_to_json(file_text)
        self.__save_result(extract_event)

        return {"quota_id": extract_event.quota_id}
