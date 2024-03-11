from typing import Callable, Dict, Union
from datetime import datetime

from simple_common.logger import logger


data_type_function = Callable[[str], Union[str, int, float]]


class DataTypeTreatment:
    contemplation_types = ["lance", "sorteio"]
    NO_CONTEMPLATION = "na"

    def __init__(self) -> None:
        self.__logger = logger

        self.__function_by_type: Dict[str, data_type_function] = {
            "string": self.__treat_string,
            "string_lr_strip": self.__treat_string_lr_strip,
            "string_without_zeroes": self.__treat_string_without_zeroes,
            "int": self.__treat_int,
            "float_perc_4dc": self.__treat_float_perc_4dc,
            "float_2dc": self.__treat_float_2dc,
            "data_ddmmaaaa": self.__treat_data_ddmmaaaa,
            "data_ddmmaa": self.__treat_data_ddmmaa,
            "contemplacao_fix_1": self.__treat_contemplacao_fix_1,
        }

    @classmethod
    def __treat_string(cls, text: str) -> str:
        return text

    @classmethod
    def __treat_string_lr_strip(cls, text: str) -> str:
        return text.strip(" ")

    @classmethod
    def __treat_string_without_zeroes(cls, text: str) -> str:
        return str(int(text))

    @classmethod
    def __treat_int(cls, text: str) -> int:
        return int(text)

    @classmethod
    def __treat_float_perc_4dc(cls, text: str) -> float:
        text = text.replace(".", "").replace(",", "")
        return float(text) / 1000000

    @classmethod
    def __treat_float_2dc(cls, text: str) -> float:
        text = text.replace(".", "").replace(",", "")
        return float(text) / 100

    @classmethod
    def __normalize_date(cls, text: str, date_format: str) -> str:
        text = text.replace(".", "/").replace(" ", "")
        return datetime.strptime(text, date_format).strftime("%Y-%m-%d")

    @classmethod
    def __treat_data_ddmmaaaa(cls, text: str) -> str:
        return cls.__normalize_date(text, "%d/%m/%Y")

    @classmethod
    def __treat_data_ddmmaa(cls, text: str) -> str:
        return cls.__normalize_date(text, "%d/%m/%y")

    @classmethod
    def __treat_contemplacao_fix_1(cls, text: str) -> str:
        for contemplation_type in cls.contemplation_types:
            if text.find(contemplation_type) != -1:
                return contemplation_type

        return cls.NO_CONTEMPLATION

    def fix_data_type(
        self, text: str, data_type: str, eliminate: str
    ) -> Union[str, int, float]:
        for i in eliminate:
            text = text.replace(i, "")

        text = text.replace("%", "%%")

        try:
            treat_function = self.__function_by_type[data_type]
        except KeyError:
            self.__logger.error(
                f"Não foi encontrado tratamento para tipo de dado {data_type}"
            )
            return ""

        try:
            return treat_function(text)
        except ValueError:
            self.__logger.error(
                f"Falha ao tentar tratar tipo de dado {data_type} com valor {text}"
            )
            return ""
