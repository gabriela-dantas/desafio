from datetime import datetime

from sqlalchemy.orm import declarative_base
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.elements import BooleanClauseList, BinaryExpression
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.query import Query
from sqlalchemy.dialects import postgresql
from sqlalchemy import Column
from typing import List, Union, Dict, Any, Type
from common.database.session import db_session
from common.exceptions import EntityNotFound, InternalServerError, Conflict
from simple_common.logger import logger
from common.models.md_quota.md_quota_base import MDQuotaBaseModel


class AbstractRepository:
    def __init__(self, model: Type[declarative_base]) -> None:
        self._model = model
        self._session: Session = db_session
        self._logger = logger

    @classmethod
    def _get_raw_query(cls, query: Query) -> str:
        return str(
            query.statement.compile(
                dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
            )
        )

    def get_model_name(self) -> str:
        return self._model.__tablename__

    def get_model_columns(self) -> Column:
        return self._model.__table__.columns

    def __set_delete_filter(
        self,
        filters: Union[BooleanClauseList, BinaryExpression],
    ) -> Union[BooleanClauseList, BinaryExpression]:
        if issubclass(self._model, MDQuotaBaseModel):
            filters &= self._model.is_deleted.is_(False)
            return filters

    def find_one(
        self, filters: Union[BooleanClauseList, BinaryExpression]
    ) -> declarative_base:
        filters = self.__set_delete_filter(filters)
        query = self._session.query(self._model).filter(filters)

        self._logger.debug(f"A executar query: {self._get_raw_query(query)}")

        item = query.first()

        if item is None:
            self._logger.debug(
                f"Query não recuperou nenhum item "
                f"da entidade {self.get_model_name()}."
            )
            raise EntityNotFound(
                detail="nenhum item encontrado ao realizar query na entidade."
            )

        self._logger.debug(
            f"Query recuperou um item com sucesso, "
            f"da entidade {self.get_model_name()}."
        )

        return item

    def find_many(
        self,
        filters: Union[BooleanClauseList, BinaryExpression],
        offset: int = 0,
        limit: int = 100,
    ) -> List[declarative_base]:
        self.__set_delete_filter(filters)
        query = (
            self._session.query(self._model).filter(filters).offset(offset).limit(limit)
        )

        self._logger.debug(f"A executar query: {self._get_raw_query(query)}")

        items = query.all()

        self._logger.debug(
            f"Query recuperou {len(items)} items, "
            f"da entidade {self.get_model_name()}."
        )

        return items

    def flush_and_commit(
        self, new_entity: declarative_base = None, commit_at_the_end: bool = True
    ) -> None:  # pragma: no cover
        try:
            if new_entity is not None:
                self._session.add(new_entity)

            self._session.flush()
        except IntegrityError as exception1:
            self._session.rollback()
            conflict = Conflict(
                detail="Erro de integridade ao tentar criar/atualizar a entidade.",
                headers={
                    "entity": self.get_model_name(),
                    "integrity_error": str(exception1.detail),
                },
            )

            self._logger.exception(f"{conflict.detail}\n{conflict.headers}")
            raise conflict
        except Exception as exception2:
            self._session.rollback()
            internal_error = InternalServerError(
                detail="Comportamento inesperado ao tentar criar/atualizar a entidade.",
                headers={
                    "entity": self.get_model_name(),
                    "generic_error": str(exception2),
                },
            )

            self._logger.exception(f"{internal_error.detail}\n{internal_error.headers}")
            raise internal_error

        if commit_at_the_end:
            self._session.commit()

    def create(
        self, attributes: Dict[str, Any], commit_at_the_end: bool = True
    ) -> declarative_base:
        new_entity = self._model(**attributes)

        self._logger.debug(
            f"A executar Inserção da entidade "
            f"{self.get_model_name()}, com atributos: {attributes}"
        )

        self.flush_and_commit(new_entity, commit_at_the_end)

        self._logger.debug(f"Entidade {self.get_model_name()} inserida com sucesso.")

        return new_entity

    def create_many(
        self, entities: List[dict], commit_at_the_end: bool = True
    ) -> Dict[str, str]:
        try:
            self._logger.debug(
                f"A executar inserção em batch de {len(entities)} entidades "
                f"{self.get_model_name()}."
            )
            self._session.bulk_insert_mappings(self._model, entities)
        except IntegrityError as exception1:
            self._session.rollback()
            conflict = Conflict(
                detail="Erro de integridade ao tentar inserir volume de entidades.",
                headers={
                    "entity": self.get_model_name(),
                    "integrity_error": str(exception1.detail),
                },
            )

            self._logger.exception(f"{conflict.detail}\n{conflict.headers}")
            raise conflict
        except Exception as exception2:
            self._session.rollback()
            internal_error = InternalServerError(
                detail="Comportamento inesperado ao tentar inserir volume de entidades.",
                headers={
                    "entity": self.get_model_name(),
                    "generic_error": str(exception2),
                },
            )

            self._logger.exception(f"{internal_error.detail}\n{internal_error.headers}")
            raise internal_error

        if commit_at_the_end:
            self._session.commit()

        self._logger.debug(
            f"Volume de {len(entities)} entidades "
            f"{self.get_model_name()} inserido com sucesso!"
        )

        return {"message": f"Volume de {len(entities)} entidades inserido com sucesso!"}

    def update(
        self,
        attributes: Dict[str, Any],
        filters: Union[BooleanClauseList, BinaryExpression],
        commit_at_the_end: bool = True,
    ) -> List[declarative_base]:
        self.__set_delete_filter(filters)
        query = self._session.query(self._model).filter(filters)

        self._logger.debug(
            f"A executar query para obter entidades {self.get_model_name()} "
            f"a serem atualizadas: {self._get_raw_query(query)}"
        )

        retrieved_entities = query.all()

        if not retrieved_entities:
            message = (
                f"Query não recuperou nenhuma entidade "
                f"{self.get_model_name()} para atualização."
            )
            self._logger.debug(message)
            raise EntityNotFound(detail=message)

        self._logger.debug(
            f"{len(retrieved_entities)} entidades {self.get_model_name()} "
            f"recuperadas para atualização, com atributos: {attributes}"
        )

        for i, entity in enumerate(retrieved_entities):
            setattr(entity, "modified_at", datetime.now())
            for column, value in attributes.items():
                setattr(entity, column, value)

            self._logger.debug(
                f"Atualizando entidade {self.get_model_name()} "
                f"- {i + 1} de {len(retrieved_entities)}..."
            )

            self.flush_and_commit(commit_at_the_end=False)

        if commit_at_the_end:
            self._session.commit()

        self._logger.debug(
            f"{len(retrieved_entities)} entidades {self.get_model_name()} "
            f"atualizadas com sucesso."
        )

        return retrieved_entities
