from typing import List, Dict, Type, Tuple
from sqlalchemy.orm.session import Session
from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.decl_api import DeclarativeMeta

from simple_common.utils import get_all_class_instances
from common import database
from common.database.connection import AbstractConnection
from simple_common.logger import logger


class SessionLocal:
    def __init__(self) -> None:
        self.__connection_module = database.__name__
        self.__engines_by_base: Dict[Type[DeclarativeMeta], Engine] = {}

        self.__logger = logger

    @property
    def engines_by_base(self) -> Dict[Type[DeclarativeMeta], Engine]:
        return self.__engines_by_base.copy()

    def create(self) -> Tuple[Session, Dict[Type[DeclarativeMeta], Engine]]:
        connection_instances: List[AbstractConnection] = get_all_class_instances(
            self.__connection_module
        )

        for connection_object in connection_instances:
            engine = create_engine(
                url=connection_object.driver,
                creator=connection_object.get_connection,
                pool_size=40,
                max_overflow=0,
                pool_pre_ping=True,
            )

            self.__engines_by_base[connection_object.base_class] = engine

            self.__logger.debug(
                f"Criado objeto de conexão com banco "
                f"{connection_object.connection_name}"
            )

        session = sessionmaker(autocommit=False, autoflush=False)
        session.configure(binds=self.__engines_by_base)
        return session(), self.engines_by_base


session_local = SessionLocal()
db_session, engines_by_base = SessionLocal().create()
