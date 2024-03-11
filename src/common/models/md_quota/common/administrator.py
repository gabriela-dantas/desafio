from sqlalchemy import (
    Column,
    String,
)


class AdministratorCommonModel:
    administrator_code = Column(String(20), nullable=False)
    administrator_desc = Column(String(255), nullable=False)
