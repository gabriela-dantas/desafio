from sqlalchemy import (
    Column,
    String,
)


class QuotaStatusTypeCommonModel:
    quota_status_type_code = Column(String(10), nullable=False)
    quota_status_type_desc = Column(String(255), nullable=False)
