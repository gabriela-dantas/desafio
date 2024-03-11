from sqlalchemy import (
    Column,
    String,
)


class QuotaOriginCommonModel:
    quota_origin_code = Column(String(20), nullable=False)
    quota_origin_desc = Column(String(255), nullable=False)
