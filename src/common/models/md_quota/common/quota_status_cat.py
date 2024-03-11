from sqlalchemy import (
    Column,
    String,
)


class QuotaStatusCatCommonModel:
    quota_status_cat_code = Column(String(10), nullable=False)
    quota_status_cat_desc = Column(String(255), nullable=False)
