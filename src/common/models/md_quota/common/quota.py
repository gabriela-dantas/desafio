from sqlalchemy import (
    Column,
    BigInteger,
    String,
    DateTime,
    Float,
    Integer,
    Boolean,
)

from common.constants import (
    DEFAULT_QUOTA_NUMBER,
    DEFAULT_QUOTA_VERSION_ID,
    DEFAULT_QUOTA_CHECK_DIGIT,
)


class QuotaCommonModel:
    quota_id = Column(BigInteger, primary_key=True, index=True)
    quota_code = Column(String(255), nullable=False)
    quota_number = Column(String(255), nullable=True, default=DEFAULT_QUOTA_NUMBER)
    check_digit = Column(String(255), nullable=True, default=DEFAULT_QUOTA_CHECK_DIGIT)
    external_reference = Column(String(255), nullable=False)
    total_installments = Column(Integer, nullable=True)
    version_id = Column(String(70), nullable=True, default=DEFAULT_QUOTA_VERSION_ID)
    contract_number = Column(String(70), nullable=True)
    is_contemplated = Column(Boolean, nullable=False, default=False)
    is_multiple_ownership = Column(Boolean, nullable=False, default=False)
    contemplation_date = Column(DateTime, nullable=True)
    administrator_fee = Column(Float, nullable=True)
    insurance_fee = Column(Float, nullable=True)
    fund_reservation_fee = Column(Float, nullable=True)
    acquisition_date = Column(DateTime, nullable=True)
    cancel_date = Column(DateTime, nullable=True)
