from sqlalchemy import (
    Column,
    String,
    DateTime,
    Float,
    Integer,
    Date,
)


class GroupCommonModel:
    group_code = Column(String(255), nullable=False)
    group_deadline = Column(Integer, nullable=True)
    group_start_date = Column(Date, nullable=True)
    group_closing_date = Column(Date, nullable=True)
    per_max_embedded_bid = Column(Float, nullable=True)
    next_adjustment_date = Column(Date, nullable=True)
    chosen_bid = Column(Float, nullable=True)
    max_bid_occurrences_perc = Column(Float, nullable=True)
    bid_calculation_date = Column(DateTime, nullable=True)
