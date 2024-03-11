from sqlalchemy import (
    Column,
    String,
    Float,
    Integer,
    Date,
)


class QuotaHistoryDetailCommonModel:
    old_quota_number = Column(Integer, nullable=True)
    old_digit = Column(Integer, nullable=True)
    quota_plan = Column(String(70), nullable=True)
    installments_paid_number = Column(Integer, nullable=True)
    overdue_installments_number = Column(Integer, nullable=True)
    overdue_percentage = Column(Float, nullable=True)
    per_amount_paid = Column(Float, nullable=True)
    per_mutual_fund_paid = Column(Float, nullable=True)
    per_reserve_fund_paid = Column(Float, nullable=True)
    per_adm_paid = Column(Float, nullable=True)
    per_subscription_paid = Column(Float, nullable=True)
    per_mutual_fund_to_pay = Column(Float, nullable=True)
    per_reserve_fund_to_pay = Column(Float, nullable=True)
    per_adm_to_pay = Column(Float, nullable=True)
    per_subscription_to_pay = Column(Float, nullable=True)
    per_insurance_to_pay = Column(Float, nullable=True)
    per_install_diff_to_pay = Column(Float, nullable=True)
    per_total_amount_to_pay = Column(Float, nullable=True)
    amnt_mutual_fund_to_pay = Column(Float, nullable=True)
    amnt_reserve_fund_to_pay = Column(Float, nullable=True)
    amnt_adm_to_pay = Column(Float, nullable=True)
    amnt_subscription_to_pay = Column(Float, nullable=True)
    amnt_insurance_to_pay = Column(Float, nullable=True)
    amnt_fine_to_pay = Column(Float, nullable=True)
    amnt_interest_to_pay = Column(Float, nullable=True)
    amnt_others_to_pay = Column(Float, nullable=True)
    amnt_install_diff_to_pay = Column(Float, nullable=True)
    amnt_to_pay = Column(Float, nullable=True)
    quitter_assembly_number = Column(Integer, nullable=True)
    cancelled_assembly_number = Column(Integer, nullable=True)
    adjustment_date = Column(Date, nullable=True)
    current_assembly_date = Column(Date, nullable=True)
    current_assembly_number = Column(Integer, nullable=True)
    asset_adm_code = Column(String(255), nullable=True)
    asset_description = Column(String(255), nullable=True)
    asset_value = Column(Float, nullable=False)
