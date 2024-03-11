from sqlalchemy import (
    Column,
    String,
)


class AssetTypeCommonModel:
    asset_type_code = Column(String(20), nullable=False)
    asset_type_code_ext = Column(String(10), nullable=True)
    asset_type_desc = Column(String(255), nullable=False)
