from pydantic import BaseModel
from typing import List, Dict, Any


class CompanyBondEventSchema(BaseModel):
    cnpj: str
    bond_type: str
    representatives: List[Dict[str, Any]]
