import os
from pydantic import BaseSettings


class Settings(BaseSettings):
    app_name: str = "MD Cota"
    description: str = (
        "API REST privada para tratamento de dados de cotas oriundas de diversas ADMs, "
        "sendo componente do sistema MD Cota."
    )
    version: str = "0.1.0"
    docs_url: str = "/docs"
    openapi_url: str = "/openapi.json"
    root_path: str = os.environ["API_ROOT_PATH"]
