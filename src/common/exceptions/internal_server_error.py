from fastapi import HTTPException, status
from typing import Dict, Any


class InternalServerError(HTTPException):
    def __init__(
        self, detail: str = "Erro desconhecido.", headers: Dict[str, Any] = None
    ) -> None:
        super().__init__(
            status.HTTP_500_INTERNAL_SERVER_ERROR, detail=detail, headers=headers
        )
