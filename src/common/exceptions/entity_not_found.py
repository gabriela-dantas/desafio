from fastapi import HTTPException, status
from typing import Dict, Any


class EntityNotFound(HTTPException):
    def __init__(
        self, detail: str = "Entidade não encontrada.", headers: Dict[str, Any] = None
    ) -> None:
        super().__init__(status.HTTP_404_NOT_FOUND, detail=detail, headers=headers)
