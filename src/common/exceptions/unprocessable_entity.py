from fastapi import HTTPException, status
from typing import Dict, Any


class UnprocessableEntity(HTTPException):
    def __init__(
        self,
        detail: str = "Entidade com erros semânticos.",
        headers: Dict[str, Any] = None,
    ) -> None:
        super().__init__(
            status.HTTP_422_UNPROCESSABLE_ENTITY, detail=detail, headers=headers
        )
