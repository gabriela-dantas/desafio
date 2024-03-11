from fastapi import HTTPException, status
from typing import Dict, Any


class Unauthorized(HTTPException):
    def __init__(
        self, detail: str = "Uusário não identificado.", headers: Dict[str, Any] = None
    ) -> None:
        super().__init__(status.HTTP_401_UNAUTHORIZED, detail=detail, headers=headers)
