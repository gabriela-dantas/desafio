from fastapi import HTTPException, status
from typing import Dict, Any


class TooManyRedirects(HTTPException):
    def __init__(
        self, detail: str = "Too Many Requests.", headers: Dict[str, Any] = None
    ) -> None:
        super().__init__(
            status.HTTP_429_TOO_MANY_REQUESTS, detail=detail, headers=headers
        )
