from fastapi import HTTPException, status
from typing import Dict, Any


class Timeout(HTTPException):
    def __init__(
        self, detail: str = "Request Timeout.", headers: Dict[str, Any] = None
    ) -> None:
        super().__init__(
            status.HTTP_408_REQUEST_TIMEOUT, detail=detail, headers=headers
        )
