import time

from fastapi import FastAPI, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import Message
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi_restful import Api
from starlette.exceptions import HTTPException as StarletteHTTPException
from mangum import Mangum
from typing import Callable

from api.configuartions.config import Settings
from api.configuartions.resource_builder import ResourceBuilder
from simple_common.logger import logger


def create_application() -> FastAPI:
    setting = Settings()

    application = FastAPI(
        title=setting.app_name,
        description=setting.description,
        version=setting.version,
        docs_url=setting.docs_url,
        openapi_url=setting.openapi_url,
        root_path=setting.root_path,
    )

    api = Api(application)

    ResourceBuilder().add_resources(api)

    return application


app = create_application()
api_logger = logger


async def set_body(request: Request, body: bytes) -> None:
    async def receive() -> Message:
        return {"type": "http.request", "body": body}

    request._receive = receive


@app.middleware("http")
async def logging_middleware(request: Request, call_next: Callable) -> Response:
    request_body = await request.body()
    await set_body(request, request_body)

    log_data = {
        "Requisição iniciada...": {
            "URL": f"{request.method} {request.url}",
            "query_params": {key: value for key, value in request.query_params.items()},
            "path_params": request.path_params,
            "Body": request_body,
        }
    }
    api_logger.extra["data_info"]["url"] = f"{request.method} {request.url}"
    api_logger.info(log_data)

    start_time = time.time()

    response: Response = await call_next(request)

    process_time = (time.time() - start_time) * 1000
    formatted_process_time = "{0:.2f}".format(process_time)

    response_body = b""
    async for chunk in response.body_iterator:
        response_body += chunk

    log_data = {
        "Response": {
            "statusCode": response.status_code,
            "content": response_body,
            "completed_in": f"{formatted_process_time} ms",
        }
    }

    api_logger.info(log_data)

    return Response(
        content=response_body,
        status_code=response.status_code,
        headers=dict(response.headers),
        media_type=response.media_type,
    )


@app.exception_handler(RequestValidationError)
def validation_exception_handler(_request: Request, exception: RequestValidationError):
    api_logger.exception(
        f"Erro de validação nos dados recebidos da requisição: {exception.errors()}",
        exc_info=exception,
    )

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=jsonable_encoder({"detail": exception.errors()}),
    )


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(_request: Request, exception: StarletteHTTPException):
    api_logger.exception(
        f"Erro identificado ao processar a requisição: {exception.detail}",
        exc_info=exception,
    )

    return JSONResponse(
        status_code=exception.status_code,
        content={"detail": exception.detail},
    )


@app.exception_handler(Exception)
async def generic_exception_handler(_request: Request, exception: Exception):
    api_logger.exception(
        f"Erro desconhecido ao processar a requisição: {exception}", exc_info=exception
    )

    return PlainTextResponse(
        f"Erro desconhecido: {exception}",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
    )


handler = Mangum(app)
