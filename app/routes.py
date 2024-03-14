import os
import jwt

from datetime import datetime, timedelta
from jwt.exceptions import DecodeError, InvalidTokenError
from fastapi import FastAPI, HTTPException, status, Security
from pydantic import ValidationError
from fastapi.security.api_key import APIKeyHeader

from schemas import TokenValidationResponseSchema, TokenValidationSchema, TokenResponseSchema


app = FastAPI(
    title="Challenge JWT API",
    description="API REST para validação de tokens JWT conforme regras definidas.",
    version="0.1.0",
    docs_url="/docs",
    openapi_url="/openapi.json"
)

api_key_header = APIKeyHeader(name="x-api-key")


@app.get(
    "/token/validate",
    tags=["token_validate"],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(api_key_header)],
    response_model=TokenValidationResponseSchema,
    description='''
    Recebe por parâmetros um JWT (string) e verifica se é valido conforme regras abaixo:
        •	Deve ser um JWT válido
        •	Deve conter apenas 3 claims (name, role e seed)
        •	A claim Name não pode ter carácter de números
        •	A claim Role deve conter apenas 1 dos três valores (Admin, Member e External)
        •	A claim Seed deve ser um número primo.
        •	O tamanho máximo da claim Name é de 256 caracteres.
    '''
)
def validate(token: str):
    try:
        data = jwt.decode(token, os.environ["JWT_SECRET"], algorithms=["HS256"])
        try:
            TokenValidationSchema(**data)
            return {"is_valid": True}
        except ValidationError:
            return {"is_valid": False}
    except DecodeError as decode_error:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Erro ao decodificar token: {decode_error}",
        )
    except InvalidTokenError as token_error:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Token inválido: {token_error}",
        )


@app.post(
    "/token/generate",
    tags=["token_generate"],
    status_code=status.HTTP_200_OK,
    dependencies=[Security(api_key_header)],
    response_model=TokenResponseSchema,
    description='''
    Recebe Gera um Token JWT a partir dos dados informados, que devem ser validados segundo as regras:
        •	Deve conter apenas os campos: name, role e seed
        •	O name não pode ter carácter de números
        •	A role deve conter apenas 1 dos três valores (Admin, Member e External)
        •	A seed deve ser um número primo.
        •	O tamanho máximo do name é de 256 caracteres.
    '''
)
def generate(data: TokenValidationSchema):
    data = data.model_dump()

    expire = datetime.utcnow() + timedelta(minutes=15)
    data.update({"exp": expire})
    encoded_jwt = jwt.encode(data, os.environ["JWT_SECRET"], algorithm="HS256")

    return {"token": encoded_jwt}
