from pydantic import BaseModel, Field, field_validator


class TokenValidationResponseSchema(BaseModel):
    is_valid: bool

    class Config:
        schema_extra = {
            "example": {"is_valid": True}
        }


class TokenValidationSchema(BaseModel):
    name: str = Field(max_length=256)
    role: str = Field(pattern=r'^Admin$|^Member$|^External$')
    seed: int

    @field_validator('name')
    def check_name_characters(cls, value: str):
        if not value.replace(" ", "").isalpha():
            raise ValueError('Campo name deve conter somente letras e espaços.')
        return value

    @field_validator('seed')
    def check_seed_is_prime(cls, value: int):
        is_prime = True

        for count in range(2, value):
            if value % count == 0:
                is_prime = False
                break

        if not is_prime:
            raise ValueError('Número não é primo.')


class TokenResponseSchema(BaseModel):
    token: str

    class Config:
        schema_extra = {
            "token": "token-generated"
        }
