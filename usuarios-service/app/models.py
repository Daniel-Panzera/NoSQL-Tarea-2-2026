from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, EmailStr, Field, PlainSerializer

# Montos como Decimal internamente (Cassandra: decimal) y como número en el JSON
Monto = Annotated[
    Decimal,
    Field(ge=0, decimal_places=2),
    PlainSerializer(float, return_type=float, when_used="json"),
]

TipoDocumento = Literal["CI", "DNI", "PASAPORTE"]


class Direccion(BaseModel):
    calle: str = Field(min_length=1)
    ciudad: str = Field(min_length=1)
    codigo_postal: str = Field(min_length=1)


class UsuarioCrear(BaseModel):
    tipo_documento: TipoDocumento
    nro_documento: str = Field(min_length=1, max_length=20)
    nombre: str = Field(min_length=1)
    apellido: str = Field(min_length=1)
    email: EmailStr
    direcciones: list[Direccion] = Field(default_factory=list)


class Usuario(UsuarioCrear):
    id: str
    creado_en: datetime


class Compra(BaseModel):
    pedido_id: str
    fecha: datetime
    total: Monto
    estado: str


class CompraRegistrar(BaseModel):
    """Lo envía el orquestador SAGA (servicio de Pedidos) al confirmar un pedido."""

    pedido_id: str = Field(min_length=1)
    total: Monto
    estado: str = "CONFIRMADO"
    fecha: datetime | None = None


class UsuarioDetalle(Usuario):
    historial_compras: list[Compra] = Field(default_factory=list)
