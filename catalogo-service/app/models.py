from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, Field, PlainSerializer

# Montos como Decimal internamente (Cassandra: decimal) y como número en el JSON
Monto = Annotated[
    Decimal,
    Field(gt=0, decimal_places=2),
    PlainSerializer(float, return_type=float, when_used="json"),
]

EstadoReserva = Literal["RESERVADA", "CONFIRMADA", "LIBERADA"]


class ProductoCrear(BaseModel):
    nombre: str = Field(min_length=1)
    descripcion: str = ""
    categoria: str = Field(min_length=1)
    precio: Monto
    stock: int = Field(ge=0, description="Stock inicial")


class Producto(BaseModel):
    id: str
    nombre: str
    descripcion: str
    categoria: str
    precio: Monto
    stock_disponible: int
    fuente_stock: Literal["cassandra (eventual)", "mongodb (consistente)"] = "cassandra (eventual)"
    actualizado_en: datetime


class ProductoResumen(BaseModel):
    id: str
    nombre: str
    precio: Monto


class ItemReserva(BaseModel):
    producto_id: str = Field(min_length=1)
    cantidad: int = Field(gt=0)


class ReservaCrear(BaseModel):
    """La envía el orquestador SAGA (servicio de Pedidos)."""

    pedido_id: str = Field(min_length=1)
    items: list[ItemReserva] = Field(min_length=1)


class Reserva(BaseModel):
    pedido_id: str
    items: list[ItemReserva]
    estado: EstadoReserva
    creado_en: datetime
    actualizado_en: datetime
