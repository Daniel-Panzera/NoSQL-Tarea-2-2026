from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, Field, PlainSerializer

# Montos como Decimal internamente (MongoDB: Decimal128) y como número en el JSON
Monto = Annotated[
    Decimal,
    Field(ge=0),
    PlainSerializer(float, return_type=float, when_used="json"),
]

EstadoPedido = Literal["PROCESANDO", "CONFIRMADO", "RECHAZADO", "COMPENSANDO", "CANCELADO", "REQUIERE_INTERVENCION"]
EstadoPago = Literal["APROBADO", "RECHAZADO", "REEMBOLSADO"]
EstadoPaso = Literal["COMPLETADO", "FALLIDO", "COMPENSADO", "COMPENSACION_FALLIDA"]


class ItemSolicitud(BaseModel):
    producto_id: str
    cantidad: int


class PedidoCrear(BaseModel):
    """Solo se validan tipos: las reglas de negocio las aplica el ValidadorDeDatos de la cadena.
    El precio NO se recibe del cliente: se toma del catálogo."""

    usuario_id: str
    items: list[ItemSolicitud]
    metodo_pago: str


class ItemPedido(BaseModel):
    producto_id: str
    cantidad: int
    precio_unitario: Monto | None = None


class Pago(BaseModel):
    metodo: str
    estado_pago: EstadoPago
    transaccion_id: str | None = None


class PasoSaga(BaseModel):
    paso: str
    estado: EstadoPaso
    detalle: str
    fecha: datetime


class Pedido(BaseModel):
    pedido_id: str
    usuario_id: str
    estado: EstadoPedido
    items: list[ItemPedido]
    total: Monto
    pago: Pago | None = None
    motivo_rechazo: str | None = None
    saga: list[PasoSaga]
    creado_en: datetime
    actualizado_en: datetime
