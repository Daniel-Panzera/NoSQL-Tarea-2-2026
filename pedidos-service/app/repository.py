from datetime import datetime, timezone
from decimal import Decimal

from bson.decimal128 import Decimal128
from pymongo.database import Database

from .chain.base import ContextoPedido
from .models import ItemPedido, Pago, PasoSaga, Pedido


class PedidoRepository:
    """Colección 'pedidos': un documento por pedido con items, pago y log de la SAGA EMBEBIDOS
    (se leen y escriben siempre juntos). Usuario y productos se REFERENCIAN por id porque
    pertenecen a otros servicios."""

    def __init__(self, db: Database):
        self._pedidos = db["pedidos"]

    def inicializar(self) -> None:
        # _id = pedido_id ya es único. La clave de idempotencia evita duplicar un pedido
        # si el cliente reintenta la misma solicitud (índice parcial: solo si viene la clave).
        self._pedidos.create_index(
            "clave_idempotencia",
            unique=True,
            partialFilterExpression={"clave_idempotencia": {"$type": "string"}},
        )
        self._pedidos.create_index("usuario_id")

    def crear(self, ctx: ContextoPedido) -> None:
        self._pedidos.insert_one(self._a_documento(ctx))

    def actualizar(self, ctx: ContextoPedido) -> None:
        documento = self._a_documento(ctx)
        for campo in ("_id", "usuario_id", "clave_idempotencia", "creado_en"):
            documento.pop(campo)
        self._pedidos.update_one({"_id": ctx.pedido_id}, {"$set": documento})

    def obtener(self, pedido_id: str) -> Pedido | None:
        documento = self._pedidos.find_one({"_id": pedido_id})
        return self._a_pedido(documento) if documento else None

    def buscar_por_clave(self, clave_idempotencia: str) -> Pedido | None:
        documento = self._pedidos.find_one({"clave_idempotencia": clave_idempotencia})
        return self._a_pedido(documento) if documento else None

    @staticmethod
    def _a_documento(ctx: ContextoPedido) -> dict:
        return {
            "_id": ctx.pedido_id,
            "usuario_id": ctx.solicitud.usuario_id,
            "clave_idempotencia": ctx.clave_idempotencia,
            "estado": ctx.estado,
            "items": [
                {
                    "producto_id": i.producto_id,
                    "cantidad": i.cantidad,
                    "precio_unitario": _a_decimal128(i.precio_unitario),
                }
                for i in ctx.items
            ],
            "total": _a_decimal128(ctx.total),
            "pago": ctx.pago.model_dump() if ctx.pago else None,
            "motivo_rechazo": ctx.motivo_rechazo,
            "saga": [p.model_dump() for p in ctx.pasos],
            "creado_en": ctx.creado_en,
            "actualizado_en": datetime.now(timezone.utc),
        }

    @staticmethod
    def _a_pedido(documento: dict) -> Pedido:
        return Pedido(
            pedido_id=documento["_id"],
            usuario_id=documento["usuario_id"],
            estado=documento["estado"],
            items=[
                ItemPedido(
                    producto_id=i["producto_id"],
                    cantidad=i["cantidad"],
                    precio_unitario=_a_decimal(i["precio_unitario"]),
                )
                for i in documento["items"]
            ],
            total=_a_decimal(documento["total"]),
            pago=Pago(**documento["pago"]) if documento.get("pago") else None,
            motivo_rechazo=documento.get("motivo_rechazo"),
            saga=[PasoSaga(**{**p, "fecha": _a_utc(p["fecha"])}) for p in documento["saga"]],
            creado_en=_a_utc(documento["creado_en"]),
            actualizado_en=_a_utc(documento["actualizado_en"]),
        )


def _a_decimal128(valor: Decimal | None) -> Decimal128 | None:
    return Decimal128(str(valor)) if valor is not None else None


def _a_decimal(valor: Decimal128 | None) -> Decimal | None:
    return valor.to_decimal() if valor is not None else None


def _a_utc(fecha: datetime) -> datetime:
    """PyMongo devuelve las fechas sin zona horaria (siempre en UTC)."""
    return fecha.replace(tzinfo=timezone.utc) if fecha.tzinfo is None else fecha
