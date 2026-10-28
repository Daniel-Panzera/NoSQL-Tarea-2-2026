import uuid
from datetime import datetime, timezone
from decimal import Decimal

from bson.decimal128 import Decimal128
from pymongo.database import Database

# Método de prueba para forzar un rechazo en la demo
METODO_RECHAZO_SIMULADO = "TARJETA_RECHAZADA"


class PagoRechazadoError(Exception):
    pass


class PasarelaPagoSimulada:
    """Simula una pasarela de pagos externa. Registra cada cobro en la colección 'pagos'
    con índice único por pedido: un mismo pedido nunca se cobra dos veces."""

    def __init__(self, db: Database, limite_aprobacion: Decimal):
        self._pagos = db["pagos"]
        self._limite = limite_aprobacion

    def inicializar(self) -> None:
        self._pagos.create_index("pedido_id", unique=True)

    def cobrar(self, pedido_id: str, monto: Decimal, metodo: str) -> str:
        existente = self._pagos.find_one({"pedido_id": pedido_id, "estado": "APROBADO"})
        if existente:
            return existente["_id"]

        if metodo == METODO_RECHAZO_SIMULADO:
            raise PagoRechazadoError("Tarjeta rechazada por el emisor")
        if monto > self._limite:
            raise PagoRechazadoError(f"El monto {monto} supera el límite de aprobación ({self._limite})")

        transaccion_id = f"tx_{uuid.uuid4().hex[:12]}"
        self._pagos.insert_one(
            {
                "_id": transaccion_id,
                "pedido_id": pedido_id,
                "monto": Decimal128(str(monto)),
                "metodo": metodo,
                "estado": "APROBADO",
                "fecha": datetime.now(timezone.utc),
            }
        )
        return transaccion_id

    def reembolsar(self, pedido_id: str) -> str | None:
        """Compensación del cobro. Idempotente: solo pasa de APROBADO a REEMBOLSADO una vez."""
        pago = self._pagos.find_one_and_update(
            {"pedido_id": pedido_id, "estado": "APROBADO"},
            {"$set": {"estado": "REEMBOLSADO", "reembolsado_en": datetime.now(timezone.utc)}},
        )
        return pago["_id"] if pago else None
