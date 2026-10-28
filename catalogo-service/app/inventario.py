from datetime import datetime, timezone

from pymongo import ReturnDocument
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from .models import ItemReserva, Reserva


class ProductoNoEncontradoError(Exception):
    def __init__(self, producto_id: str):
        super().__init__(f"Producto {producto_id} no existe")
        self.producto_id = producto_id


class StockInsuficienteError(Exception):
    def __init__(self, producto_id: str, solicitado: int, disponible: int):
        super().__init__(
            f"Stock insuficiente para {producto_id}: solicitado {solicitado}, disponible {disponible}"
        )
        self.producto_id = producto_id
        self.solicitado = solicitado
        self.disponible = disponible


class ReservaInvalidaError(Exception):
    pass


class InventarioRepository:
    """Fuente de verdad del stock en MongoDB (lado comando de CQRS).

    Colecciones:
      - inventario: {_id: producto_id, stock, actualizado_en}
      - reservas:   {_id: pedido_id, items, estado, creado_en, actualizado_en}
    """

    def __init__(self, db: Database):
        self._db = db
        self._cliente = db.client
        self._inventario = db["inventario"]
        self._reservas = db["reservas"]

    def inicializar(self) -> None:
        existentes = self._db.list_collection_names()
        if "inventario" not in existentes:
            # Segunda barrera contra la sobreventa: la base rechaza cualquier stock negativo
            self._db.create_collection(
                "inventario",
                validator={
                    "$jsonSchema": {
                        "bsonType": "object",
                        "required": ["stock"],
                        "properties": {"stock": {"bsonType": "int", "minimum": 0}},
                    }
                },
            )
        if "reservas" not in existentes:
            # Se crea fuera de la transacción; el _id = pedido_id hace única cada reserva
            self._db.create_collection("reservas")

    # ---------- Stock ----------

    def crear_stock(self, producto_id: str, stock: int) -> None:
        self._inventario.insert_one({"_id": producto_id, "stock": stock, "actualizado_en": _ahora()})

    def eliminar_stock(self, producto_id: str) -> None:
        self._inventario.delete_one({"_id": producto_id})

    def obtener_stock(self, producto_id: str) -> int | None:
        doc = self._inventario.find_one({"_id": producto_id})
        return doc["stock"] if doc else None

    # ---------- Reservas (pasos y compensación de la SAGA) ----------

    def reservar(self, pedido_id: str, items: list[ItemReserva]) -> tuple[Reserva, bool]:
        """Descuenta el stock de TODOS los items o de ninguno (transacción multi-documento).
        Devuelve (reserva, creada); si el pedido ya tenía reserva no descuenta de nuevo (idempotencia)."""
        cantidades = _agrupar(items)
        ahora = _ahora()

        def transaccion(session):
            self._reservas.insert_one(
                {
                    "_id": pedido_id,
                    "items": [{"producto_id": p, "cantidad": c} for p, c in cantidades.items()],
                    "estado": "RESERVADA",
                    "creado_en": ahora,
                    "actualizado_en": ahora,
                },
                session=session,
            )
            for producto_id, cantidad in cantidades.items():
                # Descuento condicional y atómico: solo aplica si stock >= cantidad.
                # Si dos pedidos compiten por el mismo producto, MongoDB detecta el conflicto
                # de escritura y with_transaction reintenta con el stock actualizado.
                actualizado = self._inventario.find_one_and_update(
                    {"_id": producto_id, "stock": {"$gte": cantidad}},
                    {"$inc": {"stock": -cantidad}, "$set": {"actualizado_en": ahora}},
                    session=session,
                    return_document=ReturnDocument.AFTER,
                )
                if actualizado is None:
                    actual = self._inventario.find_one({"_id": producto_id}, session=session)
                    if actual is None:
                        raise ProductoNoEncontradoError(producto_id)
                    raise StockInsuficienteError(producto_id, cantidad, actual["stock"])

        with self._cliente.start_session() as session:
            try:
                session.with_transaction(
                    transaccion,
                    read_concern=ReadConcern("snapshot"),
                    write_concern=WriteConcern("majority"),
                )
            except DuplicateKeyError:
                return self.obtener_reserva(pedido_id), False

        return self.obtener_reserva(pedido_id), True

    def confirmar(self, pedido_id: str) -> Reserva | None:
        reserva = self._reservas.find_one_and_update(
            {"_id": pedido_id, "estado": {"$in": ["RESERVADA", "CONFIRMADA"]}},
            {"$set": {"estado": "CONFIRMADA", "actualizado_en": _ahora()}},
            return_document=ReturnDocument.AFTER,
        )
        if reserva is None:
            existente = self.obtener_reserva(pedido_id)
            if existente is None:
                return None
            raise ReservaInvalidaError(f"La reserva {pedido_id} está {existente.estado} y no se puede confirmar")
        return _doc_a_reserva(reserva)

    def liberar(self, pedido_id: str) -> Reserva | None:
        """Compensación: devuelve el stock reservado. Es idempotente: liberar dos veces
        no devuelve stock dos veces, porque el cambio de estado y el $inc van en la misma transacción."""
        ahora = _ahora()

        def transaccion(session):
            reserva = self._reservas.find_one_and_update(
                {"_id": pedido_id, "estado": {"$in": ["RESERVADA", "CONFIRMADA"]}},
                {"$set": {"estado": "LIBERADA", "actualizado_en": ahora}},
                session=session,
            )
            if reserva is None:
                return
            for item in reserva["items"]:
                self._inventario.update_one(
                    {"_id": item["producto_id"]},
                    {"$inc": {"stock": item["cantidad"]}, "$set": {"actualizado_en": ahora}},
                    session=session,
                )

        with self._cliente.start_session() as session:
            session.with_transaction(
                transaccion,
                read_concern=ReadConcern("snapshot"),
                write_concern=WriteConcern("majority"),
            )

        return self.obtener_reserva(pedido_id)

    def obtener_reserva(self, pedido_id: str) -> Reserva | None:
        doc = self._reservas.find_one({"_id": pedido_id})
        return _doc_a_reserva(doc) if doc else None


def _ahora() -> datetime:
    return datetime.now(timezone.utc)


def _agrupar(items: list[ItemReserva]) -> dict[str, int]:
    """Suma cantidades si el mismo producto viene repetido en el pedido."""
    cantidades: dict[str, int] = {}
    for item in items:
        cantidades[item.producto_id] = cantidades.get(item.producto_id, 0) + item.cantidad
    return cantidades


def _doc_a_reserva(doc: dict) -> Reserva:
    return Reserva(
        pedido_id=doc["_id"],
        items=[ItemReserva(**i) for i in doc["items"]],
        estado=doc["estado"],
        creado_en=_a_utc(doc["creado_en"]),
        actualizado_en=_a_utc(doc["actualizado_en"]),
    )


def _a_utc(fecha: datetime) -> datetime:
    """PyMongo devuelve las fechas sin zona horaria (siempre en UTC)."""
    return fecha.replace(tzinfo=timezone.utc) if fecha.tzinfo is None else fecha
