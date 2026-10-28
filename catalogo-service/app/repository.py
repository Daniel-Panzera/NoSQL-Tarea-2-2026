import time
from datetime import datetime, timezone

from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.query import BatchStatement

from .models import Producto, ProductoResumen


def _a_utc(fecha: datetime) -> datetime:
    """Cassandra guarda timestamps en UTC pero el driver los devuelve sin zona horaria."""
    return fecha.replace(tzinfo=timezone.utc) if fecha.tzinfo is None else fecha


class CatalogoRepository:
    """Modelo de LECTURA del catálogo en Cassandra (lado query de CQRS).
    El stock que guarda es una copia; la fuente de verdad es MongoDB."""

    def __init__(self, session: Session, keyspace: str):
        self._session = session
        ks = keyspace

        self._insertar_por_id = session.prepare(
            f"INSERT INTO {ks}.productos_por_id "
            "(producto_id, nombre, descripcion, categoria, precio, stock_disponible, actualizado_en) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        self._insertar_por_categoria = session.prepare(
            f"INSERT INTO {ks}.productos_por_categoria (categoria, producto_id, nombre, precio) VALUES (?, ?, ?, ?)"
        )

        self._obtener = session.prepare(f"SELECT * FROM {ks}.productos_por_id WHERE producto_id = ?")
        self._obtener.consistency_level = ConsistencyLevel.LOCAL_ONE

        self._listar_por_categoria = session.prepare(
            f"SELECT * FROM {ks}.productos_por_categoria WHERE categoria = ?"
        )
        self._listar_por_categoria.consistency_level = ConsistencyLevel.LOCAL_ONE

        # USING TIMESTAMP: si dos sincronizaciones llegan desordenadas, gana la que leyó
        # el stock más reciente de MongoDB (last-write-wins de Cassandra).
        self._actualizar_stock = session.prepare(
            f"UPDATE {ks}.productos_por_id USING TIMESTAMP ? "
            "SET stock_disponible = ?, actualizado_en = ? WHERE producto_id = ?"
        )

    def insertar(self, producto: Producto) -> None:
        # Batch LOGGED: las dos tablas desnormalizadas quedan escritas juntas o ninguna
        lote = BatchStatement()
        lote.add(
            self._insertar_por_id,
            (
                producto.id,
                producto.nombre,
                producto.descripcion,
                producto.categoria,
                producto.precio,
                producto.stock_disponible,
                producto.actualizado_en,
            ),
        )
        lote.add(self._insertar_por_categoria, (producto.categoria, producto.id, producto.nombre, producto.precio))
        self._session.execute(lote)

    def obtener(self, producto_id: str) -> Producto | None:
        fila = self._session.execute(self._obtener, (producto_id,)).one()
        if fila is None:
            return None
        return Producto(
            id=fila.producto_id,
            nombre=fila.nombre,
            descripcion=fila.descripcion or "",
            categoria=fila.categoria,
            precio=fila.precio,
            stock_disponible=fila.stock_disponible,
            actualizado_en=_a_utc(fila.actualizado_en),
        )

    def listar_por_categoria(self, categoria: str) -> list[ProductoResumen]:
        filas = self._session.execute(self._listar_por_categoria, (categoria,))
        return [ProductoResumen(id=f.producto_id, nombre=f.nombre, precio=f.precio) for f in filas]

    def actualizar_stock(self, producto_id: str, stock: int, leido_en_micros: int) -> None:
        self._session.execute(
            self._actualizar_stock,
            (leido_en_micros, stock, datetime.now(timezone.utc), producto_id),
        )


def ahora_micros() -> int:
    return time.time_ns() // 1000
