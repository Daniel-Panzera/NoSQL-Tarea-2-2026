import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Request, Response, status

from .db import conectar_cassandra, conectar_mongo
from .inventario import (
    InventarioRepository,
    ProductoNoEncontradoError,
    ReservaInvalidaError,
    StockInsuficienteError,
)
from .models import Producto, ProductoCrear, ProductoResumen, Reserva, ReservaCrear
from .repository import CatalogoRepository, ahora_micros

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cluster, session = conectar_cassandra()
    cliente_mongo, db = conectar_mongo()
    app.state.catalogo = CatalogoRepository(session, os.getenv("CASSANDRA_KEYSPACE", "shopexpress"))
    app.state.inventario = InventarioRepository(db)
    app.state.inventario.inicializar()
    yield
    cliente_mongo.close()
    cluster.shutdown()


app = FastAPI(
    title="ShopExpress - Servicio de Catálogo e Inventario",
    description=(
        "CQRS: el catálogo se lee desde Cassandra (rápido, consistencia eventual) y el stock "
        "se escribe en MongoDB (transacciones, sin sobreventa)."
    ),
    lifespan=lifespan,
)


def obtener_catalogo(request: Request) -> CatalogoRepository:
    return request.app.state.catalogo


def obtener_inventario(request: Request) -> InventarioRepository:
    return request.app.state.inventario


def sincronizar_stock(
    producto_ids: list[str], catalogo: CatalogoRepository, inventario: InventarioRepository
) -> None:
    """Proyecta el stock de MongoDB al modelo de lectura en Cassandra.
    Corre en segundo plano después de responder: por eso la lectura es eventualmente consistente."""
    for producto_id in producto_ids:
        leido_en = ahora_micros()
        stock = inventario.obtener_stock(producto_id)
        if stock is not None:
            catalogo.actualizar_stock(producto_id, stock, leido_en)


# Los endpoints son sincrónicos (def) porque los drivers son bloqueantes:
# FastAPI los ejecuta en un pool de hilos sin trabar el event loop.


@app.get("/health", include_in_schema=False)
def health():
    return {"estado": "ok"}


# ---------- Catálogo ----------


@app.post("/api/productos", response_model=Producto, status_code=status.HTTP_201_CREATED)
def crear_producto(
    datos: ProductoCrear,
    catalogo: CatalogoRepository = Depends(obtener_catalogo),
    inventario: InventarioRepository = Depends(obtener_inventario),
):
    producto = Producto(
        id=f"prod_{uuid.uuid4().hex[:12]}",
        nombre=datos.nombre,
        descripcion=datos.descripcion,
        categoria=datos.categoria,
        precio=datos.precio,
        stock_disponible=datos.stock,
        actualizado_en=datetime.now(timezone.utc),
    )
    # Primero la fuente de verdad (MongoDB); si falla Cassandra se deshace
    inventario.crear_stock(producto.id, datos.stock)
    try:
        catalogo.insertar(producto)
    except Exception:
        inventario.eliminar_stock(producto.id)
        raise
    return producto


@app.get("/api/productos", response_model=list[ProductoResumen])
def listar_productos(categoria: str, catalogo: CatalogoRepository = Depends(obtener_catalogo)):
    return catalogo.listar_por_categoria(categoria)


@app.get("/api/productos/{producto_id}", response_model=Producto)
def obtener_producto(
    producto_id: str,
    consistente: bool = False,
    catalogo: CatalogoRepository = Depends(obtener_catalogo),
    inventario: InventarioRepository = Depends(obtener_inventario),
):
    """Por defecto lee todo de Cassandra (rápido, stock eventual).
    Con ?consistente=true el stock se lee de MongoDB (lo usa el ValidadorDeStock)."""
    producto = catalogo.obtener(producto_id)
    if producto is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Producto no encontrado")
    if consistente:
        stock = inventario.obtener_stock(producto_id)
        if stock is not None:
            producto.stock_disponible = stock
            producto.fuente_stock = "mongodb (consistente)"
    return producto


# ---------- Inventario (uso interno del orquestador SAGA) ----------


@app.post(
    "/api/inventario/reservas",
    response_model=Reserva,
    status_code=status.HTTP_201_CREATED,
    summary="Reservar stock de un pedido (paso de la SAGA)",
)
def reservar_stock(
    datos: ReservaCrear,
    response: Response,
    tareas: BackgroundTasks,
    catalogo: CatalogoRepository = Depends(obtener_catalogo),
    inventario: InventarioRepository = Depends(obtener_inventario),
):
    try:
        reserva, creada = inventario.reservar(datos.pedido_id, datos.items)
    except ProductoNoEncontradoError as error:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(error))
    except StockInsuficienteError as error:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error))

    if not creada:
        response.status_code = status.HTTP_200_OK
    else:
        tareas.add_task(sincronizar_stock, [i.producto_id for i in reserva.items], catalogo, inventario)
    return reserva


@app.get("/api/inventario/reservas/{pedido_id}", response_model=Reserva)
def obtener_reserva(pedido_id: str, inventario: InventarioRepository = Depends(obtener_inventario)):
    reserva = inventario.obtener_reserva(pedido_id)
    if reserva is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Reserva no encontrada")
    return reserva


@app.post(
    "/api/inventario/reservas/{pedido_id}/confirmar",
    response_model=Reserva,
    summary="Confirmar la reserva cuando el pago fue aprobado (paso de la SAGA)",
)
def confirmar_reserva(pedido_id: str, inventario: InventarioRepository = Depends(obtener_inventario)):
    try:
        reserva = inventario.confirmar(pedido_id)
    except ReservaInvalidaError as error:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error))
    if reserva is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Reserva no encontrada")
    return reserva


@app.delete(
    "/api/inventario/reservas/{pedido_id}",
    response_model=Reserva,
    summary="Liberar el stock reservado (compensación de la SAGA)",
)
def liberar_reserva(
    pedido_id: str,
    tareas: BackgroundTasks,
    catalogo: CatalogoRepository = Depends(obtener_catalogo),
    inventario: InventarioRepository = Depends(obtener_inventario),
):
    reserva = inventario.liberar(pedido_id)
    if reserva is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Reserva no encontrada")
    tareas.add_task(sincronizar_stock, [i.producto_id for i in reserva.items], catalogo, inventario)
    return reserva
