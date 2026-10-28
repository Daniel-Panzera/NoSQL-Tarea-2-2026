import logging
import os
import uuid
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, status
from pymongo.errors import DuplicateKeyError

from .chain.base import ContextoPedido
from .chain.manejadores import construir_cadena
from .clientes import CatalogoClient, UsuariosClient
from .db import conectar_mongo
from .models import Pedido, PedidoCrear
from .pagos import PasarelaPagoSimulada
from .repository import PedidoRepository
from .saga.orquestador import OrquestadorSaga

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cliente_mongo, db = conectar_mongo()

    repositorio = PedidoRepository(db)
    repositorio.inicializar()
    pasarela = PasarelaPagoSimulada(db, Decimal(os.getenv("PAGO_LIMITE_APROBACION", "5000")))
    pasarela.inicializar()

    usuarios = UsuariosClient(os.getenv("USUARIOS_URL", "http://localhost:8001"))
    catalogo = CatalogoClient(os.getenv("CATALOGO_URL", "http://localhost:8002"))

    app.state.repositorio = repositorio
    app.state.orquestador = OrquestadorSaga(repositorio, construir_cadena(usuarios, catalogo, pasarela))
    app.state.simulacion_habilitada = os.getenv("SIMULACION_FALLOS_HABILITADA", "false").lower() == "true"
    yield
    usuarios.cerrar()
    catalogo.cerrar()
    cliente_mongo.close()


app = FastAPI(
    title="ShopExpress - Servicio de Pedidos y Pagos",
    description=(
        "Orquestador SAGA + Chain of Responsibility. Persistencia en MongoDB con "
        "writeConcern/readConcern majority (prioridad: consistencia)."
    ),
    lifespan=lifespan,
)


def obtener_repositorio(request: Request) -> PedidoRepository:
    return request.app.state.repositorio


@app.get("/health", include_in_schema=False)
def health():
    return {"estado": "ok"}


@app.post(
    "/api/pedidos",
    response_model=Pedido,
    status_code=status.HTTP_201_CREATED,
    responses={
        200: {"description": "Reintento con la misma Idempotency-Key: se devuelve el pedido existente"},
        402: {"description": "Pago rechazado (stock liberado por compensación)"},
        404: {"description": "Usuario o producto inexistente"},
        409: {"description": "Stock insuficiente"},
        422: {"description": "Datos del pedido inválidos"},
        503: {"description": "Fallo técnico en un paso (se ejecutaron las compensaciones)"},
    },
)
def crear_pedido(
    datos: PedidoCrear,
    request: Request,
    response: Response,
    idempotency_key: Annotated[str | None, Header()] = None,
    x_simular_fallo: Annotated[Literal["confirmacion"] | None, Header()] = None,
):
    """Inicia la SAGA: la solicitud recorre la cadena ValidadorDeDatos → ValidadorDeUsuario →
    ValidadorDeStock → ReservaDeStock → ProcesadorDePago → ConfirmadorDePedido → RegistradorDeHistorial.
    El cuerpo de la respuesta siempre es el pedido, con el log de cada paso en `saga`."""
    repositorio: PedidoRepository = request.app.state.repositorio

    if idempotency_key:
        existente = repositorio.buscar_por_clave(idempotency_key)
        if existente:
            response.status_code = status.HTTP_200_OK
            return existente

    ctx = ContextoPedido(
        pedido_id=f"ord_{uuid.uuid4().hex[:12]}",
        solicitud=datos,
        clave_idempotencia=idempotency_key,
        simular_fallo=x_simular_fallo if request.app.state.simulacion_habilitada else None,
    )
    try:
        repositorio.crear(ctx)
    except DuplicateKeyError:
        # Dos solicitudes simultáneas con la misma clave: gana la primera
        response.status_code = status.HTTP_200_OK
        return repositorio.buscar_por_clave(idempotency_key)

    response.status_code = request.app.state.orquestador.ejecutar(ctx)
    return repositorio.obtener(ctx.pedido_id)


@app.get("/api/pedidos/{pedido_id}", response_model=Pedido)
def obtener_pedido(pedido_id: str, repositorio: PedidoRepository = Depends(obtener_repositorio)):
    pedido = repositorio.obtener(pedido_id)
    if pedido is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pedido no encontrado")
    return pedido
