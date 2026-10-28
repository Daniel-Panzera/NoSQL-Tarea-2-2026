import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

from ..clientes import ServicioNoDisponibleError
from ..models import EstadoPaso, ItemPedido, Pago, PasoSaga, PedidoCrear

logger = logging.getLogger(__name__)


class PedidoRechazadoError(Exception):
    """Un manejador corta la cadena. codigo_http es lo que se devuelve al cliente."""

    def __init__(self, motivo: str, codigo_http: int):
        super().__init__(motivo)
        self.motivo = motivo
        self.codigo_http = codigo_http
        self.paso: str | None = None


class FalloTecnicoError(PedidoRechazadoError):
    def __init__(self, motivo: str):
        super().__init__(motivo, 503)


@dataclass
class ContextoPedido:
    """Solicitud que viaja por la cadena; cada manejador la lee y la enriquece."""

    pedido_id: str
    solicitud: PedidoCrear
    clave_idempotencia: str | None = None
    simular_fallo: str | None = None
    estado: str = "PROCESANDO"
    items: list[ItemPedido] = field(default_factory=list)
    total: Decimal = Decimal("0")
    pago: Pago | None = None
    motivo_rechazo: str | None = None
    pasos: list[PasoSaga] = field(default_factory=list)
    creado_en: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    # Manejadores ya ejecutados que tienen compensación, en orden de ejecución
    compensables: list["Manejador"] = field(default_factory=list)
    # Lo asigna el orquestador para persistir el estado después de cada paso
    al_avanzar: Callable[["ContextoPedido"], None] | None = None

    def __post_init__(self):
        if not self.items:
            self.items = [ItemPedido(producto_id=i.producto_id, cantidad=i.cantidad) for i in self.solicitud.items]

    def registrar_paso(self, paso: str, estado: EstadoPaso, detalle: str) -> None:
        self.pasos.append(PasoSaga(paso=paso, estado=estado, detalle=detalle, fecha=datetime.now(timezone.utc)))


class Manejador(ABC):
    """Eslabón de la Chain of Responsibility. Si procesar() no lanza excepción,
    la solicitud pasa al siguiente; si lanza PedidoRechazadoError, la cadena se corta."""

    compensable = False

    def __init__(self):
        self._siguiente: Manejador | None = None

    @property
    def nombre(self) -> str:
        return type(self).__name__

    def enlazar(self, siguiente: "Manejador") -> "Manejador":
        self._siguiente = siguiente
        return siguiente

    def manejar(self, ctx: ContextoPedido) -> None:
        try:
            detalle = self.procesar(ctx)
        except PedidoRechazadoError as error:
            error.paso = self.nombre
            raise
        except ServicioNoDisponibleError as error:
            fallo = FalloTecnicoError(str(error))
            fallo.paso = self.nombre
            raise fallo from error
        except Exception as error:
            logger.exception("Error inesperado en %s", self.nombre)
            fallo = FalloTecnicoError(f"Error inesperado: {error}")
            fallo.paso = self.nombre
            raise fallo from error

        ctx.registrar_paso(self.nombre, "COMPLETADO", detalle)
        if self.compensable:
            ctx.compensables.append(self)
        if ctx.al_avanzar:
            ctx.al_avanzar(ctx)

        if self._siguiente:
            self._siguiente.manejar(ctx)

    @abstractmethod
    def procesar(self, ctx: ContextoPedido) -> str:
        """Ejecuta el paso y devuelve un detalle legible para el log de la SAGA."""

    def compensar(self, ctx: ContextoPedido) -> str:
        """Deshace el efecto de procesar(). Solo lo implementan los manejadores compensables."""
        raise NotImplementedError
