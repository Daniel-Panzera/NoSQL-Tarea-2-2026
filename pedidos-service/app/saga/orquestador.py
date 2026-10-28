import logging
import time

from ..chain.base import ContextoPedido, Manejador, PedidoRechazadoError
from ..repository import PedidoRepository

logger = logging.getLogger(__name__)

REINTENTOS_COMPENSACION = 3


class OrquestadorSaga:
    """Orquestador central de la SAGA. Ejecuta la cadena de manejadores, persiste el estado
    del pedido después de cada paso y, si un paso falla, ejecuta las compensaciones de los
    pasos ya completados en orden inverso."""

    def __init__(self, repositorio: PedidoRepository, cadena: Manejador):
        self._repositorio = repositorio
        self._cadena = cadena

    def ejecutar(self, ctx: ContextoPedido) -> int:
        """Devuelve el código HTTP que corresponde al resultado de la SAGA."""
        ctx.al_avanzar = self._repositorio.actualizar

        try:
            self._cadena.manejar(ctx)
            ctx.estado = "CONFIRMADO"
            codigo = 201
        except PedidoRechazadoError as error:
            ctx.registrar_paso(error.paso or "OrquestadorSaga", "FALLIDO", error.motivo)
            ctx.motivo_rechazo = error.motivo
            codigo = error.codigo_http

            if ctx.compensables:
                ctx.estado = "COMPENSANDO"
                self._repositorio.actualizar(ctx)
                ctx.estado = "CANCELADO" if self._compensar(ctx) else "REQUIERE_INTERVENCION"
            else:
                # Falló antes de cualquier efecto en otros servicios: no hay nada que deshacer
                ctx.estado = "RECHAZADO"

        self._repositorio.actualizar(ctx)
        logger.info("Pedido %s finalizó en estado %s", ctx.pedido_id, ctx.estado)
        return codigo

    def _compensar(self, ctx: ContextoPedido) -> bool:
        """Las compensaciones son idempotentes, así que se pueden reintentar sin riesgo."""
        todo_ok = True
        for manejador in reversed(ctx.compensables):
            for intento in range(1, REINTENTOS_COMPENSACION + 1):
                try:
                    detalle = manejador.compensar(ctx)
                    ctx.registrar_paso(manejador.nombre, "COMPENSADO", detalle)
                    break
                except Exception as error:
                    logger.warning(
                        "Compensación de %s falló (intento %d/%d): %s",
                        manejador.nombre, intento, REINTENTOS_COMPENSACION, error,
                    )
                    if intento == REINTENTOS_COMPENSACION:
                        ctx.registrar_paso(manejador.nombre, "COMPENSACION_FALLIDA", str(error))
                        todo_ok = False
                    else:
                        time.sleep(0.5 * intento)
            self._repositorio.actualizar(ctx)
        return todo_ok
