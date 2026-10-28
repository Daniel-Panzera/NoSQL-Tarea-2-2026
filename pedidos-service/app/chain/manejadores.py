import logging
from decimal import Decimal

from ..clientes import CatalogoClient, UsuariosClient
from ..models import ItemPedido, Pago
from ..pagos import METODO_RECHAZO_SIMULADO, PagoRechazadoError, PasarelaPagoSimulada
from .base import ContextoPedido, FalloTecnicoError, Manejador, PedidoRechazadoError

logger = logging.getLogger(__name__)

METODOS_PAGO = {"TARJETA_CREDITO", "TARJETA_DEBITO", "TRANSFERENCIA", METODO_RECHAZO_SIMULADO}
MAX_ITEMS = 20
MAX_CANTIDAD = 100


class ValidadorDeDatos(Manejador):
    """Reglas de negocio sobre la solicitud. No consulta otros servicios."""

    def procesar(self, ctx: ContextoPedido) -> str:
        solicitud = ctx.solicitud
        if not solicitud.usuario_id.strip():
            raise PedidoRechazadoError("usuario_id es obligatorio", 422)
        if not solicitud.items:
            raise PedidoRechazadoError("El pedido debe tener al menos un item", 422)
        if len(solicitud.items) > MAX_ITEMS:
            raise PedidoRechazadoError(f"El pedido no puede tener más de {MAX_ITEMS} items", 422)

        vistos = set()
        for item in solicitud.items:
            if not 1 <= item.cantidad <= MAX_CANTIDAD:
                raise PedidoRechazadoError(
                    f"Cantidad inválida para {item.producto_id}: debe estar entre 1 y {MAX_CANTIDAD}", 422
                )
            if item.producto_id in vistos:
                raise PedidoRechazadoError(f"El producto {item.producto_id} está repetido", 422)
            vistos.add(item.producto_id)

        if solicitud.metodo_pago not in METODOS_PAGO:
            raise PedidoRechazadoError(
                f"Método de pago no soportado: {solicitud.metodo_pago}. Opciones: {sorted(METODOS_PAGO)}", 422
            )
        return f"{len(solicitud.items)} item(s) válidos, método {solicitud.metodo_pago}"


class ValidadorDeUsuario(Manejador):
    def __init__(self, usuarios: UsuariosClient):
        super().__init__()
        self._usuarios = usuarios

    def procesar(self, ctx: ContextoPedido) -> str:
        usuario = self._usuarios.obtener_usuario(ctx.solicitud.usuario_id)
        if usuario is None:
            raise PedidoRechazadoError(f"El usuario {ctx.solicitud.usuario_id} no existe", 404)
        return f"Usuario {usuario['id']} válido"


class ValidadorDeStock(Manejador):
    """Chequeo previo con lectura consistente del stock y toma de precios del catálogo.
    No reserva nada: la garantía real contra la sobreventa la da ReservaDeStock."""

    def __init__(self, catalogo: CatalogoClient):
        super().__init__()
        self._catalogo = catalogo

    def procesar(self, ctx: ContextoPedido) -> str:
        items_con_precio = []
        for item in ctx.items:
            producto = self._catalogo.obtener_producto(item.producto_id)
            if producto is None:
                raise PedidoRechazadoError(f"El producto {item.producto_id} no existe", 404)
            if producto["stock_disponible"] < item.cantidad:
                raise PedidoRechazadoError(
                    f"Stock insuficiente para {item.producto_id}: "
                    f"solicitado {item.cantidad}, disponible {producto['stock_disponible']}",
                    409,
                )
            items_con_precio.append(
                ItemPedido(
                    producto_id=item.producto_id,
                    cantidad=item.cantidad,
                    precio_unitario=Decimal(str(producto["precio"])),
                )
            )

        ctx.items = items_con_precio
        ctx.total = sum((i.precio_unitario * i.cantidad for i in items_con_precio), Decimal("0")).quantize(
            Decimal("0.01")
        )
        return f"Stock disponible, total calculado {ctx.total}"


class ReservaDeStock(Manejador):
    """Paso SAGA 1: descuenta el stock en el Servicio de Inventario."""

    compensable = True

    def __init__(self, catalogo: CatalogoClient):
        super().__init__()
        self._catalogo = catalogo

    def procesar(self, ctx: ContextoPedido) -> str:
        items = [{"producto_id": i.producto_id, "cantidad": i.cantidad} for i in ctx.items]
        respuesta = self._catalogo.reservar(ctx.pedido_id, items)
        if respuesta.status_code in (404, 409):
            # 409 acá significa que otro pedido se llevó el stock después del ValidadorDeStock
            raise PedidoRechazadoError(respuesta.json()["detail"], respuesta.status_code)
        respuesta.raise_for_status()
        return "Stock reservado en inventario"

    def compensar(self, ctx: ContextoPedido) -> str:
        self._catalogo.liberar(ctx.pedido_id)
        return "Reserva liberada, stock devuelto al inventario"


class ProcesadorDePago(Manejador):
    """Paso SAGA 2: cobra el total a través de la pasarela (simulada)."""

    compensable = True

    def __init__(self, pasarela: PasarelaPagoSimulada):
        super().__init__()
        self._pasarela = pasarela

    def procesar(self, ctx: ContextoPedido) -> str:
        metodo = ctx.solicitud.metodo_pago
        try:
            transaccion_id = self._pasarela.cobrar(ctx.pedido_id, ctx.total, metodo)
        except PagoRechazadoError as error:
            ctx.pago = Pago(metodo=metodo, estado_pago="RECHAZADO")
            raise PedidoRechazadoError(f"Pago rechazado: {error}", 402)

        ctx.pago = Pago(metodo=metodo, estado_pago="APROBADO", transaccion_id=transaccion_id)
        return f"Pago aprobado ({transaccion_id})"

    def compensar(self, ctx: ContextoPedido) -> str:
        transaccion_id = self._pasarela.reembolsar(ctx.pedido_id)
        if ctx.pago:
            ctx.pago.estado_pago = "REEMBOLSADO"
        return f"Pago reembolsado ({transaccion_id})"


class ConfirmadorDePedido(Manejador):
    """Paso SAGA 3 (pivote): a partir de acá el pedido queda confirmado."""

    def __init__(self, catalogo: CatalogoClient):
        super().__init__()
        self._catalogo = catalogo

    def procesar(self, ctx: ContextoPedido) -> str:
        if ctx.simular_fallo == "confirmacion":
            raise FalloTecnicoError("Fallo simulado al confirmar el pedido (header X-Simular-Fallo)")
        self._catalogo.confirmar(ctx.pedido_id)
        ctx.estado = "CONFIRMADO"
        return "Reserva de stock confirmada, pedido CONFIRMADO"


class RegistradorDeHistorial(Manejador):
    """Paso posterior al pivote: no puede deshacer el pedido. Si el Servicio de Usuarios
    no responde, el historial queda desactualizado (consistencia eventual) pero el pedido sigue válido."""

    def __init__(self, usuarios: UsuariosClient):
        super().__init__()
        self._usuarios = usuarios

    def procesar(self, ctx: ContextoPedido) -> str:
        try:
            self._usuarios.registrar_compra(ctx.solicitud.usuario_id, ctx.pedido_id, ctx.total)
        except Exception as error:
            logger.warning("No se pudo registrar el historial del pedido %s: %s", ctx.pedido_id, error)
            return f"No crítico: no se pudo registrar en el historial ({error})"
        return "Compra registrada en el historial del usuario"


def construir_cadena(
    usuarios: UsuariosClient, catalogo: CatalogoClient, pasarela: PasarelaPagoSimulada
) -> Manejador:
    inicio = ValidadorDeDatos()
    (
        inicio.enlazar(ValidadorDeUsuario(usuarios))
        .enlazar(ValidadorDeStock(catalogo))
        .enlazar(ReservaDeStock(catalogo))
        .enlazar(ProcesadorDePago(pasarela))
        .enlazar(ConfirmadorDePedido(catalogo))
        .enlazar(RegistradorDeHistorial(usuarios))
    )
    return inicio
