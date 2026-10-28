from decimal import Decimal

import httpx


class ServicioNoDisponibleError(Exception):
    """Error técnico (timeout, conexión, 5xx): dispara compensaciones en la SAGA."""


class _ClienteHttp:
    def __init__(self, nombre: str, base_url: str):
        self._nombre = nombre
        self._http = httpx.Client(base_url=base_url, timeout=5.0)

    def _solicitar(self, metodo: str, ruta: str, **kwargs) -> httpx.Response:
        try:
            respuesta = self._http.request(metodo, ruta, **kwargs)
        except httpx.TransportError as error:
            raise ServicioNoDisponibleError(f"{self._nombre} no responde: {error}") from error
        if respuesta.status_code >= 500:
            raise ServicioNoDisponibleError(f"{self._nombre} respondió {respuesta.status_code}")
        return respuesta

    def cerrar(self) -> None:
        self._http.close()


class UsuariosClient(_ClienteHttp):
    def __init__(self, base_url: str):
        super().__init__("Servicio de Usuarios", base_url)

    def obtener_usuario(self, usuario_id: str) -> dict | None:
        respuesta = self._solicitar("GET", f"/api/usuarios/{usuario_id}")
        if respuesta.status_code == 404:
            return None
        respuesta.raise_for_status()
        return respuesta.json()

    def registrar_compra(self, usuario_id: str, pedido_id: str, total: Decimal) -> None:
        respuesta = self._solicitar(
            "POST",
            f"/api/usuarios/{usuario_id}/compras",
            json={"pedido_id": pedido_id, "total": str(total), "estado": "CONFIRMADO"},
        )
        respuesta.raise_for_status()


class CatalogoClient(_ClienteHttp):
    def __init__(self, base_url: str):
        super().__init__("Servicio de Catálogo e Inventario", base_url)

    def obtener_producto(self, producto_id: str) -> dict | None:
        # consistente=true: el stock se lee de MongoDB, no de la copia eventual en Cassandra
        respuesta = self._solicitar("GET", f"/api/productos/{producto_id}", params={"consistente": "true"})
        if respuesta.status_code == 404:
            return None
        respuesta.raise_for_status()
        return respuesta.json()

    def reservar(self, pedido_id: str, items: list[dict]) -> httpx.Response:
        """Devuelve la respuesta cruda: 201/200 reservado, 409 sin stock, 404 producto inexistente."""
        return self._solicitar("POST", "/api/inventario/reservas", json={"pedido_id": pedido_id, "items": items})

    def confirmar(self, pedido_id: str) -> None:
        respuesta = self._solicitar("POST", f"/api/inventario/reservas/{pedido_id}/confirmar")
        respuesta.raise_for_status()

    def liberar(self, pedido_id: str) -> None:
        respuesta = self._solicitar("DELETE", f"/api/inventario/reservas/{pedido_id}")
        # 404: nunca se llegó a reservar, no hay nada que devolver
        if respuesta.status_code != 404:
            respuesta.raise_for_status()
