import uuid
from collections.abc import Iterator
from datetime import datetime, timezone

from cassandra import ConsistencyLevel
from cassandra.cluster import Session

from .models import Compra, CompraRegistrar, Direccion, Usuario, UsuarioCrear

HISTORIAL_MAXIMO = 50


class DireccionUDT:
    """Clase registrada en el driver para mapear el UDT 'direccion' de Cassandra."""

    def __init__(self, calle: str | None = None, ciudad: str | None = None, codigo_postal: str | None = None):
        self.calle = calle
        self.ciudad = ciudad
        self.codigo_postal = codigo_postal


class DocumentoDuplicadoError(Exception):
    pass


def _a_utc(fecha: datetime) -> datetime:
    """Cassandra guarda timestamps en UTC pero el driver los devuelve sin zona horaria."""
    return fecha.replace(tzinfo=timezone.utc) if fecha.tzinfo is None else fecha


class UsuarioRepository:
    def __init__(self, session: Session, keyspace: str):
        self._session = session
        session.cluster.register_user_type(keyspace, "direccion", DireccionUDT)
        ks = keyspace

        # Unicidad del documento con lightweight transaction (Paxos): única operación
        # de este servicio donde se prioriza la consistencia sobre la latencia.
        self._reservar_documento = session.prepare(
            f"INSERT INTO {ks}.usuarios_por_documento (tipo_documento, nro_documento, usuario_id) "
            "VALUES (?, ?, ?) IF NOT EXISTS"
        )
        self._reservar_documento.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL

        self._liberar_documento = session.prepare(
            f"DELETE FROM {ks}.usuarios_por_documento WHERE tipo_documento = ? AND nro_documento = ?"
        )

        self._insertar_usuario = session.prepare(
            f"INSERT INTO {ks}.usuarios_por_id "
            "(usuario_id, tipo_documento, nro_documento, nombre, apellido, email, direcciones, creado_en) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )

        self._obtener_usuario = session.prepare(f"SELECT * FROM {ks}.usuarios_por_id WHERE usuario_id = ?")
        self._obtener_usuario.consistency_level = ConsistencyLevel.LOCAL_ONE

        self._listar_usuarios = session.prepare(f"SELECT * FROM {ks}.usuarios_por_id")
        self._listar_usuarios.consistency_level = ConsistencyLevel.LOCAL_ONE
        self._listar_usuarios.fetch_size = 500

        self._insertar_compra = session.prepare(
            f"INSERT INTO {ks}.compras_por_usuario (usuario_id, fecha, pedido_id, total, estado) "
            "VALUES (?, ?, ?, ?, ?)"
        )

        self._obtener_historial = session.prepare(
            f"SELECT * FROM {ks}.compras_por_usuario WHERE usuario_id = ? LIMIT {HISTORIAL_MAXIMO}"
        )
        self._obtener_historial.consistency_level = ConsistencyLevel.LOCAL_ONE

    def crear(self, datos: UsuarioCrear) -> Usuario:
        usuario_id = f"usr_{uuid.uuid4().hex[:12]}"

        resultado = self._session.execute(
            self._reservar_documento, (datos.tipo_documento, datos.nro_documento, usuario_id)
        )
        if not resultado.was_applied:
            raise DocumentoDuplicadoError(f"Ya existe un usuario con {datos.tipo_documento} {datos.nro_documento}")

        usuario = Usuario(id=usuario_id, creado_en=datetime.now(timezone.utc), **datos.model_dump())
        direcciones = [DireccionUDT(**d.model_dump()) for d in usuario.direcciones]
        try:
            self._session.execute(
                self._insertar_usuario,
                (
                    usuario.id,
                    usuario.tipo_documento,
                    usuario.nro_documento,
                    usuario.nombre,
                    usuario.apellido,
                    usuario.email,
                    direcciones,
                    usuario.creado_en,
                ),
            )
        except Exception:
            # Si falla la escritura del perfil, se libera el documento reservado
            self._session.execute(self._liberar_documento, (datos.tipo_documento, datos.nro_documento))
            raise

        return usuario

    def obtener(self, usuario_id: str) -> Usuario | None:
        fila = self._session.execute(self._obtener_usuario, (usuario_id,)).one()
        return self._fila_a_usuario(fila) if fila else None

    def listar_todos(self) -> Iterator[Usuario]:
        # Recorre la tabla paginando (fetch_size); solo se usa para la exportación
        for fila in self._session.execute(self._listar_usuarios):
            yield self._fila_a_usuario(fila)

    def registrar_compra(self, usuario_id: str, datos: CompraRegistrar) -> Compra:
        compra = Compra(
            pedido_id=datos.pedido_id,
            fecha=datos.fecha or datetime.now(timezone.utc),
            total=datos.total,
            estado=datos.estado,
        )
        self._session.execute(
            self._insertar_compra, (usuario_id, compra.fecha, compra.pedido_id, compra.total, compra.estado)
        )
        return compra

    def historial(self, usuario_id: str) -> list[Compra]:
        filas = self._session.execute(self._obtener_historial, (usuario_id,))
        return [
            Compra(pedido_id=f.pedido_id, fecha=_a_utc(f.fecha), total=f.total, estado=f.estado)
            for f in filas
        ]

    @staticmethod
    def _fila_a_usuario(fila) -> Usuario:
        return Usuario(
            id=fila.usuario_id,
            tipo_documento=fila.tipo_documento,
            nro_documento=fila.nro_documento,
            nombre=fila.nombre,
            apellido=fila.apellido,
            email=fila.email,
            direcciones=[
                Direccion(calle=d.calle, ciudad=d.ciudad, codigo_postal=d.codigo_postal)
                for d in (fila.direcciones or [])
            ],
            creado_en=_a_utc(fila.creado_en),
        )
