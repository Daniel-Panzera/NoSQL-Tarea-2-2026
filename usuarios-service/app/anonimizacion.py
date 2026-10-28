import hashlib
import hmac

from .models import Usuario


def seudonimizar(valor: str, clave: bytes) -> str:
    """HMAC-SHA256: el mismo usuario da siempre el mismo seudónimo, pero sin la clave
    no se puede revertir ni recalcular a partir del id original."""
    return "anon_" + hmac.new(clave, valor.encode(), hashlib.sha256).hexdigest()[:16]


def enmascarar_email(email: str) -> str:
    local, dominio = email.split("@", 1)
    return f"{local[0]}***@{dominio}"


def anonimizar_usuario(usuario: Usuario, clave: bytes) -> dict:
    """Se eliminan documento, apellido, calle y código postal; el resto se seudonimiza,
    enmascara o generaliza (solo ciudad y fecha de alta sin hora)."""
    return {
        "id_seudonimo": seudonimizar(usuario.id, clave),
        "nombre_inicial": f"{usuario.nombre[0]}.",
        "email_enmascarado": enmascarar_email(usuario.email),
        "ciudades": sorted({d.ciudad for d in usuario.direcciones}),
        "fecha_alta": usuario.creado_en.date().isoformat(),
    }
