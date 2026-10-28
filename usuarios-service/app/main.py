import csv
import io
import logging
import os
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status

from .anonimizacion import anonimizar_usuario
from .db import conectar_cassandra
from .models import Compra, CompraRegistrar, Usuario, UsuarioCrear, UsuarioDetalle
from .repository import DocumentoDuplicadoError, UsuarioRepository

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cluster, session = conectar_cassandra()
    app.state.repo = UsuarioRepository(session, os.getenv("CASSANDRA_KEYSPACE", "shopexpress"))
    app.state.clave_seudonimo = os.getenv("PSEUDONIMO_CLAVE", "clave-de-desarrollo").encode()
    yield
    cluster.shutdown()


app = FastAPI(
    title="ShopExpress - Servicio de Usuarios",
    description="Perfiles, direcciones e historial de compras. Persistencia en Cassandra (AP).",
    lifespan=lifespan,
)


def obtener_repo(request: Request) -> UsuarioRepository:
    return request.app.state.repo


# Los endpoints son sincrónicos (def) porque el driver de Cassandra es bloqueante:
# FastAPI los ejecuta en un pool de hilos sin trabar el event loop.


@app.get("/health", include_in_schema=False)
def health():
    return {"estado": "ok"}


@app.post("/api/usuarios", response_model=Usuario, status_code=status.HTTP_201_CREATED)
def crear_usuario(datos: UsuarioCrear, repo: UsuarioRepository = Depends(obtener_repo)):
    try:
        return repo.crear(datos)
    except DocumentoDuplicadoError as error:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error))


# Debe declararse antes de /{usuario_id} para que "exportar" no se tome como un id
@app.get("/api/usuarios/exportar")
def exportar_usuarios(
    request: Request,
    formato: Literal["json", "csv"] = "json",
    repo: UsuarioRepository = Depends(obtener_repo),
):
    clave = request.app.state.clave_seudonimo
    registros = [anonimizar_usuario(u, clave) for u in repo.listar_todos()]

    if formato == "json":
        return registros

    salida = io.StringIO()
    escritor = csv.DictWriter(
        salida, fieldnames=["id_seudonimo", "nombre_inicial", "email_enmascarado", "ciudades", "fecha_alta"]
    )
    escritor.writeheader()
    for registro in registros:
        escritor.writerow({**registro, "ciudades": ";".join(registro["ciudades"])})

    return Response(
        content=salida.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="usuarios_anonimizados.csv"'},
    )


@app.get("/api/usuarios/{usuario_id}", response_model=UsuarioDetalle)
def obtener_usuario(usuario_id: str, repo: UsuarioRepository = Depends(obtener_repo)):
    usuario = repo.obtener(usuario_id)
    if usuario is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario no encontrado")
    return UsuarioDetalle(**usuario.model_dump(), historial_compras=repo.historial(usuario_id))


@app.post(
    "/api/usuarios/{usuario_id}/compras",
    response_model=Compra,
    status_code=status.HTTP_201_CREATED,
    summary="Registrar compra en el historial (uso interno del orquestador SAGA)",
)
def registrar_compra(usuario_id: str, datos: CompraRegistrar, repo: UsuarioRepository = Depends(obtener_repo)):
    if repo.obtener(usuario_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario no encontrado")
    return repo.registrar_compra(usuario_id, datos)
