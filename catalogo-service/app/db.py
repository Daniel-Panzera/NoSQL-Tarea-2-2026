import logging
import os
import time

from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, NoHostAvailable, Session
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from pymongo import MongoClient
from pymongo.database import Database

logger = logging.getLogger(__name__)


def conectar_cassandra() -> tuple[Cluster, Session]:
    """Conecta al cluster con reintentos, por si Cassandra todavía está arrancando.
    La sesión no fija keyspace: las consultas usan nombres calificados (keyspace.tabla)."""
    hosts = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
    datacenter = os.getenv("CASSANDRA_DC", "datacenter1")

    # Por defecto las escrituras usan LOCAL_QUORUM; las lecturas bajan a LOCAL_ONE
    # en cada sentencia preparada (prioridad: disponibilidad y latencia).
    # TokenAware envía cada consulta directo a un nodo réplica de la partición.
    perfil = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=datacenter)),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
    )

    for intento in range(1, 31):
        try:
            cluster = Cluster(
                contact_points=hosts,
                protocol_version=5,
                execution_profiles={EXEC_PROFILE_DEFAULT: perfil},
            )
            session = cluster.connect()
            logger.info("Conectado a Cassandra (%s)", hosts)
            return cluster, session
        except NoHostAvailable:
            logger.warning("Cassandra no disponible (intento %d/30), reintentando...", intento)
            time.sleep(2)

    raise RuntimeError("No se pudo conectar a Cassandra")


def conectar_mongo() -> tuple[MongoClient, Database]:
    """Cliente con w=majority y readConcern=majority: una escritura confirmada sobrevive
    a la caída del primario y nunca se leen datos que puedan revertirse (prioridad: consistencia)."""
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0")
    nombre_db = os.getenv("MONGO_DB", "catalogo_inventario")

    cliente = MongoClient(uri, w="majority", readConcernLevel="majority", serverSelectionTimeoutMS=30000)
    cliente.admin.command("ping")
    logger.info("Conectado a MongoDB, base %s", nombre_db)
    return cliente, cliente[nombre_db]
