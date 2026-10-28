import logging
import os

from pymongo import MongoClient
from pymongo.database import Database

logger = logging.getLogger(__name__)


def conectar_mongo() -> tuple[MongoClient, Database]:
    """Cliente con w=majority y readConcern=majority: una escritura confirmada sobrevive
    a la caída del primario y nunca se leen datos que puedan revertirse (prioridad: consistencia)."""
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/?replicaSet=rs0")
    nombre_db = os.getenv("MONGO_DB", "pedidos")

    cliente = MongoClient(uri, w="majority", readConcernLevel="majority", serverSelectionTimeoutMS=30000)
    cliente.admin.command("ping")
    logger.info("Conectado a MongoDB, base %s", nombre_db)
    return cliente, cliente[nombre_db]
