-- Esquema de Cassandra para ShopExpress (modelo de LECTURA: catálogo y usuarios).
-- Modelado orientado a consultas: una tabla por patrón de acceso, desnormalizando a propósito.

-- En la demo hay un solo nodo (RF=1). En producción: NetworkTopologyStrategy con RF=3
-- y lecturas con LOCAL_ONE para priorizar disponibilidad (AP, consistencia eventual).
CREATE KEYSPACE IF NOT EXISTS shopexpress
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE shopexpress;

-- ---------- Servicio de Usuarios ----------

-- Las direcciones se EMBEBEN en el usuario (UDT congelado): siempre se leen juntas
-- y pertenecen solo a ese usuario.
CREATE TYPE IF NOT EXISTS direccion (
  calle         text,
  ciudad        text,
  codigo_postal text
);

-- Consulta: GET /api/usuarios/{usuario_id}
CREATE TABLE IF NOT EXISTS usuarios_por_id (
  usuario_id     text PRIMARY KEY,
  tipo_documento text,
  nro_documento  text,
  nombre         text,
  apellido       text,
  email          text,
  direcciones    list<frozen<direccion>>,
  creado_en      timestamp
);

-- Garantiza documento único al registrar (INSERT ... IF NOT EXISTS, lightweight transaction).
CREATE TABLE IF NOT EXISTS usuarios_por_documento (
  tipo_documento text,
  nro_documento  text,
  usuario_id     text,
  PRIMARY KEY ((tipo_documento, nro_documento))
);

-- Historial de compras: se escribe cuando la SAGA confirma un pedido (consistencia eventual).
-- Referencia al pedido por id; el detalle completo vive en MongoDB (servicio de Pedidos).
CREATE TABLE IF NOT EXISTS compras_por_usuario (
  usuario_id text,
  fecha      timestamp,
  pedido_id  text,
  total      decimal,
  estado     text,
  PRIMARY KEY ((usuario_id), fecha, pedido_id)
) WITH CLUSTERING ORDER BY (fecha DESC, pedido_id ASC);

-- ---------- Servicio de Catálogo e Inventario ----------

-- Consulta: GET /api/productos/{producto_id}
-- stock_disponible es una COPIA de lectura; la fuente de verdad del stock está en MongoDB.
CREATE TABLE IF NOT EXISTS productos_por_id (
  producto_id      text PRIMARY KEY,
  nombre           text,
  descripcion      text,
  categoria        text,
  precio           decimal,
  stock_disponible int,
  actualizado_en   timestamp
);

-- Consulta: listar productos de una categoría (desnormalizada).
CREATE TABLE IF NOT EXISTS productos_por_categoria (
  categoria   text,
  producto_id text,
  nombre      text,
  precio      decimal,
  PRIMARY KEY ((categoria), producto_id)
);
