// Inicializa el replica set "rs0" de un solo nodo si todavía no existe.
// Se ejecuta como healthcheck del contenedor: devuelve 0 recién cuando el nodo es PRIMARY.
// El replica set es necesario para transacciones y para writeConcern/readConcern "majority".
try {
  rs.status();
} catch (e) {
  rs.initiate({ _id: "rs0", members: [{ _id: 0, host: "mongo:27017" }] });
}

quit(db.hello().isWritablePrimary ? 0 : 1);
