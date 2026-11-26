"""Metricas Prometheus del Hito 10. Sin IDs, contenido de comentarios ni datos personales."""
from prometheus_client import Gauge, Histogram

BUCKETS = (.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5)

HTTP_LATENCY = Histogram("fixture2030_http_request_duration_seconds",
                         "Tiempo de respuesta HTTP de la API, excluye /metrics",
                         ["method", "route", "status"], buckets=BUCKETS)
COMMENT_WRITE = Histogram("fixture2030_comentario_escritura_segundos",
                          "Duracion del LOGGED BATCH que publica un comentario en Cassandra",
                          ["result"], buckets=BUCKETS)
DEPENDENCY_UP = Gauge("fixture2030_dependencia_disponible",
                      "1 si la API alcanza la base con un ping real al recolectar metricas, 0 si no",
                      ["dependencia"])
DEPENDENCY_LATENCY = Histogram("fixture2030_dependencia_duracion_segundos",
                               "Duracion de cada operacion de la API contra una base, incluye fallos",
                               ["dependencia", "operacion", "resultado"], buckets=BUCKETS)
DEPENDENCY_IN_FLIGHT = Gauge("fixture2030_dependencia_en_curso",
                             "Operaciones de la API contra cada base que todavia no terminaron",
                             ["dependencia"])
MONGO_POOL_IN_USE = Gauge("fixture2030_mongodb_pool_en_uso",
                          "Conexiones del pool de pymongo prestadas a una operacion en este momento")
MONGO_SERVER_CONNECTIONS = Gauge("fixture2030_mongodb_conexiones_servidor",
                                 "Conexiones abiertas segun serverStatus de MongoDB (todos los clientes)")
MONGO_OPERATIONS = Gauge("fixture2030_mongodb_operaciones_acumuladas",
                         "opcounters de serverStatus: operaciones desde que arranco MongoDB; usar rate()",
                         ["tipo"])
