from pymongo import MongoClient, monitoring
from pymongo.errors import PyMongoError

from app.metrics import MONGO_POOL_IN_USE
from app.observability import guarded


class PoolUsage(monitoring.ConnectionPoolListener):
    """Cuenta las conexiones del pool prestadas a una operacion."""

    def connection_checked_out(self, event):
        MONGO_POOL_IN_USE.inc()

    def connection_checked_in(self, event):
        MONGO_POOL_IN_USE.dec()

    def pool_created(self, event): pass
    def pool_ready(self, event): pass
    def pool_cleared(self, event): pass
    def pool_closed(self, event): pass
    def connection_created(self, event): pass
    def connection_ready(self, event): pass
    def connection_closed(self, event): pass
    def connection_check_out_started(self, event): pass
    def connection_check_out_failed(self, event): pass


class MongoAdapter:
    def __init__(self, settings):
        ms = int(settings.timeout_seconds*1000)
        self.client = MongoClient(settings.mongo_uri.get_secret_value(), connect=False,
                                  serverSelectionTimeoutMS=ms, connectTimeoutMS=ms,
                                  socketTimeoutMS=ms, timeoutMS=ms, event_listeners=[PoolUsage()])
        self.collection = self.client[settings.mongo_database].equipos
        self.projection = dict.fromkeys(["codigo_fifa", "nombre", "confederacion", "entrenador", "ranking_fifa"], 1)

    @staticmethod
    def transform(document):
        return {"id": str(document.pop("_id")), **document}

    @guarded("mongodb", PyMongoError)
    def get_team(self, code):
        doc = self.collection.find_one({"codigo_fifa": code}, self.projection)
        return self.transform(doc) if doc else None

    @guarded("mongodb", PyMongoError)
    def list_teams(self, after, limit, confederation):
        filters = {"codigo_fifa": {"$gt": after}} if after else {}
        if confederation:
            filters["confederacion"] = confederation
        return [self.transform(doc) for doc in self.collection.find(filters, self.projection)
                .sort("codigo_fifa", 1).limit(limit+1)]

    @guarded("mongodb", PyMongoError)
    def ping(self):
        self.client.admin.command("ping")

    def server_status(self):
        """Conexiones y opcounters del servidor, para /metrics. Sin datos de negocio."""
        status = self.client.admin.command("serverStatus", repl=0, metrics=0, locks=0)
        return {"conexiones": status["connections"]["current"], "operaciones": dict(status["opcounters"])}

    def close(self):
        self.client.close()
