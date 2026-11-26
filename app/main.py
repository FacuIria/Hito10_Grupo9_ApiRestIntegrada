import asyncio
from contextlib import asynccontextmanager
import logging
from pathlib import Path
from time import perf_counter
from uuid import UUID, uuid4

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, FileResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException

from app.config.settings import Settings
from app.errors import ApiError
from app.metrics import DEPENDENCY_UP, HTTP_LATENCY, MONGO_OPERATIONS, MONGO_SERVER_CONNECTIONS
from app.observability import configure_loki, emit, request_id
from app.routes.api import router
from app.schemas.models import ErrorResponse
from app.services.fixture import FixtureService


METRICS_CHECK_TIMEOUT = 3


def build_adapters(settings):
    from app.adapters.mongo import MongoAdapter
    from app.adapters.neo4j import Neo4jAdapter
    from app.adapters.cassandra import CassandraAdapter
    from app.adapters.redis import RedisAdapter
    from app.adapters.influx import InfluxAdapter
    from app.adapters.iris import IrisAdapter
    return {"mongodb": MongoAdapter(settings), "neo4j": Neo4jAdapter(settings),
            "cassandra": CassandraAdapter(settings), "redis": RedisAdapter(settings),
            "influxdb": InfluxAdapter(settings), "iris": IrisAdapter(settings)}


def create_app(settings=None, adapters=None):
    settings = settings or Settings()
    configure_loki(settings.loki_url)

    @asynccontextmanager
    async def lifespan(app):
        clients = adapters if adapters is not None else build_adapters(settings)
        app.state.service = FixtureService(clients, settings)
        yield
        for name, client in clients.items():
            try:
                client.close()
            except Exception as exc:
                emit(operation="close", dependency=name, error_type=type(exc).__name__)

    app = FastAPI(title="Fixture 2030 - API integrada Grupo 9", version="1.0.0",
        description="Acceso por casos de uso a seis fuentes NoSQL. API local de laboratorio: lecturas en las seis fuentes y alta de comentarios en Cassandra.",
        lifespan=lifespan, servers=[{"url": "/", "description": "Instancia local actual"}])

    def error_response(request, status, code, message, dependency=None, fields=None):
        rid = getattr(request.state, "request_id", request_id.get())
        return JSONResponse(status_code=status, headers={"X-Request-ID": rid}, content={"error": {
            "code": code, "message": message, "request_id": rid,
            "dependency": dependency, "fields": fields or []}})

    @app.exception_handler(ApiError)
    async def api_error(request, exc):
        return error_response(request, exc.status, exc.code, exc.message, exc.dependency)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        fields = [".".join(map(str, error["loc"])) for error in exc.errors()]
        return error_response(request, 422, "entrada_invalida", "Revise los parametros indicados en el contrato.", fields=fields)

    @app.exception_handler(HTTPException)
    async def http_error(request, exc):
        return error_response(request, exc.status_code, "error_http", "Ruta o metodo HTTP no disponible.")

    @app.middleware("http")
    async def correlate(request: Request, call_next):
        start = perf_counter()
        try:
            rid = str(UUID(request.headers.get("X-Request-ID", "")))
        except ValueError:
            rid = str(uuid4())
        request.state.request_id = rid
        context_token = request_id.set(rid)
        try:
            try:
                response = await call_next(request)
            except Exception as exc:
                emit(result="internal_error", error_type=type(exc).__name__)
                response = error_response(request, 500, "error_interno", "Ocurrio un error interno.")
            response.headers["X-Request-ID"] = rid
            response.headers["Cache-Control"] = "no-store"
            route = request.scope.get("route")
            path = getattr(route, "path", "unmatched")
            elapsed = perf_counter()-start
            if path != "/metrics":
                HTTP_LATENCY.labels(request.method, path, str(response.status_code)).observe(elapsed)
            emit(method=request.method, route=path,
                 status=response.status_code, duration_ms=round(elapsed*1000, 2))
            return response
        finally:
            request_id.reset(context_token)

    app.include_router(router)
    frontend = Path(__file__).resolve().parent / "frontend"
    app.mount("/ui", StaticFiles(directory=frontend), name="ui")

    @app.get("/", include_in_schema=False)
    def dashboard():
        return FileResponse(frontend / "index.html")

    @app.get("/metrics", include_in_schema=False)
    async def metrics(request: Request):
        # Ping real a las seis bases, en paralelo y con tope de 3 s: Prometheus corta a los 5 s.
        svc = request.app.state.service
        sources = list(svc.adapters)
        tasks = {source: asyncio.ensure_future(run_in_threadpool(svc.check, source)) for source in sources}
        mongo_status = asyncio.ensure_future(run_in_threadpool(svc.adapters["mongodb"].server_status))
        await asyncio.wait([*tasks.values(), mongo_status], timeout=METRICS_CHECK_TIMEOUT)
        try:
            status = mongo_status.result()
            MONGO_SERVER_CONNECTIONS.set(float(status["conexiones"]))
            for kind, value in status["operaciones"].items():
                MONGO_OPERATIONS.labels(kind).set(float(value))
        except Exception:
            pass  # MongoDB no respondio a tiempo: se conservan los ultimos valores
        for source, task in tasks.items():
            ok = task.done() and not task.exception() and task.result()["estado"] == "ok"
            DEPENDENCY_UP.labels(source).set(1 if ok else 0)
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/health/live", tags=["Operacion"], summary="Verificar el proceso HTTP")
    def live() -> dict[str, str]:
        return {"estado": "ok"}

    original_openapi = app.openapi

    def openapi():
        schema = original_openapi()
        for path in schema["paths"].values():
            for operation in path.values():
                if not isinstance(operation, dict) or "responses" not in operation:
                    continue
                for response in operation["responses"].values():
                    response.setdefault("headers", {})["X-Request-ID"] = {
                        "description": "UUID de correlacion de la request y sus logs.",
                        "schema": {"type": "string", "format": "uuid"}}
        return schema

    app.openapi = openapi
    return app


logging.basicConfig(level=logging.INFO, format="%(message)s")
for library in ("httpx", "httpcore", "neo4j", "cassandra", "influxdb_client", "urllib3", "pymongo"):
    logging.getLogger(library).setLevel(logging.CRITICAL)
app = create_app()
