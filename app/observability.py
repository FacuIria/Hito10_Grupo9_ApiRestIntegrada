from contextvars import ContextVar
from functools import wraps
import json
import logging
import queue
import threading
import time
import urllib.request
from time import perf_counter

from app.errors import ApiError, unavailable
from app.metrics import DEPENDENCY_IN_FLIGHT, DEPENDENCY_LATENCY

request_id = ContextVar("request_id", default="startup")
logger = logging.getLogger("fixture.api")


class LokiShipper:
    """Envia los logs a Loki en segundo plano. Nunca bloquea ni falla una request:
    si Loki no responde o la cola se llena, las lineas se descartan."""

    def __init__(self, url):
        self.url = url.rstrip("/") + "/loki/api/v1/push"
        self.lines = queue.Queue(maxsize=10000)
        threading.Thread(target=self.run, name="loki-shipper", daemon=True).start()

    def put(self, line):
        try:
            self.lines.put_nowait([str(time.time_ns()), line])
        except queue.Full:
            pass

    def run(self):
        while True:
            batch = [self.lines.get()]
            time.sleep(0.5)
            while len(batch) < 500 and not self.lines.empty():
                batch.append(self.lines.get_nowait())
            body = json.dumps({"streams": [{"stream": {"app": "fixture2030-api", "hito": "10"}, "values": batch}]})
            request = urllib.request.Request(self.url, data=body.encode(), method="POST",
                                             headers={"Content-Type": "application/json"})
            try:
                urllib.request.urlopen(request, timeout=3).close()
            except Exception:
                pass


_shipper = None


def configure_loki(url):
    global _shipper
    if url and _shipper is None:
        _shipper = LokiShipper(url)


def emit(**fields):
    line = json.dumps({"request_id": request_id.get(), **fields}, ensure_ascii=True)
    logger.info(line)
    if _shipper is not None:
        _shipper.put(line)


def guarded(source, exceptions):
    def decorate(method):
        @wraps(method)
        def wrapped(*args, **kwargs):
            start = perf_counter()
            result = "ok"
            error_type = None
            DEPENDENCY_IN_FLIGHT.labels(source).inc()
            try:
                return method(*args, **kwargs)
            except ApiError as exc:
                result = exc.code
                raise
            except exceptions as exc:
                result = "dependency_error"
                error_type = type(exc).__name__
                raise unavailable(source) from None
            except Exception as exc:
                result = "internal_error"
                error_type = type(exc).__name__
                raise
            finally:
                elapsed = perf_counter()-start
                DEPENDENCY_IN_FLIGHT.labels(source).dec()
                DEPENDENCY_LATENCY.labels(source, method.__name__, result).observe(elapsed)
                emit(dependency=source, operation=method.__name__, result=result,
                     error_type=error_type, duration_ms=round(elapsed*1000, 2))
        return wrapped
    return decorate
