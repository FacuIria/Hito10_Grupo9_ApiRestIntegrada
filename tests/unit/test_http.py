import json
from uuid import UUID

from jsonschema import Draft202012Validator
import pytest

from app.errors import unavailable

COMMENTS = "/api/v1/partidos/P001/comentarios?bucket_10m=2030-06-10T18:00:00Z&shard=0"
STATS = "/api/v1/partidos/P001/estadisticas?desde=2030-06-08T19:00:00Z&hasta=2030-06-08T20:30:00Z"
ENDPOINTS = [
    ("/api/v1/equipos", "mongodb", "list_teams"),
    ("/api/v1/equipos/AAA", "mongodb", "get_team"),
    ("/api/v1/partidos/P001/eventos", "neo4j", "events"),
    (COMMENTS, "cassandra", "comments"),
    ("/api/v1/sesiones/sess-demo-001", "redis", "session"),
    (STATS, "influxdb", "statistics"),
    ("/api/v1/objetos/partidos/P001", "iris", "match"),
]


@pytest.mark.parametrize("url,source,method", ENDPOINTS)
def test_success_and_openapi_response(client, adapters, url, source, method):
    response = client.get(url)
    assert response.status_code == 200, response.text
    getattr(adapters[source], method).assert_called_once()
    UUID(response.headers["X-Request-ID"])
    route = next(route for route in client.app.routes
                 if hasattr(route, "path_regex") and route.path_regex.fullmatch(url.split("?")[0]))
    spec = client.app.openapi()
    schema = spec["paths"][route.path]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
    Draft202012Validator({**schema, "components": spec["components"]}).validate(response.json())


@pytest.mark.parametrize("url,source,method", ENDPOINTS)
def test_unavailable_is_never_empty_success(client, adapters, url, source, method):
    getattr(adapters[source], method).side_effect = unavailable(source)
    response = client.get(url)
    assert response.status_code == 503
    error = response.json()["error"]
    assert error["dependency"] == source
    assert error["request_id"] == response.headers["X-Request-ID"]


@pytest.mark.parametrize("url", [
    "/api/v1/equipos/aa", "/api/v1/equipos?limite=0", "/api/v1/equipos?limite=101",
    "/api/v1/equipos?confederacion=INVALID", "/api/v1/partidos/P001/eventos?offset=10001",
    COMMENTS.replace("18:00", "18:03"), COMMENTS.replace("shard=0", "shard=8"),
    COMMENTS.replace("18:00:00Z", "18:00:00"), COMMENTS+"&cursor=corrupto",
    STATS.replace("20:30:00Z", "18:00:00Z"), STATS.replace("2030-06-08T20", "2030-06-10T20"),
    STATS.replace("19:00:00Z", "19:00:00"), STATS+"&resolucion=1s",
    STATS+"&equipo=AAA%22", STATS+"&consulta=libre",
    "/api/v1/objetos/partidos/invalid", "/api/v1/partidos/P001/comentarios",
])
def test_invalid_never_touches_database(client, adapters, url):
    response = client.get(url)
    assert response.status_code == 422, response.text
    assert "error" in response.json()
    for adapter in adapters.values():
        assert adapter.mock_calls == []


@pytest.mark.parametrize("url,source,method", [ENDPOINTS[1], ENDPOINTS[2], ENDPOINTS[4], ENDPOINTS[6]])
def test_missing_is_404(client, adapters, url, source, method):
    getattr(adapters[source], method).return_value = None
    assert client.get(url).status_code == 404


def test_safe_internal_error(client, adapters):
    adapters["mongodb"].get_team.side_effect = RuntimeError("password=SECRET mongodb://sensitive")
    response = client.get("/api/v1/equipos/AAA")
    assert response.status_code == 500
    assert "SECRET" not in response.text
    assert "mongodb://" not in response.text


def test_request_id_and_logs_do_not_include_session_id(client, caplog):
    import logging
    rid = "bc867f9c-bc84-456c-b333-f90e4e75d2e6"
    with caplog.at_level(logging.INFO, logger="fixture.api"):
        response = client.get("/api/v1/sesiones/private-session-id", headers={"X-Request-ID": rid})
    assert response.headers["X-Request-ID"] == rid
    logs = [record.message for record in caplog.records if record.name == "fixture.api"]
    assert any(rid in message for message in logs)
    assert all("private-session-id" not in message for message in logs)


def test_readiness_degraded_and_liveness(client, adapters):
    adapters["redis"].ping.side_effect = unavailable("redis")
    response = client.get("/api/v1/salud")
    assert response.status_code == 503
    assert response.json()["data"]["dependencias"]["redis"]["estado"] == "no_disponible"
    assert client.get("/health/live").status_code == 200


def test_cursor_bound_to_partition_and_page_size(client, adapters):
    adapters["cassandra"].comments.return_value = ([], b"next-page")
    token = client.get(COMMENTS).json()["data"]["siguiente_cursor"]
    adapters["cassandra"].reset_mock()
    response = client.get(COMMENTS+"&cursor="+token)
    assert response.status_code == 200
    assert adapters["cassandra"].comments.call_args.args[-1] == b"next-page"
    adapters["cassandra"].reset_mock()
    assert client.get(COMMENTS+"&limite=30&cursor="+token).status_code == 422
    assert not adapters["cassandra"].comments.called


def test_mongo_keyset_page(client, adapters):
    base = adapters["mongodb"].get_team.return_value
    adapters["mongodb"].list_teams.return_value = [base, {**base, "codigo_fifa": "AAB"}]
    data = client.get("/api/v1/equipos?limite=1").json()["data"]
    assert len(data["items"]) == 1
    assert data["siguiente_codigo"] == "AAA"


def test_unknown_route_and_method_have_common_error(client):
    for response in [client.get("/not-found"), client.post("/api/v1/equipos", json={})]:
        assert response.status_code in [404, 405]
        assert response.json()["error"]["request_id"] == response.headers["X-Request-ID"]


def test_post_comment_writes_partition_and_shard(client, adapters):
    response = client.post("/api/v1/partidos/P001/comentarios", json={"usuario_id": "USR002", "contenido": "Golazo"})
    assert response.status_code == 201, response.text
    data = response.json()["data"]
    stored = adapters["cassandra"].add_comment.call_args.args[0]
    assert UUID(data["comentario_id"]) == stored["comentario_id"]
    assert data["shard"] == stored["comentario_id"].int % 8
    assert stored["bucket_10m"].minute % 10 == 0 and stored["bucket_10m"].second == 0
    assert stored["bucket_10m"] <= stored["creado_en"] and stored["mes_bucket"] == stored["creado_en"].strftime("%Y-%m")


@pytest.mark.parametrize("url,body", [
    ("/api/v1/partidos/P001/comentarios", {"usuario_id": "USR002", "contenido": ""}),
    ("/api/v1/partidos/P001/comentarios", {"usuario_id": "USR 002", "contenido": "x"}),
    ("/api/v1/partidos/P001/comentarios", {"usuario_id": "USR002", "contenido": "x"*501}),
    ("/api/v1/partidos/P001/comentarios", {"usuario_id": "USR002", "contenido": "x", "shard": 3}),
    ("/api/v1/partidos/invalido/comentarios", {"usuario_id": "USR002", "contenido": "x"}),
])
def test_post_comment_invalid_never_writes(client, adapters, url, body):
    response = client.post(url, json=body)
    assert response.status_code == 422
    assert not adapters["cassandra"].add_comment.called


def test_post_comment_unavailable_is_503(client, adapters):
    adapters["cassandra"].add_comment.side_effect = unavailable("cassandra")
    response = client.post("/api/v1/partidos/P001/comentarios", json={"usuario_id": "USR002", "contenido": "x"})
    assert response.status_code == 503
    assert response.json()["error"]["dependency"] == "cassandra"


def test_metrics_measure_comment_latency_without_private_data(client, adapters):
    client.post("/api/v1/partidos/P001/comentarios", json={"usuario_id": "USR-PRIVADO", "contenido": "texto privado"})
    body = client.get("/metrics").text
    assert 'fixture2030_comentario_escritura_segundos_count{result="ok"}' in body
    assert 'route="/api/v1/partidos/{partido_id}/comentarios"' in body
    assert "USR-PRIVADO" not in body and "texto privado" not in body
    assert 'route="/metrics"' not in body


def test_create_session_sets_ttl_and_hides_user(client, adapters):
    response = client.post("/api/v1/sesiones", json={"usuario_id": "USR000002", "origen": "app"})
    assert response.status_code == 201, response.text
    data = response.json()["data"]
    session_id, fields, ttl = adapters["redis"].create_session.call_args.args
    assert data["session_id"] == session_id and session_id.startswith("sess-")
    assert data["ttl_segundos"] == ttl == 1800 and data["estado"] == fields["estado"] == "activa"
    assert fields["usuario_id"] == "USR000002" and "USR000002" not in response.text


@pytest.mark.parametrize("body", [{"usuario_id": "USR 2"}, {"usuario_id": "USR2", "origen": "tv"}, {}])
def test_create_session_invalid_never_writes(client, adapters, body):
    assert client.post("/api/v1/sesiones", json=body).status_code == 422
    assert not adapters["redis"].create_session.called


def test_create_session_unavailable_is_503(client, adapters):
    adapters["redis"].create_session.side_effect = unavailable("redis")
    assert client.post("/api/v1/sesiones", json={"usuario_id": "USR2"}).status_code == 503


def test_metrics_report_each_dependency_up_or_down(client, adapters):
    adapters["redis"].ping.side_effect = unavailable("redis")
    body = client.get("/metrics").text
    for source in ["mongodb", "neo4j", "cassandra", "influxdb", "iris"]:
        assert f'fixture2030_dependencia_disponible{{dependencia="{source}"}} 1.0' in body
    assert 'fixture2030_dependencia_disponible{dependencia="redis"} 0.0' in body


def test_loki_shipper_never_blocks_when_loki_is_down():
    from app.observability import LokiShipper
    shipper = LokiShipper("http://127.0.0.1:1")
    for i in range(20000):
        shipper.put(f'{{"n": {i}}}')
    assert shipper.lines.qsize() <= 10000


def test_guarded_records_latency_in_flight_and_failures():
    from app.observability import guarded

    class Fake:
        @guarded("redis", ValueError)
        def ok(self):
            return 1

        @guarded("redis", ValueError)
        def broken(self):
            raise ValueError("x")

    Fake().ok()
    with pytest.raises(Exception):
        Fake().broken()
    from prometheus_client import generate_latest
    body = generate_latest().decode()
    assert 'fixture2030_dependencia_duracion_segundos_count{dependencia="redis",operacion="ok",resultado="ok"}' in body
    assert 'fixture2030_dependencia_duracion_segundos_count{dependencia="redis",operacion="broken",resultado="dependency_error"}' in body
    assert 'fixture2030_dependencia_en_curso{dependencia="redis"} 0.0' in body


def test_metrics_publish_mongodb_server_status(client, adapters):
    adapters["mongodb"].server_status.return_value = {"conexiones": 7, "operaciones": {"query": 42, "insert": 3}}
    body = client.get("/metrics").text
    assert "fixture2030_mongodb_conexiones_servidor 7.0" in body
    assert 'fixture2030_mongodb_operaciones_acumuladas{tipo="query"} 42.0' in body
