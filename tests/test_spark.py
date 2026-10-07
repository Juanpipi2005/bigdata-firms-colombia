import os

import pytest
import requests

API_URL = os.environ.get("API_URL", "http://api:5000")


def get(ruta, **params):
    return requests.get(f"{API_URL}{ruta}", params=params, timeout=30)


# ---------- /spark/serie-mensual ----------

def test_serie_mensual_estructura():
    r = get("/spark/serie-mensual")
    assert r.status_code == 200
    d = r.json()
    assert d["total_meses"] == len(d["meses"]) > 0
    assert "nota" in d
    for m in d["meses"]:
        assert {"mes", "sensor", "detecciones_total", "detecciones_sin_estaticas",
                "frp_promedio", "parcial"} <= set(m)
        assert m["detecciones_sin_estaticas"] <= m["detecciones_total"]


def test_serie_mensual_ordenada_y_con_un_solo_mes_parcial():
    meses = get("/spark/serie-mensual").json()["meses"]
    etiquetas = [m["mes"] for m in meses]
    assert etiquetas == sorted(etiquetas)
    assert sum(m["parcial"] for m in meses) <= 1


def test_serie_mensual_un_solo_sensor_por_mes():
    meses = get("/spark/serie-mensual").json()["meses"]
    assert len({m["mes"] for m in meses}) == len(meses)


def test_serie_mensual_filtra_por_rango():
    d = get("/spark/serie-mensual", desde="2020-03", hasta="2020-03").json()
    assert [m["mes"] for m in d["meses"]] == ["2020-03"]


@pytest.mark.parametrize(
    "params",
    [
        {"desde": "2020-13"},
        {"desde": "marzo"},
        {"hasta": "20-03"},
        {"desde": "2026-09", "hasta": "2026-01"},
    ],
)
def test_serie_mensual_parametros_invalidos(params):
    r = get("/spark/serie-mensual", **params)
    assert r.status_code == 400
    assert "error" in r.json()


# ---------- /spark/serie-horaria ----------

def test_serie_horaria_estructura():
    r = get("/spark/serie-horaria")
    assert r.status_code == 200
    d = r.json()
    assert "nota" in d and len(d["horas"]) > 0
    horas = [h["hora_local"] for h in d["horas"]]
    assert horas == sorted(horas)
    assert all(0 <= h <= 23 for h in horas)


# ---------- /spark/celdas-estaticas ----------

def test_celdas_estaticas_estructura():
    r = get("/spark/celdas-estaticas", limite=5)
    assert r.status_code == 200
    d = r.json()
    assert "nota" in d
    assert d["devueltas"] == len(d["celdas"]) <= 5
    assert d["total_candidatas"] >= d["devueltas"]
    for c in d["celdas"]:
        assert c["posible_fuente_estatica"] is True
        assert isinstance(c["primera"], str)


def test_celdas_estaticas_ordenadas_por_detecciones():
    celdas = get("/spark/celdas-estaticas", limite=20).json()["celdas"]
    conteos = [c["detecciones"] for c in celdas]
    assert conteos == sorted(conteos, reverse=True)


@pytest.mark.parametrize("limite", [0, 5000, "x"])
def test_celdas_estaticas_limite_invalido(limite):
    assert get("/spark/celdas-estaticas", limite=limite).status_code == 400
