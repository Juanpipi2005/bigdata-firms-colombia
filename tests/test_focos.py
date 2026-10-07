import os

import pytest
import requests

API_URL = os.environ.get("API_URL", "http://api:5000")

PUNTO = {"lat": 11.13, "lon": -72.58}
CUADRADO = {
    "type": "Polygon",
    "coordinates": [
        [[-72.63, 11.08], [-72.53, 11.08], [-72.53, 11.18], [-72.63, 11.18], [-72.63, 11.08]]
    ],
}


def get(ruta, **params):
    return requests.get(f"{API_URL}{ruta}", params=params, timeout=30)


def post_poligono(cuerpo):
    return requests.post(f"{API_URL}/focos/poligono", json=cuerpo, timeout=30)


# ---------- /focos/cerca ($near) ----------

def test_cerca_estructura():
    r = get("/focos/cerca", **PUNTO, radio_km=5, limite=2)
    assert r.status_code == 200
    d = r.json()
    for clave in ("centro", "radio_km", "total_en_radio", "devueltos", "focos"):
        assert clave in d
    assert d["devueltos"] == len(d["focos"]) <= 2
    assert d["total_en_radio"] >= d["devueltos"]


def test_cerca_formato_de_los_focos():
    d = get("/focos/cerca", **PUNTO, radio_km=5, limite=3).json()
    for f in d["focos"]:
        assert "_id" not in f
        assert f["fecha_hora_utc"].endswith("Z")
        assert not f["fecha_hora_colombia"].endswith("Z")


def test_cerca_filtra_por_confianza():
    d = get("/focos/cerca", **PUNTO, radio_km=5, limite=50, confianza="h").json()
    assert all(f["confidence"] == "h" for f in d["focos"])


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"lat": 11.13},
        {"lat": 100, "lon": -72.58},
        {"lat": "abc", "lon": -72.58},
        {**PUNTO, "radio_km": 0},
        {**PUNTO, "radio_km": 500},
        {**PUNTO, "limite": 0},
        {**PUNTO, "limite": 5000},
        {**PUNTO, "confianza": "x"},
    ],
)
def test_cerca_parametros_invalidos(params):
    r = get("/focos/cerca", **params)
    assert r.status_code == 400
    assert "error" in r.json()


# ---------- /focos/poligono ($geoWithin) ----------

def test_poligono_estructura():
    r = post_poligono({"poligono": CUADRADO, "limite": 2})
    assert r.status_code == 200
    d = r.json()
    for clave in ("total_en_poligono", "devueltos", "focos"):
        assert clave in d
    assert d["devueltos"] == len(d["focos"]) <= 2


def test_poligono_filtra_por_confianza():
    d = post_poligono({"poligono": CUADRADO, "limite": 50, "confianza": "n,h"}).json()
    assert all(f["confidence"] in ("n", "h") for f in d["focos"])


@pytest.mark.parametrize(
    "cuerpo",
    [
        {},
        {"poligono": {"type": "Point", "coordinates": [-72.58, 11.13]}},
        {"poligono": {"type": "Polygon", "coordinates": []}},
        {"poligono": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1]]]}},
        {"poligono": CUADRADO, "limite": 0},
        {"poligono": CUADRADO, "confianza": "z"},
    ],
)
def test_poligono_invalido(cuerpo):
    r = post_poligono(cuerpo)
    assert r.status_code == 400
    assert "error" in r.json()


def test_poligono_sin_json():
    r = requests.post(
        f"{API_URL}/focos/poligono",
        data="esto no es json",
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    assert r.status_code == 400


# ---------- /agregados/cerca-mensual ($geoNear) ----------

def test_agregado_estructura():
    r = get("/agregados/cerca-mensual", **PUNTO, radio_km=5)
    assert r.status_code == 200
    d = r.json()
    assert d["total_meses"] == len(d["meses"])
    for m in d["meses"]:
        assert set(m) == {"mes", "detecciones", "frp_promedio", "distancia_promedio_m"}


def test_agregado_radio_maximo_excedido():
    assert get("/agregados/cerca-mensual", **PUNTO, radio_km=51).status_code == 400


def test_agregado_y_cerca_cuentan_lo_mismo():
    total = get("/focos/cerca", **PUNTO, radio_km=5, limite=1).json()["total_en_radio"]
    meses = get("/agregados/cerca-mensual", **PUNTO, radio_km=5).json()["meses"]
    assert sum(m["detecciones"] for m in meses) == total


# ---------- /focos/cerca: filtro frp_min ----------

def test_cerca_frp_min_filtra():
    d = get("/focos/cerca", **PUNTO, radio_km=5, limite=50, frp_min=10).json()
    assert all(f["frp"] >= 10 for f in d["focos"])


def test_cerca_frp_min_cero_no_cambia_el_total():
    sin = get("/focos/cerca", **PUNTO, radio_km=5, limite=1).json()["total_en_radio"]
    con = get("/focos/cerca", **PUNTO, radio_km=5, limite=1, frp_min=0).json()["total_en_radio"]
    assert con == sin


def test_cerca_frp_min_reduce_el_total():
    sin = get("/focos/cerca", **PUNTO, radio_km=5, limite=1).json()["total_en_radio"]
    con = get("/focos/cerca", **PUNTO, radio_km=5, limite=1, frp_min=10).json()["total_en_radio"]
    assert con <= sin


@pytest.mark.parametrize("frp_min", ["-1", "x", "10001"])
def test_cerca_frp_min_invalido(frp_min):
    r = get("/focos/cerca", **PUNTO, frp_min=frp_min)
    assert r.status_code == 400
    assert "error" in r.json()
