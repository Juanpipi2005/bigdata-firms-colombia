import os
from datetime import datetime

from flask import Flask, jsonify, request
from pymongo import MongoClient
from pymongo.errors import OperationFailure, PyMongoError

app = Flask(__name__)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "firms_colombia")
COLECCION = os.environ.get("COLECCION_FOCOS", "focos_calor")

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
focos = client[MONGO_DB][COLECCION]

LIMITE_DEFECTO = 100
LIMITE_MAXIMO = 1000
RADIO_DEFECTO_KM = 5
RADIO_MAXIMO_KM = 100
RADIO_MAXIMO_AGREGADO_KM = 50
RADIO_TIERRA_KM = 6378.1
CONFIANZAS_VALIDAS = {"l", "n", "h"}
TIMEOUT_MS = 30000

PROYECCION = {
    "_id": 0,
    "latitude": 1,
    "longitude": 1,
    "fecha_hora_utc": 1,
    "fecha_hora_colombia": 1,
    "satellite": 1,
    "confidence": 1,
    "frp": 1,
    "daynight": 1,
    "type": 1,
    "origen": 1,
}


class ParametroInvalido(Exception):
    pass


@app.errorhandler(ParametroInvalido)
def manejar_parametro_invalido(e):
    return jsonify({"error": str(e)}), 400


@app.errorhandler(PyMongoError)
def manejar_error_mongo(e):
    app.logger.error("Error de MongoDB: %s", e)
    return jsonify({"error": "Error consultando MongoDB"}), 503


def leer_float(args, nombre, minimo, maximo, defecto=None):
    valor = args.get(nombre)
    if valor is None:
        if defecto is None:
            raise ParametroInvalido(f"Falta el parametro '{nombre}'")
        return defecto
    try:
        numero = float(valor)
    except ValueError:
        raise ParametroInvalido(f"'{nombre}' debe ser numerico")
    if not (minimo <= numero <= maximo):
        raise ParametroInvalido(f"'{nombre}' debe estar entre {minimo} y {maximo}")
    return numero


def leer_limite(args):
    valor = args.get("limite")
    if valor is None:
        return LIMITE_DEFECTO
    try:
        limite = int(valor)
    except ValueError:
        raise ParametroInvalido("'limite' debe ser un entero")
    if not (1 <= limite <= LIMITE_MAXIMO):
        raise ParametroInvalido(f"'limite' debe estar entre 1 y {LIMITE_MAXIMO}")
    return limite


def leer_confianza(args):
    valor = args.get("confianza")
    if valor is None:
        return None
    niveles = [n.strip().lower() for n in valor.split(",") if n.strip()]
    if not niveles or not set(niveles) <= CONFIANZAS_VALIDAS:
        raise ParametroInvalido("'confianza' debe ser una lista de l, n, h (ej: n,h)")
    return niveles


def serializar(doc):
    salida = {}
    for clave, valor in doc.items():
        if isinstance(valor, datetime):
            valor = valor.isoformat() + ("Z" if clave == "fecha_hora_utc" else "")
        salida[clave] = valor
    return salida


def validar_poligono(poligono):
    if not isinstance(poligono, dict) or poligono.get("type") != "Polygon":
        raise ParametroInvalido("'poligono' debe ser un GeoJSON de tipo Polygon")
    anillos = poligono.get("coordinates")
    if not isinstance(anillos, list) or not anillos:
        raise ParametroInvalido("'poligono.coordinates' debe ser una lista de anillos")
    for anillo in anillos:
        if not isinstance(anillo, list) or len(anillo) < 4:
            raise ParametroInvalido("Cada anillo necesita al menos 4 puntos")
        if anillo[0] != anillo[-1]:
            raise ParametroInvalido("Cada anillo debe cerrarse (primer y ultimo punto iguales)")


@app.route("/health")
def health():
    estado_mongo = "desconocido"
    try:
        client.admin.command("ping")
        estado_mongo = "conectado"
    except Exception as e:
        estado_mongo = f"error: {e}"

    return jsonify({"status": "ok", "mongo": estado_mongo, "db": MONGO_DB})


@app.route("/focos/cerca")
def focos_cerca():
    lat = leer_float(request.args, "lat", -90, 90)
    lon = leer_float(request.args, "lon", -180, 180)
    radio_km = leer_float(request.args, "radio_km", 0.1, RADIO_MAXIMO_KM, RADIO_DEFECTO_KM)
    limite = leer_limite(request.args)
    confianza = leer_confianza(request.args)

    filtro_confianza = {"confidence": {"$in": confianza}} if confianza else {}

    # $near no se puede usar en count_documents, asi que el total se cuenta con $geoWithin
    total = focos.count_documents(
        {
            "location": {"$geoWithin": {"$centerSphere": [[lon, lat], radio_km / RADIO_TIERRA_KM]}},
            **filtro_confianza,
        },
        maxTimeMS=TIMEOUT_MS,
    )

    consulta = {
        "location": {
            "$near": {
                "$geometry": {"type": "Point", "coordinates": [lon, lat]},
                "$maxDistance": radio_km * 1000,
            }
        },
        **filtro_confianza,
    }
    docs = [
        serializar(d)
        for d in focos.find(consulta, PROYECCION).limit(limite).max_time_ms(TIMEOUT_MS)
    ]

    return jsonify(
        {
            "centro": {"lat": lat, "lon": lon},
            "radio_km": radio_km,
            "total_en_radio": total,
            "devueltos": len(docs),
            "focos": docs,
        }
    )


@app.route("/focos/poligono", methods=["POST"])
def focos_poligono():
    cuerpo = request.get_json(silent=True)
    if not isinstance(cuerpo, dict):
        raise ParametroInvalido("El cuerpo debe ser un JSON con la clave 'poligono'")

    poligono = cuerpo.get("poligono")
    validar_poligono(poligono)
    limite = leer_limite(cuerpo)
    confianza = leer_confianza(cuerpo)

    filtro = {"location": {"$geoWithin": {"$geometry": poligono}}}
    if confianza:
        filtro["confidence"] = {"$in": confianza}

    try:
        total = focos.count_documents(filtro, maxTimeMS=TIMEOUT_MS)
        docs = [
            serializar(d)
            for d in focos.find(filtro, PROYECCION).limit(limite).max_time_ms(TIMEOUT_MS)
        ]
    except OperationFailure as e:
        app.logger.error("Poligono rechazado por MongoDB: %s", e)
        raise ParametroInvalido("MongoDB rechazo el poligono (revisa coordenadas y orden [lon, lat])")

    return jsonify({"total_en_poligono": total, "devueltos": len(docs), "focos": docs})


@app.route("/agregados/cerca-mensual")
def agregado_cerca_mensual():
    lat = leer_float(request.args, "lat", -90, 90)
    lon = leer_float(request.args, "lon", -180, 180)
    radio_km = leer_float(
        request.args, "radio_km", 0.1, RADIO_MAXIMO_AGREGADO_KM, RADIO_DEFECTO_KM
    )

    pipeline = [
        {
            "$geoNear": {
                "near": {"type": "Point", "coordinates": [lon, lat]},
                "distanceField": "distancia_m",
                "maxDistance": radio_km * 1000,
                "spherical": True,
                "key": "location",
            }
        },
        {
            "$group": {
                "_id": {"$dateToString": {"format": "%Y-%m", "date": "$fecha_hora_utc"}},
                "detecciones": {"$sum": 1},
                "frp_promedio": {"$avg": "$frp"},
                "distancia_promedio_m": {"$avg": "$distancia_m"},
            }
        },
        {"$sort": {"_id": 1}},
    ]

    meses = []
    for fila in focos.aggregate(pipeline, maxTimeMS=TIMEOUT_MS):
        meses.append(
            {
                "mes": fila["_id"],
                "detecciones": fila["detecciones"],
                "frp_promedio": round(fila["frp_promedio"], 2),
                "distancia_promedio_m": round(fila["distancia_promedio_m"], 1),
            }
        )

    return jsonify(
        {
            "centro": {"lat": lat, "lon": lon},
            "radio_km": radio_km,
            "total_meses": len(meses),
            "meses": meses,
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
