"""Interfaz web de la ESTACIÓN DE RIEGO (Monitor + Engine).

Arranca WM_WS_M (Monitor) y WM_WS_E (Engine) tal cual, los conecta entre sí y enseña de forma
visual lo que imprimen. Los botones mandan al Engine las mismas teclas del menú (i, f, k, r).

Variables de entorno:
  BROKER_HOST / BROKER_PORT      Kafka                         (127.0.0.1 / 9092)
  CENTRAL_HOST / CENTRAL_PORT    CENTRAL (sockets)             (127.0.0.1 / 8080)
  STATION_ID                     ID que se registra en CENTRAL (WS-01)
  STATION_LOCATION               ubicación de la estación      (Sin ubicacion)
  MONITOR_PORT                   puerto del Monitor            (5000)
  WEB_PORT                       puerto de esta web            (5200)
  WM_BIN_DIR                     carpeta con los binarios si no están en el PATH
"""
import re
import threading
import time

from flask import Flask, jsonify, render_template, request

from skin import Consola, Proceso, binario, entorno

BROKER_HOST = entorno("BROKER_HOST", "127.0.0.1")
BROKER_PORT = entorno("BROKER_PORT", "9092")
CENTRAL_HOST = entorno("CENTRAL_HOST", "127.0.0.1")
CENTRAL_PORT = entorno("CENTRAL_PORT", "8080")
STATION_ID = entorno("STATION_ID", "WS-01")
STATION_LOCATION = entorno("STATION_LOCATION", "Sin ubicacion")
MONITOR_PORT = entorno("MONITOR_PORT", "5000")
WEB_PORT = int(entorno("WEB_PORT", "5200"))

app = Flask(__name__)
consola = Consola()
cerrojo = threading.Lock()
estado = {}


def reiniciar_estado():
    estado.clear()
    estado.update({
        "central_ok": False,      # CENTRAL ha validado la estación (Monitor)
        "central_msg": "",
        "engine_conectado": False,
        "kafka": False,
        "id": "",                 # ID asignado por el Monitor al Engine
        "valvula": False,         # electroválvula abierta
        "fuga": False,
        "operario": "",
        "duracion": 0,
        "inicio_riego": 0.0,
        "caudal": 0.0,
        "volumen": 0.0,
        "serie": [],
        "estado_central": "",     # último STATUS que devolvió CENTRAL al Monitor (OK / KO)
        "aviso": "",
        "historial": [],          # riegos terminados
        "eventos": [],            # cosas relevantes (fugas, órdenes, conexiones)
    })


reiniciar_estado()

RE_ABRE = re.compile(r"Electroválvula ABIERTA\. Riego para el operario (\S+) durante (\d+) s")
RE_CIERRA = re.compile(r"Electroválvula CERRADA\. Riego finalizado \((.*?)\): ([\d.]+) L en (\d+) s")
RE_REGANDO = re.compile(r"Regando: caudal ([\d.]+) L/min, volumen ([\d.]+) L")
RE_ID = re.compile(r"ID asignado por el Monitor: (\S+)")
RE_STATUS = re.compile(r"Estado recibido: STATUS#(\w+)")


def hora():
    return time.strftime("%H:%M:%S")


def evento(txt, nivel="info"):
    estado["eventos"] = ([{"hora": hora(), "txt": txt, "nivel": nivel}] + estado["eventos"])[:40]


def al_linea(linea):
    txt, src = linea["txt"], linea["src"]
    if linea["cls"] == "entrada":
        return
    with cerrojo:
        e = estado
        if txt.startswith("[web] El programa ha terminado"):
            if src == "engine":
                e["engine_conectado"] = False
                e["kafka"] = False
                e["valvula"] = False
                e["caudal"] = 0.0
            else:
                e["central_ok"] = False
            evento("%s terminado" % ("Engine" if src == "engine" else "Monitor"), "mal")
            return
        if "CENTRAL ha validado la estación" in txt:
            e["central_ok"] = True
            e["central_msg"] = txt.split(": ", 1)[-1]
            evento("CENTRAL ha validado la estación", "ok")
        elif "CENTRAL rechaza la autenticación" in txt:
            e["central_ok"] = False
            e["aviso"] = txt
            evento(txt.replace("[Monitor] ", ""), "mal")
        elif "CENTRAL no disponible" in txt:
            e["central_ok"] = False
            e["aviso"] = "CENTRAL no disponible. El Monitor reintenta cada 3 s."
        elif "Engine conectado" in txt:
            evento("Engine conectado al Monitor", "ok")
        elif "Engine sin respuesta o desconectado" in txt:
            e["engine_conectado"] = False
            evento("El Monitor pierde al Engine", "mal")
        m = RE_STATUS.search(txt)
        if m:
            e["estado_central"] = m.group(1)
            return
        if "Conectado a Kafka" in txt:
            e["kafka"] = True
            return
        if "Conectado con el Monitor" in txt:
            e["engine_conectado"] = True
            e["aviso"] = ""
            return
        if "Conexión con el Monitor perdida" in txt:
            e["engine_conectado"] = False
            evento("Conexión Engine-Monitor perdida", "mal")
            return
        m = RE_ID.search(txt)
        if m:
            e["id"] = m.group(1)
            return
        m = RE_ABRE.search(txt)
        if m:
            e.update(valvula=True, operario=m.group(1), duracion=int(m.group(2)), inicio_riego=time.time(),
                     caudal=0.0, volumen=0.0, serie=[])
            evento("Electroválvula ABIERTA para %s (%s s)" % (m.group(1), m.group(2)), "ok")
            return
        m = RE_REGANDO.search(txt)
        if m:
            e["valvula"] = True
            e["caudal"] = float(m.group(1))
            e["volumen"] = float(m.group(2))
            e["serie"] = (e["serie"] + [e["caudal"]])[-90:]
            return
        m = RE_CIERRA.search(txt)
        if m:
            e["historial"] = ([{"hora": hora(), "operario": e["operario"], "volumen": float(m.group(2)),
                                "segundos": int(m.group(3)), "motivo": m.group(1)}] + e["historial"])[:30]
            e.update(valvula=False, caudal=0.0)
            evento("Electroválvula CERRADA (%s): %s L en %s s" % (m.group(1), m.group(2), m.group(3)))
            return
        if "¡Fuga simulada!" in txt:
            e["fuga"] = True
            evento("¡Fuga detectada! Estado KO", "mal")
        elif "¡Fuga durante el riego!" in txt:
            e["fuga"] = True
            e["aviso"] = "Fuga durante el riego: se ha cortado el suministro."
            evento("¡Fuga durante el riego!", "mal")
        elif "Fuga reparada" in txt:
            e["fuga"] = False
            e["aviso"] = ""
            evento("Fuga reparada. Estado OK", "ok")
        elif "Orden ABRIR rechazada" in txt:
            evento(txt.replace("[Engine] ", ""), "aviso")
        elif "Aún no tengo ID" in txt:
            e["aviso"] = "El Engine aún no tiene ID: espera a que el Monitor se registre en CENTRAL."
        elif "Riego manual solicitado" in txt:
            evento("Riego manual solicitado a CENTRAL", "info")
        elif "No hay ningún riego en curso" in txt:
            e["aviso"] = "No hay ningún riego en curso."


monitor = Proceso("monitor", [binario("WM_WS_M"), MONITOR_PORT, CENTRAL_HOST, CENTRAL_PORT, STATION_ID, STATION_LOCATION],
                  consola, al_linea)
engine = Proceso("engine", [binario("WM_WS_E"), BROKER_HOST, BROKER_PORT, "127.0.0.1", MONITOR_PORT], consola, al_linea)


def arrancar():
    monitor.iniciar()
    time.sleep(2)
    engine.iniciar()


def derivar():
    e = estado
    if not engine.vivo() or not e["engine_conectado"]:
        return "DISCONNECTED"
    if e["fuga"]:
        return "KO"
    if e["valvula"]:
        return "WATERING"
    return "AVAILABLE"


@app.route("/")
def index():
    return render_template("estacion.html", estacion=STATION_ID, ubicacion=STATION_LOCATION)


@app.route("/api/estado")
def api_estado():
    try:
        desde = int(request.args.get("desde", "0"))
    except ValueError:
        desde = 0
    with cerrojo:
        d = dict(estado)
        for k in ("serie", "historial", "eventos"):
            d[k] = list(estado[k])
        d["estado"] = derivar()
    d.update(station=STATION_ID, ubicacion=STATION_LOCATION, broker=BROKER_HOST + ":" + BROKER_PORT,
             central=CENTRAL_HOST + ":" + CENTRAL_PORT, monitor_vivo=monitor.vivo(), engine_vivo=engine.vivo(),
             ahora=time.time(), lineas=consola.desde(desde), ultimo=consola.ultimo)
    return jsonify(d)


def error(m, c=409):
    return jsonify({"ok": False, "error": m}), c


ACCIONES = {"iniciar": "i", "finalizar": "f", "fuga": "k", "reparar": "r"}


@app.route("/api/accion", methods=["POST"])
def api_accion():
    accion = (request.get_json(silent=True) or {}).get("accion", "")
    if accion not in ACCIONES:
        return error("Acción desconocida.", 400)
    if not engine.vivo():
        return error("El Engine no está en marcha. Pulsa Reiniciar estación.")
    with cerrojo:
        estado["aviso"] = ""
    engine.enviar(ACCIONES[accion])
    return jsonify({"ok": True})


@app.route("/api/reiniciar", methods=["POST"])
def api_reiniciar():
    engine.detener()
    monitor.detener()
    with cerrojo:
        h, ev = list(estado["historial"]), list(estado["eventos"])
        reiniciar_estado()
        estado["historial"], estado["eventos"] = h, ev
    threading.Thread(target=arrancar, daemon=True).start()
    return jsonify({"ok": True})


if __name__ == "__main__":
    arrancar()
    app.run(host="0.0.0.0", port=WEB_PORT, threaded=True)
