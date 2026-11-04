"""Interfaz web del OPERARIO (Field Operator).

Es una "piel" sobre el programa WM_FO: arranca WM_FO tal cual, le manda lo que escribiría el
operario (estación y segundos, 'l', 'f fichero') y enseña de forma visual lo que WM_FO va imprimiendo.

Variables de entorno:
  BROKER_HOST  IP del broker Kafka           (por defecto 127.0.0.1)
  BROKER_PORT  puerto del broker             (por defecto 9092)
  OPERARIO     ID del operario               (por defecto OP-1)
  WEB_PORT     puerto de esta web            (por defecto 5100)
  WM_BIN_DIR   carpeta de WM_FO si no está en el PATH
"""
import os
import re
import tempfile
import threading
import time

from flask import Flask, jsonify, render_template, request

from skin import Consola, Proceso, binario, entorno

BROKER_HOST = entorno("BROKER_HOST", "127.0.0.1")
BROKER_PORT = entorno("BROKER_PORT", "9092")
OPERARIO = entorno("OPERARIO", "OP-1")
WEB_PORT = int(entorno("WEB_PORT", "5100"))

app = Flask(__name__)
consola = Consola()
cerrojo = threading.Lock()

# Estado que la web deduce de lo que imprime WM_FO
estado = {}


def reiniciar_estado():
    estado.clear()
    estado.update({
        "kafka": False,
        "fase": "reposo",        # reposo | solicitando | autorizado | regando | finalizado | denegado
        "ws": "",                # estación de la petición actual
        "segundos": 0,           # duración pedida
        "inicio_peticion": 0.0,
        "inicio_riego": 0.0,
        "caudal": 0.0,
        "volumen": 0.0,
        "serie": [],             # caudales del riego actual (para la gráfica)
        "mensaje": "",           # último mensaje de CENTRAL
        "aviso": "",             # avisos importantes (avería, tiempo agotado...)
        "disponibles": None,     # None = aún no se ha pedido la lista
        "disponibles_ts": 0.0,
        "historial": [],         # resultados de las últimas peticiones
        "fichero": {"activo": False, "ruta": "", "espera": 0},
    })


reiniciar_estado()

RE_SOLICITA = re.compile(r"Solicitando riego en (\S+) durante (\d+) s")
RE_CENTRAL = re.compile(r"^\[CENTRAL\] (INFO|OK|DENEGADO|RESUMEN): (.*)$")
RE_RIEGO = re.compile(r"^\s*\[(\S+)\] REGANDO\s+caudal ([\d.]+) L/min \| volumen acumulado ([\d.]+) L")
RE_DISP = re.compile(r"Estaciones disponibles \((\d+)\):\s*(.*)$")


def hora():
    return time.strftime("%H:%M:%S")


def al_linea(linea):
    """Interpreta cada línea que imprime WM_FO y actualiza el estado visual."""
    txt = linea["txt"]
    if linea["cls"] in ("entrada", "sistema") and not txt.startswith("[web] El programa ha terminado"):
        return
    with cerrojo:
        e = estado
        if txt.startswith("[web] El programa ha terminado"):
            e["kafka"] = False
            return
        if "Conectado a Kafka" in txt:
            e["kafka"] = True
            return

        m = RE_SOLICITA.search(txt)
        if m:
            e.update(fase="solicitando", ws=m.group(1), segundos=int(m.group(2)), inicio_peticion=time.time(),
                     inicio_riego=0.0, caudal=0.0, volumen=0.0, serie=[], aviso="")
            return

        m = RE_RIEGO.match(txt)
        if m:
            if e["fase"] not in ("solicitando", "regando"):
                return  # línea rezagada tras el resumen: se ignora
            if e["fase"] != "regando":
                e["fase"] = "regando"
                e["inicio_riego"] = e["inicio_riego"] or time.time()
            e["ws"] = m.group(1)
            e["caudal"] = float(m.group(2))
            e["volumen"] = float(m.group(3))
            e["serie"] = (e["serie"] + [e["caudal"]])[-90:]
            return

        m = RE_CENTRAL.match(txt)
        if m:
            tipo, texto = m.groups()
            e["mensaje"] = texto
            if tipo == "INFO":
                d = RE_DISP.search(texto)
                if d:
                    e["disponibles"] = d.group(2).split()
                    e["disponibles_ts"] = time.time()
                elif texto.startswith("No hay estaciones disponibles"):
                    e["disponibles"] = []
                    e["disponibles_ts"] = time.time()
                elif "Avería" in texto or "bloqueado" in texto:
                    e["aviso"] = texto
            elif tipo == "OK":
                e["fase"] = "regando"
                e["inicio_riego"] = time.time()
            elif tipo == "DENEGADO":
                e["fase"] = "denegado"
                e["historial"] = ([{"hora": hora(), "ws": e["ws"], "resultado": "DENEGADO", "texto": texto}]
                                  + e["historial"])[:30]
            elif tipo == "RESUMEN":
                e["fase"] = "finalizado"
                e["caudal"] = 0.0
                e["historial"] = ([{"hora": hora(), "ws": e["ws"], "resultado": "COMPLETADO", "texto": texto}]
                                  + e["historial"])[:30]
            return

        if "no ha concluido la petición a tiempo" in txt:
            e["fase"] = "reposo"
            e["aviso"] = "CENTRAL no respondió a tiempo. ¿Está caída?"
            return
        if "No se pudo enviar la petición" in txt:
            e["fase"] = "reposo"
            e["aviso"] = "No se pudo enviar la petición (¿Kafka caído?)"
            return
        m = re.search(r"Ejecutando servicios del fichero '(.*)'", txt)
        if m:
            e["fichero"] = {"activo": True, "ruta": m.group(1), "espera": 0}
            return
        if re.search(r"Fichero '.*' terminado", txt):
            e["fichero"] = {"activo": False, "ruta": "", "espera": 0}
            return
        if "No se puede abrir el fichero" in txt:
            e["fichero"] = {"activo": False, "ruta": "", "espera": 0}
            e["aviso"] = txt
            return


programa = Proceso("fo", [binario("WM_FO"), BROKER_HOST, BROKER_PORT, OPERARIO], consola, al_linea)


def ocupado():
    e = estado
    return e["fase"] in ("solicitando", "regando") or e["fichero"]["activo"]


# ---------------------------------------------------------------------- rutas
@app.route("/")
def index():
    return render_template("operario.html", operario=OPERARIO)


@app.route("/api/estado")
def api_estado():
    try:
        desde = int(request.args.get("desde", "0"))
    except ValueError:
        desde = 0
    with cerrojo:
        datos = dict(estado)
        datos["historial"] = list(estado["historial"])
        datos["serie"] = list(estado["serie"])
        datos["ocupado"] = ocupado()
    datos.update(operario=OPERARIO, broker=BROKER_HOST + ":" + BROKER_PORT, vivo=programa.vivo(),
                 ahora=time.time(), lineas=consola.desde(desde), ultimo=consola.ultimo)
    return jsonify(datos)


def error(mensaje, codigo=400):
    return jsonify({"ok": False, "error": mensaje}), codigo


@app.route("/api/regar", methods=["POST"])
def api_regar():
    datos = request.get_json(silent=True) or {}
    ws = str(datos.get("ws", "")).strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,30}", ws):
        return error("Escribe el ID de una estación (por ejemplo WS-01).")
    try:
        segundos = int(datos.get("segundos", 0))
    except (TypeError, ValueError):
        return error("Los segundos deben ser un número.")
    if not 1 <= segundos <= 600:
        return error("Los segundos deben estar entre 1 y 600.")
    if not programa.vivo():
        return error("WM_FO no está en marcha. Pulsa Reiniciar.", 409)
    if ocupado():
        return error("Ya hay una petición en curso: espera a que termine.", 409)
    with cerrojo:
        # Se marca ya como 'solicitando' para que la web responda al instante
        estado.update(fase="solicitando", ws=ws, segundos=segundos, inicio_peticion=time.time(),
                      inicio_riego=0.0, caudal=0.0, volumen=0.0, serie=[], aviso="", mensaje="")
    programa.enviar("%s %d" % (ws, segundos))
    return jsonify({"ok": True})


@app.route("/api/listar", methods=["POST"])
def api_listar():
    if not programa.vivo():
        return error("WM_FO no está en marcha. Pulsa Reiniciar.", 409)
    if ocupado():
        return error("WM_FO está esperando a que termine el riego; no puede listar hasta entonces.", 409)
    programa.enviar("l")
    return jsonify({"ok": True})


@app.route("/api/fichero", methods=["POST"])
def api_fichero():
    datos = request.get_json(silent=True) or {}
    contenido = str(datos.get("contenido", ""))
    if not contenido.strip():
        return error("El fichero está vacío.")
    if len(contenido) > 4000:
        return error("El fichero es demasiado largo (máximo 4000 caracteres).")
    for linea in contenido.splitlines():
        linea = linea.strip()
        if linea and not linea.startswith("//") and not re.fullmatch(r"[A-Za-z0-9_-]{1,30}(#\d{1,3})?", linea):
            return error("Línea no válida: '%s'. Formato: ID_WS#segundos" % linea[:40])
    if not programa.vivo():
        return error("WM_FO no está en marcha. Pulsa Reiniciar.", 409)
    if ocupado():
        return error("Ya hay una petición en curso: espera a que termine.", 409)
    carpeta = os.path.join(tempfile.gettempdir(), "wm_servicios")
    os.makedirs(carpeta, exist_ok=True)
    ruta = os.path.join(carpeta, "%s_%d.txt" % (re.sub(r"[^A-Za-z0-9_-]", "", OPERARIO), int(time.time())))
    with open(ruta, "w", encoding="utf-8") as f:
        f.write(contenido.strip() + "\n")
    with cerrojo:
        estado["fichero"] = {"activo": True, "ruta": ruta, "espera": 0}
        estado["aviso"] = ""
    programa.enviar("f " + ruta)
    return jsonify({"ok": True, "ruta": ruta})


@app.route("/api/reiniciar", methods=["POST"])
def api_reiniciar():
    with cerrojo:
        historial = list(estado["historial"])
        reiniciar_estado()
        estado["historial"] = historial
    programa.reiniciar()
    return jsonify({"ok": True})


if __name__ == "__main__":
    programa.iniciar()
    app.run(host="0.0.0.0", port=WEB_PORT, threaded=True)
