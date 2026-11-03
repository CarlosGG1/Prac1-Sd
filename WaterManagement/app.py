"""Panel web de la CENTRAL de WaterManagement.

- Lee la BD de la central (SQLite) en SOLO LECTURA y la muestra en tiempo real (estaciones, resumen, sucesos).
- Opcionalmente puede enviar órdenes de administración a la CENTRAL por socket (iniciar / bloquear / activar):
  la CENTRAL las ejecuta ella misma con su código; este panel no toca la BD ni Kafka.

Variables de entorno:
  DB_PATH       ruta de central.db                          (./central.db)
  CENTRAL_HOST  host de la CENTRAL (para las órdenes)       (127.0.0.1)
  CENTRAL_PORT  puerto de sockets de la CENTRAL             (8080)
  ADMIN_TOKEN   clave de administración (la misma que en la CENTRAL). Sin ella el panel es solo de lectura.
  WEB_PORT      puerto de esta web                          (5000)
"""
import os
import re
import socket
import sqlite3
import threading
import time

from flask import Flask, jsonify, render_template, request

app = Flask(__name__)
DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "central.db"))
CENTRAL_HOST = os.environ.get("CENTRAL_HOST", "127.0.0.1").strip() or "127.0.0.1"
CENTRAL_PORT = int(os.environ.get("CENTRAL_PORT", "8080") or 8080)
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "").strip()
WEB_PORT = int(os.environ.get("WEB_PORT", "5000") or 5000)

ESTADOS = ("AVAILABLE", "WATERING", "KO", "OUT_OF_SERVICE", "DISCONNECTED")
TEXTO_ESTADO = {"AVAILABLE": "disponible", "WATERING": "regando", "KO": "averiada (KO)",
                "OUT_OF_SERVICE": "fuera de servicio", "DISCONNECTED": "desconectada"}
NIVEL_ESTADO = {"AVAILABLE": "ok", "WATERING": "ok", "KO": "mal", "OUT_OF_SERVICE": "aviso", "DISCONNECTED": "info"}

cerrojo = threading.Lock()
estaciones = []          # última lectura de la BD
eventos = []             # sucesos deducidos de los cambios
central = {"alcanzable": None, "mensaje": "", "ts": 0.0, "bd": True}


def leer_bd():
    # Solo lectura: el panel nunca modifica la base de datos de la central
    conn = sqlite3.connect("file:%s?mode=ro" % DB_PATH, uri=True, timeout=2)
    conn.row_factory = sqlite3.Row
    try:
        filas = conn.execute("SELECT ID, UBICACION, ESTADO, CAUDAL, VOLUMEN, OPERARIO FROM ESTACIONES ORDER BY ID;").fetchall()
    finally:
        conn.close()
    return [{"id": f["ID"], "ubicacion": f["UBICACION"] or "", "estado": f["ESTADO"] or "DISCONNECTED",
             "caudal": f["CAUDAL"] or 0, "volumen": f["VOLUMEN"] or 0, "operario": f["OPERARIO"] or ""} for f in filas]


def evento(txt, nivel="info"):
    eventos.insert(0, {"ts": time.time(), "hora": time.strftime("%H:%M:%S"), "txt": txt, "nivel": nivel})
    del eventos[80:]


def comparar(antes, ahora):
    """Genera sucesos legibles a partir de lo que ha cambiado entre dos lecturas de la BD."""
    previo = {e["id"]: e for e in antes}
    for e in ahora:
        p = previo.get(e["id"])
        lugar = "%s (%s)" % (e["id"], e["ubicacion"]) if e["ubicacion"] else e["id"]
        if p is None:
            evento("Nueva estación registrada: %s" % lugar, "ok")
        elif p["estado"] != e["estado"]:
            if e["estado"] == "WATERING":
                evento("%s empieza a regar para %s" % (lugar, e["operario"] or "un operario"), "ok")
            elif p["estado"] == "WATERING":
                evento("%s termina el riego (%.1f L) y pasa a %s" % (lugar, p["volumen"], TEXTO_ESTADO.get(e["estado"], e["estado"])),
                       NIVEL_ESTADO.get(e["estado"], "info"))
            else:
                evento("%s pasa a %s" % (lugar, TEXTO_ESTADO.get(e["estado"], e["estado"])), NIVEL_ESTADO.get(e["estado"], "info"))
    for p in antes:
        if not any(e["id"] == p["id"] for e in ahora):
            evento("La estación %s ya no está en la base de datos" % p["id"], "aviso")


def vigilar_bd():
    global estaciones
    primera = True
    while True:
        try:
            ahora = leer_bd()
            with cerrojo:
                if not primera:
                    comparar(estaciones, ahora)
                estaciones = ahora
                if not central["bd"]:
                    evento("Base de datos accesible de nuevo", "ok")
                central["bd"] = True
            primera = False
        except Exception as ex:
            with cerrojo:
                if central["bd"]:
                    evento("No se puede leer la base de datos de CENTRAL (%s)" % ex, "mal")
                central["bd"] = False
        time.sleep(1)


def enviar_a_central(destino, accion, segundos=0):
    """Envía ADMIN#TOKEN#destino accion [segundos] a CENTRAL y devuelve (ok, texto)."""
    if not ADMIN_TOKEN:
        return False, "Administración desactivada: el panel no tiene ADMIN_TOKEN."
    orden = "%s %s" % (destino, accion) + (" %d" % segundos if accion == "iniciar" else "")
    try:
        s = socket.create_connection((CENTRAL_HOST, CENTRAL_PORT), timeout=8)
        s.settimeout(15)
        s.sendall(("ADMIN#%s#%s" % (ADMIN_TOKEN, orden)).encode())
        r = s.recv(1024).decode("utf-8", "replace").strip()
        s.close()
    except OSError as ex:
        return False, "No se puede contactar con CENTRAL (%s:%s): %s" % (CENTRAL_HOST, CENTRAL_PORT, ex)
    partes = r.split("#", 2)
    if len(partes) == 3 and partes[0] == "STATUS":
        return partes[1] == "OK", partes[2]
    return False, "Respuesta inesperada de CENTRAL: %s" % r[:80]


def latido():
    """Cada 30 s comprueba que la CENTRAL contesta (solo si hay token)."""
    while True:
        if ADMIN_TOKEN:
            ok, msg = enviar_a_central("todas", "ping")
            with cerrojo:
                if central["alcanzable"] is not ok:
                    evento("CENTRAL responde" if ok else "CENTRAL no responde: " + msg, "ok" if ok else "mal")
                central.update(alcanzable=ok, mensaje=msg, ts=time.time())
        time.sleep(30)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/estaciones")
def api_estaciones():
    with cerrojo:
        return jsonify(list(estaciones))


@app.route("/api/panel")
def api_panel():
    with cerrojo:
        cuenta = {k: 0 for k in ESTADOS}
        for e in estaciones:
            cuenta[e["estado"]] = cuenta.get(e["estado"], 0) + 1
        return jsonify({
            "estaciones": list(estaciones),
            "resumen": {"total": len(estaciones), "estados": cuenta,
                        "caudal": round(sum(e["caudal"] for e in estaciones if e["estado"] == "WATERING"), 1),
                        "volumen": round(sum(e["volumen"] for e in estaciones if e["estado"] == "WATERING"), 1)},
            "eventos": list(eventos[:40]),
            "central": dict(central, host="%s:%s" % (CENTRAL_HOST, CENTRAL_PORT), admin=bool(ADMIN_TOKEN)),
            "ahora": time.time(),
        })


@app.route("/api/admin", methods=["POST"])
def api_admin():
    d = request.get_json(silent=True) or {}
    destino, accion = str(d.get("destino", "")).strip(), str(d.get("accion", "")).strip()
    if destino != "todas" and not re.fullmatch(r"[A-Za-z0-9_-]{1,30}", destino):
        return jsonify({"ok": False, "error": "Destino no válido."}), 400
    if accion not in ("iniciar", "bloquear", "activar"):
        return jsonify({"ok": False, "error": "Acción no válida."}), 400
    try:
        segundos = int(d.get("segundos", 20))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "error": "Los segundos deben ser un número."}), 400
    if accion == "iniciar" and not 1 <= segundos <= 600:
        return jsonify({"ok": False, "error": "Los segundos deben estar entre 1 y 600."}), 400
    ok, msg = enviar_a_central(destino, accion, segundos)
    with cerrojo:
        evento("Orden «%s» sobre %s: %s" % (accion, destino, msg), "ok" if ok else "mal")
    return jsonify({"ok": ok, "mensaje": msg, "error": None if ok else msg}), (200 if ok else 502)


if __name__ == "__main__":
    threading.Thread(target=vigilar_bd, daemon=True).start()
    threading.Thread(target=latido, daemon=True).start()
    evento("Panel iniciado", "info")
    app.run(host="0.0.0.0", port=WEB_PORT, threaded=True)
