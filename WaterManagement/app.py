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
  LAYOUT_PATH   fichero donde se guarda el mapa (posiciones y estaciones añadidas a mano)
                                                            (layout.json junto a central.db)
"""
import json
import os
import re
import socket
import tempfile
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

LAYOUT_PATH = os.environ.get("LAYOUT_PATH", "").strip() or os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "layout.json")
MAPA_W, MAPA_H = 900, 560      # tamaño del mapa (unidades del SVG); las posiciones se guardan en estas unidades
ID_VALIDO = re.compile(r"[A-Za-z0-9_-]{1,30}")

ESTADOS = ("AVAILABLE", "WATERING", "KO", "OUT_OF_SERVICE", "DISCONNECTED")
TEXTO_ESTADO = {"AVAILABLE": "disponible", "WATERING": "regando", "KO": "averiada (KO)",
                "OUT_OF_SERVICE": "fuera de servicio", "DISCONNECTED": "desconectada"}
NIVEL_ESTADO = {"AVAILABLE": "ok", "WATERING": "ok", "KO": "mal", "OUT_OF_SERVICE": "aviso", "DISCONNECTED": "info"}

cerrojo = threading.Lock()
estaciones = []          # última lectura de la BD
eventos = []             # sucesos deducidos de los cambios
central = {"alcanzable": None, "mensaje": "", "ts": 0.0, "bd": True}
# Diseño del mapa: lo gestiona SOLO el panel (no toca la BD de la central ni Kafka)
#   pos      {id: {x, y}}              posición que el usuario ha dado a cada estación arrastrándola
#   manuales {id: {ubicacion}}         estaciones añadidas desde la interfaz (aún sin registrar en CENTRAL)
layout = {"pos": {}, "manuales": {}}
cerrojo_layout = threading.Lock()


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


def cargar_layout():
    global layout
    try:
        with open(LAYOUT_PATH, encoding="utf-8") as f:
            d = json.load(f)
        layout = {"pos": d.get("pos", {}), "manuales": d.get("manuales", {})}
    except (OSError, ValueError):
        pass


def guardar_layout():
    """Guarda el mapa de forma atómica. Si la carpeta de la BD no admite escritura, usa la carpeta temporal."""
    global LAYOUT_PATH
    datos = json.dumps(layout, ensure_ascii=False, indent=1)
    for ruta in (LAYOUT_PATH, os.path.join(tempfile.gettempdir(), "layout.json")):
        try:
            tmp = ruta + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(datos)
            os.replace(tmp, ruta)
            LAYOUT_PATH = ruta
            return True
        except OSError:
            continue
    return False


def estaciones_con_manuales():
    """Estaciones de la BD + las añadidas a mano que todavía no están registradas (salen como desconectadas)."""
    lista = [dict(e, manual=e["id"] in layout["manuales"], registrada=True) for e in estaciones]
    en_bd = {e["id"] for e in estaciones}
    for i, m in layout["manuales"].items():
        if i not in en_bd:
            lista.append({"id": i, "ubicacion": m.get("ubicacion", ""), "estado": "DISCONNECTED", "caudal": 0, "volumen": 0,
                          "operario": "", "manual": True, "registrada": False})
    return lista


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/estaciones")
def api_estaciones():
    with cerrojo, cerrojo_layout:
        return jsonify(estaciones_con_manuales())


@app.route("/api/panel")
def api_panel():
    with cerrojo, cerrojo_layout:
        todas = estaciones_con_manuales()
        cuenta = {k: 0 for k in ESTADOS}
        for e in todas:
            cuenta[e["estado"]] = cuenta.get(e["estado"], 0) + 1
        return jsonify({
            "estaciones": todas,
            "layout": {"pos": layout["pos"], "w": MAPA_W, "h": MAPA_H},
            "resumen": {"total": len(todas), "estados": cuenta,
                        "caudal": round(sum(e["caudal"] for e in todas if e["estado"] == "WATERING"), 1),
                        "volumen": round(sum(e["volumen"] for e in todas if e["estado"] == "WATERING"), 1)},
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


def _datos():
    return request.get_json(silent=True) or {}


def _fallo(msg, codigo=400):
    return jsonify({"ok": False, "error": msg}), codigo


def _guardado():
    return jsonify({"ok": True}) if guardar_layout() else _fallo("No se pudo guardar el mapa en el servidor.", 500)


@app.route("/api/layout/pos", methods=["POST"])
def api_layout_pos():
    """El usuario ha soltado una estación en el mapa: se guarda su posición."""
    d = _datos()
    i = str(d.get("id", "")).strip()
    try:
        x, y = float(d.get("x")), float(d.get("y"))
    except (TypeError, ValueError):
        return _fallo("Posición no válida.")
    if not ID_VALIDO.fullmatch(i):
        return _fallo("Estación no válida.")
    with cerrojo_layout:
        layout["pos"][i] = {"x": round(min(max(x, 0), MAPA_W), 1), "y": round(min(max(y, 0), MAPA_H), 1)}
        return _guardado()


@app.route("/api/layout/estacion", methods=["POST"])
def api_layout_anadir():
    """Añade una estación al mapa desde la interfaz. Se muestra como desconectada hasta que un Monitor con ese ID se registre."""
    d = _datos()
    i, ub = str(d.get("id", "")).strip(), str(d.get("ubicacion", "")).strip()[:60]
    if not ID_VALIDO.fullmatch(i):
        return _fallo("El ID solo puede tener letras, números, guiones y _ (máximo 30), por ejemplo WS-05.")
    if not ub:
        return _fallo("Indica la ubicación de la estación.")
    with cerrojo, cerrojo_layout:
        if i in layout["manuales"] or any(e["id"] == i for e in estaciones):
            return _fallo("Ya existe una estación con el ID %s." % i)
        layout["manuales"][i] = {"ubicacion": ub}
        try:
            layout["pos"][i] = {"x": round(min(max(float(d["x"]), 0), MAPA_W), 1), "y": round(min(max(float(d["y"]), 0), MAPA_H), 1)}
        except (KeyError, TypeError, ValueError):
            pass
        evento("Estación %s (%s) añadida al mapa desde el panel; sin registrar en CENTRAL" % (i, ub), "info")
        return _guardado()


@app.route("/api/layout/quitar", methods=["POST"])
def api_layout_quitar():
    """Quita del mapa una estación añadida a mano. Las que están registradas en CENTRAL no se pueden quitar desde aquí."""
    i = str(_datos().get("id", "")).strip()
    with cerrojo, cerrojo_layout:
        if any(e["id"] == i for e in estaciones):
            return _fallo("%s está registrada en CENTRAL: solo se puede mover, no quitar." % i)
        if i not in layout["manuales"]:
            return _fallo("Esa estación no existe.", 404)
        del layout["manuales"][i]
        layout["pos"].pop(i, None)
        evento("Estación %s quitada del mapa" % i, "info")
        return _guardado()


@app.route("/api/layout/reset", methods=["POST"])
def api_layout_reset():
    """Vuelve a la colocación automática (por ubicación). Las estaciones añadidas a mano se conservan."""
    with cerrojo_layout:
        layout["pos"] = {}
        return _guardado()


def iniciar_hilos():
    """Arranca la lectura de la BD y el latido hacia CENTRAL (también lo usa la página única app_web.py)."""
    cargar_layout()
    threading.Thread(target=vigilar_bd, daemon=True).start()
    threading.Thread(target=latido, daemon=True).start()
    evento("Panel iniciado", "info")


if __name__ == "__main__":
    iniciar_hilos()
    app.run(host="0.0.0.0", port=WEB_PORT, threaded=True)
