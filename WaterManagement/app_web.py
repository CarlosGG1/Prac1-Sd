"""PÁGINA ÚNICA de WaterManagement: un solo enlace con pestañas.

  /            portal con pestañas (Panel de CENTRAL | Operario | Estación) y vista "todo a la vez"
  /central/    panel de CENTRAL         (app.py)
  /operario/   interfaz del operario    (app_operario.py)
  /estacion/   interfaz de la estación  (app_estacion.py)

Cada interfaz sigue siendo la misma de siempre; aquí solo se montan juntas en un único servidor y puerto.

Variables de entorno (además de las de cada interfaz):
  MODULOS   interfaces que se activan, separadas por comas   (central,operario,estacion)
  WEB_PORT  puerto de la página única                        (8000)
"""
import os

from flask import Flask, render_template
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.serving import run_simple

MODULOS = [m.strip() for m in os.environ.get("MODULOS", "central,operario,estacion").split(",") if m.strip()]
WEB_PORT = int(os.environ.get("WEB_PORT", "8000") or 8000)
# Las interfaces leen WEB_PORT al importarse; aquí no se usa para ellas
os.environ["WEB_PORT"] = "0"

portal = Flask(__name__)
montajes = {}
NOMBRES = {"central": "Panel de CENTRAL", "operario": "Operario", "estacion": "Estación"}

if "central" in MODULOS:
    import app as panel
    montajes["/central"] = panel.app
if "operario" in MODULOS:
    import app_operario as operario
    montajes["/operario"] = operario.app
if "estacion" in MODULOS:
    import app_estacion as estacion
    montajes["/estacion"] = estacion.app


@portal.route("/")
def portal_inicio():
    return render_template("portal.html", modulos=[(m, NOMBRES[m]) for m in MODULOS if m in NOMBRES])


def arrancar():
    if "central" in MODULOS:
        panel.iniciar_hilos()
    if "operario" in MODULOS:
        operario.programa.iniciar()
    if "estacion" in MODULOS:
        estacion.arrancar()


if __name__ == "__main__":
    arrancar()
    aplicacion = DispatcherMiddleware(portal, montajes)
    run_simple("0.0.0.0", WEB_PORT, aplicacion, threaded=True, use_reloader=False)
