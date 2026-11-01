"""Panel web de WaterManagement: lee la BD de la central (SQLite) y la muestra en tiempo real.

Uso:  python app.py            (usa central.db de esta carpeta)
      DB_PATH=/ruta/central.db python app.py
Se abre en http://localhost:5000
"""
import os
import sqlite3

from flask import Flask, jsonify, render_template

app = Flask(__name__)
DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "central.db"))


def obtener_estaciones():
    try:
        # Solo lectura: el panel nunca modifica la base de datos de la central
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=2)
        conn.row_factory = sqlite3.Row
        filas = conn.execute(
            "SELECT ID, UBICACION, ESTADO, CAUDAL, VOLUMEN, OPERARIO FROM ESTACIONES ORDER BY ID;"
        ).fetchall()
        conn.close()
    except Exception as e:
        print("Error al leer la BD:", e)
        return []

    return [
        {
            "id": f["ID"],
            "ubicacion": f["UBICACION"] or "",
            "estado": f["ESTADO"] or "DISCONNECTED",
            "caudal": f["CAUDAL"] or 0,
            "volumen": f["VOLUMEN"] or 0,
            "operario": f["OPERARIO"] or "",
        }
        for f in filas
    ]


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/estaciones")
def api_estaciones():
    return jsonify(obtener_estaciones())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
