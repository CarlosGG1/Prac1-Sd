from flask import Flask, render_template, jsonify
import sqlite3

app = Flask(__name__)

def obtener_estaciones():
    try:
        conn = sqlite3.connect('central.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT ID, UBICACION, ESTADO FROM ESTACIONES;")
        filas = cursor.fetchall()
        conn.close()
        
        estaciones = []
        for fila in filas:
            estaciones.append({
                "id": fila["ID"],
                "ubicacion": fila["UBICACION"],
                "estado": fila["ESTADO"]
            })
        return estaciones
    except Exception as e:
        print("Error al leer la BD:", e)
        return []

@app.route('/')
def index():
    estaciones = obtener_estaciones()
    return render_template('index.html', estaciones=estaciones)

@app.route('/api/estaciones')
def api_estaciones():
    return jsonify(obtener_estaciones())

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)