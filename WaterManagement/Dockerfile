# Usamos una imagen oficial de C (Linux)
FROM gcc:latest

# Instalamos SQLite
RUN apt-get update && apt-get install -y libsqlite3-dev sqlite3

# Copiamos nuestro código a una carpeta temporal
COPY WM_Central.c /tmp/WM_Central.c

# Compilamos y guardamos el ejecutable en /bin (para que el volumen no lo aplaste)
RUN gcc /tmp/WM_Central.c -o /bin/WM_Central -lsqlite3

# Usamos /app como espacio de trabajo. Aquí es donde se creará central.db
WORKDIR /app

# Al arrancar, ejecutamos el servidor (ya no hace falta el ./)
CMD ["WM_Central", "8080"]
