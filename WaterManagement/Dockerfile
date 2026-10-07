# Usamos una imagen oficial de C (Linux)
FROM gcc:latest

# Instalamos SQLite y la librería de Kafka
RUN apt-get update && apt-get install -y libsqlite3-dev sqlite3 librdkafka-dev

# Copiamos nuestro código a una carpeta temporal
COPY WM_Central.c /tmp/WM_Central.c

# Compilamos y guardamos el ejecutable enlazando SQLite y Kafka
RUN gcc /tmp/WM_Central.c -o /bin/WM_Central -lsqlite3 -lrdkafka

# Usamos /app como espacio de trabajo
WORKDIR /app

# Al arrancar, ejecutamos el servidor
CMD ["WM_Central", "8080"]
