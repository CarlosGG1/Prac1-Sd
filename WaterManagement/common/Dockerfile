FROM gcc:latest

RUN apt-get update && apt-get install -y libsqlite3-dev sqlite3 librdkafka-dev

COPY central/WM_Central.c common/wm_kafka.h /tmp/

RUN gcc /tmp/WM_Central.c -o /bin/WM_Central -lsqlite3 -lrdkafka

WORKDIR /app

ENV PORT=8080 \
    BROKER_HOST=127.0.0.1 \
    BROKER_PORT=9092

EXPOSE 8080

CMD ["sh", "-c", "exec WM_Central ${PORT} ${BROKER_HOST} ${BROKER_PORT}"]
