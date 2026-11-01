#!/bin/bash
# Crea los topics de Kafka que usa el sistema (el contenedor del broker se llama "broker").
# Uso: bash crear_topics.sh
for t in peticiones_riego notificaciones_fo ordenes_ws eventos_ws alertas_agua; do
  docker exec broker /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
    --topic "$t" --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092
done
echo "Topics disponibles:"
docker exec broker /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
