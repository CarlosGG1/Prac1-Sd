#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <librdkafka/rdkafka.h>

static int run = 1;

// Manejador para salir limpiamente con Ctrl + C
static void stop(int sig) {
    run = 0;
    printf("\n[WM_FO] Cerrando consumidor de Kafka...\n\r");
}

int main(int argc, char *argv[]) {
    rd_kafka_t *rk;
    rd_kafka_conf_t *conf;
    rd_kafka_resp_err_t err;
    char errstr[512];
    const char *brokers;
    const char *topic;
    rd_kafka_topic_partition_list_t *topics;

    if (argc != 3) {
        fprintf(stderr, "Sintaxis: %s <IP:Puerto_Broker> <Topic>\n", argv[0]);
        fprintf(stderr, "Ejemplo : %s 127.0.0.1:9092 alertas_agua\n", argv[0]);
        return 1;
    }

    brokers = argv[1];
    topic = argv[2];

    printf("========================================\n\r");
    printf("   OPERADOR DE CAMPO - CONSUMIDOR (WM_FO)\n\r");
    printf("========================================\n\r");

    // Configuración del consumidor de Kafka
    conf = rd_kafka_conf_new();

    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error config brokers: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return 1;
    }

    if (rd_kafka_conf_set(conf, "group.id", "water-management-fo-group", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "Error config group.id: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return 1;
    }

    rd_kafka_conf_set(conf, "auto.offset.reset", "latest", errstr, sizeof(errstr));

    // Crear instancia del consumidor
    rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr, "Error creando consumidor en WM_FO: %s\n", errstr);
        return 1;
    }

    rd_kafka_poll_set_consumer(rk);

    // Suscribirse al topic de alertas
    topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, topic, 0);
    err = rd_kafka_subscribe(rk, topics);
    rd_kafka_topic_partition_list_destroy(topics);

    if (err) {
        fprintf(stderr, "Error suscribiéndose al topic %s: %s\n", topic, rd_kafka_err2str(err));
        rd_kafka_destroy(rk);
        return 1;
    }

    printf("[i] Conectado al broker: %s\n\r", brokers);
    printf("[i] Suscrito al canal: '%s'\n\r", topic);
    printf("[i] Esperando alertas en tiempo real... (Ctrl+C para salir)\n\r\n\r");

    signal(SIGINT, stop);

    // Bucle de escucha de Kafka
    while (run) {
        rd_kafka_message_t *rkmessage;

        rkmessage = rd_kafka_consumer_poll(rk, 1000);
        if (!rkmessage) continue;

        if (rkmessage->err) {
            if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                rd_kafka_message_destroy(rkmessage);
                continue;
            }
            fprintf(stderr, "Error en mensaje: %s\n", rd_kafka_message_errstr(rkmessage));
            rd_kafka_message_destroy(rkmessage);
            continue;
        }

        // Mensaje de alerta recibido con éxito desde la Central
        printf("----------------------------------------\n\r");
        printf("\033[0;31m[!] ALERTA RECIBIDA DESDE KAFKA\033[0m\n\r");
        printf(" Contenido : %.*s\n\r", (int)rkmessage->len, (char *)rkmessage->payload);
        printf("----------------------------------------\n\r");

        rd_kafka_message_destroy(rkmessage);
    }

    rd_kafka_consumer_close(rk);
    rd_kafka_destroy(rk);
    printf("[WM_FO] Finalizado.\n\r");

    return 0;
}