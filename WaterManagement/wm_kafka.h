/*
 * wm_kafka.h - funciones de Kafka compartidas por WM_Central, WM_WS_E y WM_FO.
 * Son "static" para poder incluir este fichero directamente en cada programa.
 *
 * TOPICS DEL SISTEMA (todos los mensajes son texto con campos separados por '#'):
 *
 *   peticiones_riego   FO (o menú de la WS) -> CENTRAL   PETICION#operario#ws#segundos
 *   notificaciones_fo  CENTRAL -> FO                      operario#TIPO#texto
 *                                                         (TIPO: INFO, OK, DENEGADO, RESUMEN)
 *   ordenes_ws         CENTRAL -> Engine de la WS         ABRIR#ws#operario#segundos | CERRAR#ws#operario
 *   eventos_ws         Engine -> CENTRAL y FO             INICIO#ws#operario
 *                                                         RIEGO#ws#operario#caudal#volumen   (cada segundo)
 *                                                         FIN#ws#operario#volumen#segundos#motivo
 *   alertas_agua       CENTRAL -> quien quiera           JSON de avería
 *
 * Todos los mensajes llevan CLAVE (ws u operario). Kafka manda las claves iguales a la misma
 * partición, así que los mensajes de una misma estación u operario llegan EN ORDEN.
 */
#ifndef WM_KAFKA_H
#define WM_KAFKA_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <librdkafka/rdkafka.h>

#define TOPIC_PETICIONES     "peticiones_riego"
#define TOPIC_NOTIFICACIONES "notificaciones_fo"
#define TOPIC_ORDENES        "ordenes_ws"
#define TOPIC_EVENTOS        "eventos_ws"
#define TOPIC_ALERTAS        "alertas_agua"

#define DURACION_POR_DEFECTO 20 /* segundos de riego si la petición no indica duración */

/* Crea un productor de Kafka. Devuelve NULL si falla. */
__attribute__((unused)) static rd_kafka_t *kafka_crear_productor(const char *ip, const char *puerto)
{
	char errstr[512], brokers[256];
	rd_kafka_t *rk;
	rd_kafka_conf_t *conf = rd_kafka_conf_new();

	snprintf(brokers, sizeof(brokers), "%s:%s", ip, puerto);
	if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		fprintf(stderr, "Error config Kafka: %s\n", errstr);
		rd_kafka_conf_destroy(conf);
		return NULL;
	}
	/* Si el broker no responde, descartamos el mensaje a los 5 s en vez de reintentar 5 minutos */
	rd_kafka_conf_set(conf, "message.timeout.ms", "5000", errstr, sizeof(errstr));

	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!rk)
	{
		fprintf(stderr, "Error creando productor Kafka: %s\n", errstr);
		rd_kafka_conf_destroy(conf);
		return NULL;
	}
	return rk;
}

/* Publica 'mensaje' en 'topic' con la clave indicada. Devuelve 0 si se encoló bien y -1 si no. */
__attribute__((unused)) static int kafka_publicar(rd_kafka_t *rk, const char *topic, const char *clave, const char *mensaje)
{
	rd_kafka_resp_err_t err;
	int intentos = 0;

	do
	{
		err = rd_kafka_producev(rk,
								RD_KAFKA_V_TOPIC(topic),
								RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
								RD_KAFKA_V_KEY((void *)clave, strlen(clave)),
								RD_KAFKA_V_VALUE((void *)mensaje, strlen(mensaje)),
								RD_KAFKA_V_END);
		if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
			rd_kafka_poll(rk, 100); /* la cola está llena: dejamos que se vacíe un poco y reintentamos */
	} while (err == RD_KAFKA_RESP_ERR__QUEUE_FULL && ++intentos < 5);

	rd_kafka_poll(rk, 0); /* atiende las confirmaciones de entrega pendientes */
	if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
	{
		fprintf(stderr, "[KAFKA] Error publicando en %s: %s\n", topic, rd_kafka_err2str(err));
		return -1;
	}
	return 0;
}

/*
 * Crea un productor temporal, publica un mensaje, espera a que salga y lo destruye.
 * Para procesos que publican muy de vez en cuando (por ejemplo, los hijos de CENTRAL).
 */
__attribute__((unused)) static int kafka_publicar_una_vez(const char *ip, const char *puerto, const char *topic, const char *clave, const char *mensaje)
{
	rd_kafka_t *rk = kafka_crear_productor(ip, puerto);
	if (!rk) return -1;

	int rc = kafka_publicar(rk, topic, clave, mensaje);
	rd_kafka_flush(rk, 2000);
	if (rd_kafka_outq_len(rk) > 0)
	{
		fprintf(stderr, "[KAFKA] No se pudo entregar el mensaje (broker %s:%s no responde)\n", ip, puerto);
		rc = -1;
	}
	rd_kafka_destroy(rk);
	return rc;
}

/*
 * Crea un consumidor del grupo 'grupo', lo suscribe a los topics indicados y ESPERA a que Kafka
 * le asigne las particiones. Esa espera es importante: con "latest", un mensaje que llegue antes
 * de tener la asignación se perdería.
 */
__attribute__((unused)) static rd_kafka_t *kafka_crear_consumidor(const char *ip, const char *puerto, const char *grupo, const char *topics[], int ntopics)
{
	char errstr[512], brokers[256];
	rd_kafka_t *rk;
	rd_kafka_conf_t *conf = rd_kafka_conf_new();
	int i;

	snprintf(brokers, sizeof(brokers), "%s:%s", ip, puerto);
	if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
		rd_kafka_conf_set(conf, "group.id", grupo, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
	{
		fprintf(stderr, "Error config Kafka: %s\n", errstr);
		rd_kafka_conf_destroy(conf);
		return NULL;
	}
	rd_kafka_conf_set(conf, "auto.offset.reset", "latest", errstr, sizeof(errstr));
	rd_kafka_conf_set(conf, "allow.auto.create.topics", "true", errstr, sizeof(errstr)); /* crea el topic si no existe */

	rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
	if (!rk)
	{
		fprintf(stderr, "Error creando consumidor Kafka: %s\n", errstr);
		rd_kafka_conf_destroy(conf);
		return NULL;
	}
	rd_kafka_poll_set_consumer(rk);

	rd_kafka_topic_partition_list_t *lista = rd_kafka_topic_partition_list_new(ntopics);
	for (i = 0; i < ntopics; i++)
		rd_kafka_topic_partition_list_add(lista, topics[i], RD_KAFKA_PARTITION_UA);
	rd_kafka_resp_err_t err = rd_kafka_subscribe(rk, lista);
	rd_kafka_topic_partition_list_destroy(lista);
	if (err)
	{
		fprintf(stderr, "Error suscribiéndose: %s\n", rd_kafka_err2str(err));
		rd_kafka_destroy(rk);
		return NULL;
	}

	/* Esperamos (hasta ~20 s) a que el broker nos asigne particiones */
	for (i = 0; i < 40; i++)
	{
		rd_kafka_message_t *m = rd_kafka_consumer_poll(rk, 500); /* hace falta hacer poll para que avance la asignación */
		if (m) rd_kafka_message_destroy(m);

		rd_kafka_topic_partition_list_t *asignadas = NULL;
		int listo = (rd_kafka_assignment(rk, &asignadas) == RD_KAFKA_RESP_ERR_NO_ERROR && asignadas != NULL && asignadas->cnt > 0);
		if (asignadas) rd_kafka_topic_partition_list_destroy(asignadas);
		if (listo) break;
	}
	if (i == 40)
		fprintf(stderr, "[KAFKA] Aviso: el broker %s no ha asignado particiones todavía (¿está en marcha?)\n", brokers);

	return rk;
}

/*
 * Divide 'texto' por '#' sobre el propio buffer (lo modifica) y deja en campos[] los punteros
 * a cada trozo. Devuelve cuántos campos hay (como mucho 'max'; el último se queda con el resto).
 */
__attribute__((unused)) static int dividir_campos(char *texto, char *campos[], int max)
{
	int n = 0;
	char *p = texto;

	while (n < max)
	{
		campos[n++] = p;
		char *sep = (n < max) ? strchr(p, '#') : NULL;
		if (!sep) break;
		*sep = '\0';
		p = sep + 1;
	}
	return n;
}

#endif
