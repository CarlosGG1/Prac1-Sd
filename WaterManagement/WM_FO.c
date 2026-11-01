/*
 * WM_FO  (Field Operator: aplicación del operario)
 *
 * El operario pide un riego a CENTRAL (por Kafka) y va viendo los pasos del proceso: validación,
 * autorización o denegación, caudal y volumen en tiempo real y el resumen final.
 *
 * Uso: WM_FO <IP_Broker> <Puerto_Broker> <ID_Operario> [fichero_servicios]
 * Ej.: WM_FO 127.0.0.1 9092 OP-1
 *      WM_FO 127.0.0.1 9092 OP-1 servicios.txt
 *
 * Fichero de servicios: una línea por riego con el formato  ID_WS#segundos  (los segundos son
 * opcionales; por defecto 20). Las líneas vacías o que empiezan por '//' se ignoran. Ejemplo:
 *      WS-01#10
 *      WS-02#15
 *      WS-01
 * Tras la conclusión de cada servicio (éxito o fracaso) se esperan 4 segundos y se pasa al siguiente.
 *
 * Con el programa en marcha también se puede escribir en el teclado:
 *      WS-01 15      pide riego en WS-01 durante 15 s (los segundos son opcionales)
 *      f fichero     ejecuta un fichero de servicios
 *      q             sale
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>

#include "wm_kafka.h"

#define ESPERA_ENTRE_SERVICIOS 4 /* segundos entre un servicio y el siguiente (enunciado) */
#define MARGEN_RESPUESTA 25      /* segundos extra de espera por encima de la duración del riego */

char *operario;                  /* ID del operario (argumento) */
rd_kafka_t *productor;
rd_kafka_t *consumidor;
volatile int terminado = 0;      /* 1 cuando CENTRAL ha concluido la petición actual (resumen o denegación) */

/*
 * Hilo que recibe los mensajes de Kafka y los muestra. Solo enseña los dirigidos a ESTE operario:
 *   - notificaciones_fo:  operario#TIPO#texto
 *   - eventos_ws:         RIEGO#ws#operario#caudal#volumen   (progreso en tiempo real)
 */
void *recibir_mensajes(void *arg)
{
	while (1)
	{
		rd_kafka_message_t *m = rd_kafka_consumer_poll(consumidor, 500);
		if (!m) continue;

		if (!m->err)
		{
			char buffer[512];
			char *campos[6];
			size_t len = m->len;

			if (len >= sizeof(buffer)) len = sizeof(buffer) - 1;
			memcpy(buffer, m->payload, len);
			buffer[len] = '\0';
			int n = dividir_campos(buffer, campos, 6);

			if (strcmp(campos[0], "RIEGO") == 0 && n >= 5)
			{
				if (strcmp(campos[2], operario) == 0)
					printf("   [%s] REGANDO  caudal %s L/min | volumen acumulado %s L\n", campos[1], campos[3], campos[4]);
			}
			else if (strcmp(campos[0], operario) == 0 && n >= 3)
			{
				const char *tipo = campos[1];
				const char *color = "\033[0m";

				if (strcmp(tipo, "OK") == 0) color = "\033[0;32m";
				else if (strcmp(tipo, "DENEGADO") == 0) color = "\033[0;31m";
				else if (strcmp(tipo, "RESUMEN") == 0) color = "\033[0;34m";

				printf("%s[CENTRAL] %s: %s\033[0m\n", color, tipo, campos[2]);

				if (strcmp(tipo, "DENEGADO") == 0 || strcmp(tipo, "RESUMEN") == 0)
					terminado = 1; /* la petición ha concluido (con éxito o fracaso) */
			}
		}
		rd_kafka_message_destroy(m);
	}
	return NULL;
}

/* Envía una petición de riego y espera a que CENTRAL la concluya. */
void solicitar_riego(const char *ws, int segundos)
{
	char peticion[128];
	int esperas;

	if (segundos <= 0) segundos = DURACION_POR_DEFECTO;

	terminado = 0;
	snprintf(peticion, sizeof(peticion), "PETICION#%s#%s#%d", operario, ws, segundos);
	printf("\n[FO %s] Solicitando riego en %s durante %d s...\n", operario, ws, segundos);
	if (kafka_publicar(productor, TOPIC_PETICIONES, operario, peticion) != 0)
	{
		printf("[FO %s] No se pudo enviar la petición (¿Kafka caído?)\n", operario);
		return;
	}

	/* Esperamos la conclusión (resumen o denegación), con un tiempo máximo por si CENTRAL no responde */
	for (esperas = 0; esperas < (segundos + MARGEN_RESPUESTA) * 10 && !terminado; esperas++)
		usleep(100000);

	if (!terminado)
		printf("\033[0;33m[FO %s] CENTRAL no ha concluido la petición a tiempo. Se pasa al siguiente servicio.\033[0m\n", operario);
}

/* Lee un fichero de servicios y los va solicitando uno a uno */
void ejecutar_fichero(const char *ruta)
{
	char linea[128], ws[64];
	int segundos;
	FILE *f = fopen(ruta, "r");

	if (!f)
	{
		printf("[FO %s] No se puede abrir el fichero '%s'\n", operario, ruta);
		return;
	}
	printf("[FO %s] Ejecutando servicios del fichero '%s'\n", operario, ruta);

	while (fgets(linea, sizeof(linea), f) != NULL)
	{
		linea[strcspn(linea, "\r\n")] = '\0';
		if (linea[0] == '\0' || strncmp(linea, "//", 2) == 0) continue;

		segundos = 0;
		if (sscanf(linea, "%63[^# \t]#%d", ws, &segundos) < 1) continue;

		solicitar_riego(ws, segundos);
		printf("[FO %s] Esperando %d s antes del siguiente servicio...\n", operario, ESPERA_ENTRE_SERVICIOS);
		sleep(ESPERA_ENTRE_SERVICIOS);
	}
	fclose(f);
	printf("[FO %s] Fichero '%s' terminado.\n", operario, ruta);
}

int main(int argc, char *argv[])
{
	char *ip_broker, *puerto_broker;
	char grupo[80], linea[128], ws[64], ruta[100];
	int segundos;
	pthread_t hilo;

	if (argc != 4 && argc != 5)
	{
		fprintf(stderr, "Sintaxis: %s <IP_Broker> <Puerto_Broker> <ID_Operario> [fichero_servicios]\n", argv[0]);
		fprintf(stderr, "Ejemplo : %s 127.0.0.1 9092 OP-1 servicios.txt\n", argv[0]);
		return 1;
	}

	ip_broker = argv[1];
	puerto_broker = argv[2];
	operario = argv[3];

	setvbuf(stdout, NULL, _IOLBF, 0);

	printf("========================================\n");
	printf("   OPERADOR DE CAMPO (WM_FO) - %s\n", operario);
	printf("========================================\n");

	productor = kafka_crear_productor(ip_broker, puerto_broker);

	/* Grupo propio en cada ejecución: así este operario no recibe mensajes antiguos */
	snprintf(grupo, sizeof(grupo), "wm_fo_%s_%ld_%d", operario, (long)time(NULL), (int)getpid());
	const char *topics[] = {TOPIC_NOTIFICACIONES, TOPIC_EVENTOS};
	consumidor = kafka_crear_consumidor(ip_broker, puerto_broker, grupo, topics, 2);
	if (!productor || !consumidor)
	{
		fprintf(stderr, "[FO] No se pudo conectar con Kafka (%s:%s)\n", ip_broker, puerto_broker);
		return 1;
	}
	printf("[FO %s] Conectado a Kafka (%s:%s)\n", operario, ip_broker, puerto_broker);

	pthread_create(&hilo, NULL, recibir_mensajes, NULL);

	if (argc == 5)
		ejecutar_fichero(argv[4]);

	/* Modo interactivo */
	printf("\nEscribe  <ID_WS> [segundos]  para pedir un riego, 'l' para ver las estaciones disponibles, 'f <fichero>' para ejecutar un fichero, 'q' para salir.\n");
	while (fgets(linea, sizeof(linea), stdin) != NULL)
	{
		linea[strcspn(linea, "\r\n")] = '\0';

		if (linea[0] == '\0') continue;
		if (strcmp(linea, "q") == 0) break;

		if (strcmp(linea, "l") == 0)
		{
			/* Pide a CENTRAL la lista de estaciones disponibles; la respuesta llega como notificación INFO */
			char peticion[128];
			snprintf(peticion, sizeof(peticion), "LISTAR#%s", operario);
			kafka_publicar(productor, TOPIC_PETICIONES, operario, peticion);
			continue;
		}

		if (sscanf(linea, "f %99s", ruta) == 1)
		{
			ejecutar_fichero(ruta);
			continue;
		}

		segundos = 0;
		if (sscanf(linea, "%63s %d", ws, &segundos) >= 1)
			solicitar_riego(ws, segundos);
	}

	printf("[FO %s] Fin.\n", operario);
	return 0;
}
