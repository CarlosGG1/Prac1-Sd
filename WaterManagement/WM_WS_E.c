/*
 * WM_WS_E  (Engine de la Watering Station)
 *
 * Simula el caudalímetro y la electroválvula de la estación. Hace TRES cosas a la vez:
 *   - Hilo "monitor":  es CLIENTE de sockets de su Monitor (WM_WS_M). El Monitor le asigna el ID
 *                      y le manda un HEARTBEAT cada segundo; el Engine contesta OK o KO.
 *   - Hilo "teclado":  menú de la estación (iniciar riego, finalizar, simular fuga, reparar).
 *   - Hilo principal:  escucha por Kafka las órdenes de CENTRAL (ABRIR / CERRAR) y, mientras
 *                      riega, publica cada segundo el caudal y el volumen acumulado.
 *
 * Uso: WM_WS_E <IP_Broker> <Puerto_Broker> <IP_Monitor> <Puerto_Monitor>
 * Ej.: WM_WS_E 127.0.0.1 9092 127.0.0.1 5000
 */
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <pthread.h>
#include <time.h>

#include "wm_kafka.h"

/*
 * Estado compartido entre hilos (todos los hilos de un proceso comparten las variables globales;
 * por eso aquí NO usamos fork(): un proceso hijo tendría su propia copia y no vería los cambios).
 */
volatile int estado_salud = 1;        /* 1 = OK, 0 = KO (fuga) */
volatile int pedir_riego_manual = 0;  /* el teclado pide iniciar un riego desde la propia estación */
volatile int pedir_fin_manual = 0;    /* el teclado pide finalizar el riego en curso */

char id_estacion[64] = "";            /* lo asigna el Monitor al conectar */
pthread_mutex_t m_id = PTHREAD_MUTEX_INITIALIZER; /* protege id_estacion (lo escribe un hilo y lo lee otro) */

/* Estado del riego (solo lo toca el hilo principal) */
int regando = 0;
char operario_riego[64] = "";
time_t inicio_riego = 0;
int duracion_riego = 0;
double volumen_riego = 0.0;
time_t ultimo_tick = 0;

rd_kafka_t *productor;

void obtener_id(char *destino, int tam)
{
	pthread_mutex_lock(&m_id);
	snprintf(destino, tam, "%s", id_estacion);
	pthread_mutex_unlock(&m_id);
}

/* Abre una conexión TCP con host:puerto (acepta IP o nombre de host). Devuelve el socket o -1. */
int conectar(const char *host, const char *puerto)
{
	struct addrinfo pistas, *resultado, *p;
	int sock = -1;

	memset(&pistas, 0, sizeof(pistas));
	pistas.ai_family = AF_INET;
	pistas.ai_socktype = SOCK_STREAM;

	if (getaddrinfo(host, puerto, &pistas, &resultado) != 0) return -1;

	for (p = resultado; p != NULL; p = p->ai_next)
	{
		sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
		if (sock == -1) continue;
		if (connect(sock, p->ai_addr, p->ai_addrlen) == 0) break;
		close(sock);
		sock = -1;
	}
	freeaddrinfo(resultado);
	return sock;
}

/* ======================= HILO DEL TECLADO ======================= */

void *leer_teclado(void *arg)
{
	char linea[64];

	printf("[Engine] Menú de la estación (letra + Enter):\n");
	printf("         i = INICIAR riego manual | f = FINALIZAR riego | k = simular FUGA | r = fuga REPARADA\n");
	while (fgets(linea, sizeof(linea), stdin) != NULL)
	{
		switch (linea[0])
		{
		case 'i': case 'I':
			pedir_riego_manual = 1;
			break;
		case 'f': case 'F':
			pedir_fin_manual = 1;
			break;
		case 'k': case 'K':
			estado_salud = 0;
			printf("[Engine] \033[0;31m¡Fuga simulada! El estado pasa a KO.\033[0m\n");
			break;
		case 'r': case 'R':
			estado_salud = 1;
			printf("[Engine] \033[0;32mFuga reparada. El estado vuelve a OK.\033[0m\n");
			break;
		}
	}
	return NULL; /* stdin cerrado (por ejemplo, Docker sin -it): el Engine sigue funcionando sin teclado */
}

/* ======================= HILO DEL MONITOR (SOCKETS) ======================= */

char *ip_monitor, *puerto_monitor;

void *atender_monitor(void *arg)
{
	char mensaje[1024];
	int s, n;

	/* Conectar con el Monitor y atenderlo; si se cae, reintentar */
	while (1)
	{
		s = conectar(ip_monitor, puerto_monitor);
		if (s == -1)
		{
			fprintf(stderr, "[Engine] Monitor %s:%s no disponible. Reintentando en 2 s...\n", ip_monitor, puerto_monitor);
			sleep(2);
			continue;
		}
		printf("[Engine] Conectado con el Monitor (%s:%s).\n", ip_monitor, puerto_monitor);

		while ((n = read(s, mensaje, sizeof(mensaje) - 1)) > 0)
		{
			mensaje[n] = '\0';
			mensaje[strcspn(mensaje, "\r\n")] = '\0';

			if (strncmp(mensaje, "ASIGNACION#", 11) == 0)
			{
				/* El Monitor nos dice con qué ID está dada de alta esta estación */
				pthread_mutex_lock(&m_id);
				snprintf(id_estacion, sizeof(id_estacion), "%s", mensaje + 11);
				pthread_mutex_unlock(&m_id);
				printf("[Engine] ID asignado por el Monitor: %s\n", mensaje + 11);
				write(s, "STATUS#OK", strlen("STATUS#OK"));
			}
			else if (strcmp(mensaje, "HEARTBEAT") == 0)
			{
				const char *resp = (estado_salud == 1) ? "STATUS#OK" : "STATUS#KO#FUGA_DETECTADA";
				write(s, resp, strlen(resp));
			}
			else
			{
				write(s, "STATUS#ERROR#Comando desconocido", strlen("STATUS#ERROR#Comando desconocido"));
			}
		}

		printf("[Engine] Conexión con el Monitor perdida.\n");
		close(s);
		sleep(2);
	}
	return NULL;
}

/* ======================= RIEGO (HILO PRINCIPAL) ======================= */

void publicar_evento(const char *ws, const char *mensaje)
{
	kafka_publicar(productor, TOPIC_EVENTOS, ws, mensaje);
}

/* Cierra la electroválvula y avisa de que el riego ha terminado: FIN#ws#op#volumen#segundos#motivo */
void terminar_riego(const char *ws, const char *motivo)
{
	char mensaje[256];
	int segundos = (int)(time(NULL) - inicio_riego);

	snprintf(mensaje, sizeof(mensaje), "FIN#%s#%s#%.1f#%d#%s", ws, operario_riego, volumen_riego, segundos, motivo);
	publicar_evento(ws, mensaje);
	printf("[Engine] \033[0;34mElectroválvula CERRADA. Riego finalizado (%s): %.1f L en %d s\033[0m\n", motivo, volumen_riego, segundos);
	regando = 0;
}

/* Orden recibida de CENTRAL: ABRIR#ws#operario#segundos | CERRAR#ws#operario */
void procesar_orden(const char *payload, size_t len, const char *mi_id)
{
	char buffer[256];
	char *campos[5];

	if (len >= sizeof(buffer)) len = sizeof(buffer) - 1;
	memcpy(buffer, payload, len);
	buffer[len] = '\0';

	int n = dividir_campos(buffer, campos, 5);
	if (n < 3 || strcmp(campos[1], mi_id) != 0) return; /* la orden es para otra estación */

	if (strcmp(campos[0], "ABRIR") == 0 && n >= 4)
	{
		char evento[256];

		if (regando)
		{
			snprintf(evento, sizeof(evento), "FIN#%s#%s#0#0#RECHAZADO_OCUPADA", mi_id, campos[2]);
			publicar_evento(mi_id, evento);
			printf("[Engine] Orden ABRIR rechazada: ya estoy regando.\n");
		}
		else if (estado_salud == 0)
		{
			snprintf(evento, sizeof(evento), "FIN#%s#%s#0#0#RECHAZADO_FUGA", mi_id, campos[2]);
			publicar_evento(mi_id, evento);
			printf("[Engine] Orden ABRIR rechazada: hay una fuga.\n");
		}
		else
		{
			regando = 1;
			snprintf(operario_riego, sizeof(operario_riego), "%s", campos[2]);
			duracion_riego = atoi(campos[3]);
			if (duracion_riego <= 0) duracion_riego = DURACION_POR_DEFECTO;
			inicio_riego = time(NULL);
			ultimo_tick = inicio_riego;
			volumen_riego = 0.0;

			snprintf(evento, sizeof(evento), "INICIO#%s#%s", mi_id, operario_riego);
			publicar_evento(mi_id, evento);
			printf("[Engine] \033[0;32mElectroválvula ABIERTA. Riego para el operario %s durante %d s\033[0m\n", operario_riego, duracion_riego);
		}
	}
	else if (strcmp(campos[0], "CERRAR") == 0)
	{
		if (regando) terminar_riego(mi_id, "ORDEN_CENTRAL");
	}
}

int main(int argc, char *argv[])
{
	char *ip_broker, *puerto_broker;
	char mi_id[64];
	char grupo[64];
	pthread_t hilo_teclado, hilo_monitor;

	if (argc != 5)
	{
		fprintf(stderr, "Error. Faltan argumentos\r\n");
		fprintf(stderr, "Sintaxis: %s <IP_Broker> <Puerto_Broker> <IP_Monitor> <Puerto_Monitor>\n\r", argv[0]);
		fprintf(stderr, "Ejemplo : %s 127.0.0.1 9092 127.0.0.1 5000\n\r", argv[0]);
		return 1;
	}

	ip_broker = argv[1];
	puerto_broker = argv[2];
	ip_monitor = argv[3];
	puerto_monitor = argv[4];

	setvbuf(stdout, NULL, _IOLBF, 0); /* salida por líneas: los logs salen al momento también bajo Docker */
	signal(SIGPIPE, SIG_IGN);
	srand(time(NULL) ^ getpid());

	/* Kafka: un productor (eventos de riego) y un consumidor (órdenes de CENTRAL) */
	productor = kafka_crear_productor(ip_broker, puerto_broker);

	/*
	 * Grupo propio en cada ejecución (con la hora): así, al reiniciar el Engine no se le entregan
	 * órdenes antiguas que se quedaron en Kafka mientras estaba apagado.
	 */
	snprintf(grupo, sizeof(grupo), "wm_engine_%ld_%d", (long)time(NULL), (int)getpid());
	const char *topics[] = {TOPIC_ORDENES};
	rd_kafka_t *consumidor = kafka_crear_consumidor(ip_broker, puerto_broker, grupo, topics, 1);
	if (!productor || !consumidor)
	{
		fprintf(stderr, "[Engine] No se pudo conectar con Kafka (%s:%s)\n", ip_broker, puerto_broker);
		return 1;
	}
	printf("[Engine] Conectado a Kafka (%s:%s). Esperando órdenes de CENTRAL...\n", ip_broker, puerto_broker);

	pthread_create(&hilo_teclado, NULL, leer_teclado, NULL);
	pthread_create(&hilo_monitor, NULL, atender_monitor, NULL);

	/* Bucle principal */
	while (1)
	{
		rd_kafka_message_t *m = rd_kafka_consumer_poll(consumidor, 200);
		obtener_id(mi_id, sizeof(mi_id));

		if (m)
		{
			/* Sin ID no sabemos si la orden es para nosotros (el Monitor aún no nos ha autenticado) */
			if (!m->err && mi_id[0] != '\0') procesar_orden((const char *)m->payload, m->len, mi_id);
			rd_kafka_message_destroy(m);
		}
		rd_kafka_poll(productor, 0);

		/* Opciones del menú de la estación */
		if (pedir_riego_manual)
		{
			pedir_riego_manual = 0;
			if (mi_id[0] == '\0')
			{
				printf("[Engine] Aún no tengo ID (el Monitor no se ha conectado).\n");
			}
			else
			{
				/* Riego manual: pasa por CENTRAL igual que el de un operario (operario "LOCAL") */
				char peticion[128];
				snprintf(peticion, sizeof(peticion), "PETICION#LOCAL#%s#%d", mi_id, DURACION_POR_DEFECTO);
				kafka_publicar(productor, TOPIC_PETICIONES, "LOCAL", peticion);
				printf("[Engine] Riego manual solicitado a CENTRAL (%d s)...\n", DURACION_POR_DEFECTO);
			}
		}
		if (pedir_fin_manual)
		{
			pedir_fin_manual = 0;
			if (regando) terminar_riego(mi_id, "MANUAL");
			else printf("[Engine] No hay ningún riego en curso.\n");
		}

		/* Riego en curso */
		if (regando)
		{
			if (estado_salud == 0)
			{
				/* Avería durante el riego: se corta inmediatamente */
				printf("[Engine] \033[0;31m¡Fuga durante el riego! Se corta el suministro.\033[0m\n");
				terminar_riego(mi_id, "FUGA");
			}
			else if (time(NULL) != ultimo_tick)
			{
				/* Un mensaje por segundo con el caudal (L/min) y el volumen acumulado (L) */
				char evento[128];
				double caudal = 8.0 + (rand() % 70) / 10.0; /* entre 8.0 y 14.9 L/min */

				ultimo_tick = time(NULL);
				volumen_riego += caudal / 60.0;
				snprintf(evento, sizeof(evento), "RIEGO#%s#%s#%.1f#%.2f", mi_id, operario_riego, caudal, volumen_riego);
				publicar_evento(mi_id, evento);
				printf("[Engine] Regando: caudal %.1f L/min, volumen %.2f L\n", caudal, volumen_riego);

				if (time(NULL) - inicio_riego >= duracion_riego)
					terminar_riego(mi_id, "TIEMPO");
			}
		}
	}

	return 0;
}
