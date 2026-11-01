/*
 * WM_WS_M  (Monitor de la Watering Station)
 *
 * Papel en el sistema (según el enunciado):
 *   - Al arrancar se conecta a WM_Central para AUTENTICARSE / REGISTRARSE.
 *   - Después actúa de SERVIDOR de sockets: espera a que se conecte su Engine (WM_WS_E).
 *   - Cada segundo le envía un HEARTBEAT al Engine. Si el Engine responde KO,
 *     no responde, o se desconecta, avisa a CENTRAL de la avería (ALERTA).
 *   - Cuando el Engine vuelve a estar OK avisa a CENTRAL (RECUPERADA).
 *
 * Uso: WM_WS_M <Puerto_Monitor> <IP_Central> <Puerto_Central> <ID_WS> [Ubicacion]
 * Ej.: WM_WS_M 5000 127.0.0.1 8080 WS-01 "River Park"
 */
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>

#define SEGUNDOS_TIMEOUT_ENGINE 2   /* si el Engine no contesta en 2 s lo damos por averiado */
#define SEGUNDOS_TIMEOUT_CENTRAL 3  /* para no quedarnos colgados si CENTRAL no responde */

int s_escucha = -1; /* socket de escucha (global para poder cerrarlo con Ctrl+C) */

void finalizar(int senyal)
{
	printf("\n[Monitor] Recibida la señal de fin (Ctrl+C)\n");
	if (s_escucha != -1) close(s_escucha);
	exit(0);
}

/*
 * Abre una conexión TCP con ip:puerto. Usa getaddrinfo para aceptar tanto
 * direcciones IP como nombres de host (necesario para Railway, que da un dominio).
 * Devuelve el descriptor del socket o -1 si falla.
 */
int conectar(const char *host, const char *puerto, int timeout_seg)
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

		struct timeval tv;
		tv.tv_sec = timeout_seg;
		tv.tv_usec = 0;
		setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
		setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

		if (connect(sock, p->ai_addr, p->ai_addrlen) == 0) break; /* conectado */
		close(sock);
		sock = -1;
	}
	freeaddrinfo(resultado);
	return sock;
}

/*
 * Envía una trama a CENTRAL y deja la respuesta en 'respuesta'.
 * CENTRAL atiende una petición por conexión, así que abrimos y cerramos cada vez.
 * Devuelve 0 si hubo respuesta y -1 si no se pudo hablar con CENTRAL.
 */
int hablar_con_central(const char *ip, const char *puerto, const char *trama, char *respuesta, int tam)
{
	int sock = conectar(ip, puerto, SEGUNDOS_TIMEOUT_CENTRAL);
	if (sock == -1) return -1;

	if (write(sock, trama, strlen(trama)) <= 0)
	{
		close(sock);
		return -1;
	}

	int n = read(sock, respuesta, tam - 1);
	close(sock);
	if (n <= 0) return -1;

	respuesta[n] = '\0';
	return 0;
}

/* Avisa a CENTRAL de un evento (ALERTA o RECUPERADA) con el formato COMANDO#ID#texto */
void notificar_central(const char *ip, const char *puerto, const char *comando, const char *id, const char *texto)
{
	char trama[256], respuesta[256];

	snprintf(trama, sizeof(trama), "%s#%s#%s", comando, id, texto);
	if (hablar_con_central(ip, puerto, trama, respuesta, sizeof(respuesta)) == 0)
		printf("[Monitor] -> CENTRAL (%s): %s\n", comando, respuesta);
	else
		fprintf(stderr, "[Monitor] No se pudo avisar a CENTRAL (%s). Se perdió la notificación.\n", comando);
}

int main(int argc, char *argv[])
{
	char *puerto_monitor, *ip_central, *puerto_central, *id_ws, *ubicacion;
	char trama[256], respuesta[256];
	struct sockaddr_in dir_servidor, dir_cliente;
	unsigned int long_dir_cliente;
	int s2, n;
	int en_averia = 0; /* 1 si ya hemos avisado a CENTRAL de una avería (para no repetirla cada segundo) */

	if (argc != 5 && argc != 6)
	{
		fprintf(stderr, "Error. Faltan argumentos\r\n");
		fprintf(stderr, "Sintaxis: %s <Puerto_Monitor> <IP_Central> <Puerto_Central> <ID_WS> [Ubicacion]\n\r", argv[0]);
		fprintf(stderr, "Ejemplo : %s 5000 127.0.0.1 8080 WS-01 \"River Park\"\n\r", argv[0]);
		return 1;
	}

	puerto_monitor = argv[1];
	ip_central = argv[2];
	puerto_central = argv[3];
	id_ws = argv[4];
	ubicacion = (argc == 6) ? argv[5] : "Sin ubicacion";

	setvbuf(stdout, NULL, _IOLBF, 0); /* salida por líneas: los logs salen al momento también bajo Docker */
	signal(SIGINT, finalizar);
	signal(SIGPIPE, SIG_IGN); /* si el Engine cierra, write devuelve error en vez de matar el proceso */

	/**** Paso 1: Autenticarse / registrarse en CENTRAL ****/

	snprintf(trama, sizeof(trama), "REGISTRO#%s#%s", id_ws, ubicacion);
	printf("[Monitor] Autenticando %s en CENTRAL (%s:%s)...\n", id_ws, ip_central, puerto_central);

	while (1)
	{
		if (hablar_con_central(ip_central, puerto_central, trama, respuesta, sizeof(respuesta)) == 0)
		{
			if (strncmp(respuesta, "STATUS#OK", 9) == 0)
			{
				printf("[Monitor] CENTRAL ha validado la estación: %s\n", respuesta);
				break;
			}
			/* CENTRAL ha contestado pero rechaza la estación: no tiene sentido reintentar */
			fprintf(stderr, "[Monitor] CENTRAL rechaza la autenticación: %s\n", respuesta);
			return 1;
		}
		fprintf(stderr, "[Monitor] CENTRAL no disponible. Reintentando en 3 s...\n");
		sleep(3);
	}

	/**** Paso 2: Servidor de sockets para el Engine ****/

	s_escucha = socket(AF_INET, SOCK_STREAM, 0);
	if (s_escucha == -1)
	{
		fprintf(stderr, "Error abriendo socket\n\r");
		return 1;
	}

	int opcion = 1;
	setsockopt(s_escucha, SOL_SOCKET, SO_REUSEADDR, &opcion, sizeof(opcion)); /* permite reiniciar sin esperar */

	dir_servidor.sin_family = AF_INET;
	dir_servidor.sin_port = htons(atoi(puerto_monitor));
	dir_servidor.sin_addr.s_addr = INADDR_ANY;

	if (bind(s_escucha, (struct sockaddr *)&dir_servidor, sizeof(dir_servidor)) == -1)
	{
		fprintf(stderr, "Error en bind (¿puerto %s ocupado?)\n\r", puerto_monitor);
		close(s_escucha);
		return 1;
	}
	if (listen(s_escucha, 4) == -1)
	{
		fprintf(stderr, "Error en listen\n\r");
		close(s_escucha);
		return 1;
	}

	/**** Paso 3: Esperar al Engine y monitorizarlo ****/

	while (1)
	{
		printf("[Monitor] Esperando al Engine en el puerto %s...\n", puerto_monitor);
		long_dir_cliente = sizeof(dir_cliente);
		s2 = accept(s_escucha, (struct sockaddr *)&dir_cliente, &long_dir_cliente);
		if (s2 == -1)
		{
			if (errno == EINTR) continue;
			break;
		}

		/* Si el Engine no responde en SEGUNDOS_TIMEOUT_ENGINE, read() falla y lo tratamos como avería */
		struct timeval tv;
		tv.tv_sec = SEGUNDOS_TIMEOUT_ENGINE;
		tv.tv_usec = 0;
		setsockopt(s2, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

		printf("[Monitor] Engine conectado. Le asigno el ID %s.\n", id_ws);

		/* El Engine no conoce su ID: se lo da el Monitor (el enunciado lo exige así) */
		snprintf(trama, sizeof(trama), "ASIGNACION#%s", id_ws);
		write(s2, trama, strlen(trama));
		n = read(s2, respuesta, sizeof(respuesta) - 1);
		if (n <= 0)
		{
			fprintf(stderr, "[Monitor] El Engine no confirmó la asignación. Cierro la conexión.\n");
			close(s2);
			continue;
		}

		/* Bucle de latidos: un HEARTBEAT por segundo */
		int segundos = 0;
		while (1)
		{
			if (write(s2, "HEARTBEAT", 9) <= 0)
			{
				n = -1;
			}
			else
			{
				n = read(s2, respuesta, sizeof(respuesta) - 1);
			}

			if (n <= 0)
			{
				/* Engine caído o sin respuesta: avería. Avisamos UNA vez y dejamos de usar esta conexión */
				fprintf(stderr, "[Monitor] \033[0;31mEngine sin respuesta o desconectado\033[0m\n");
				if (!en_averia)
				{
					notificar_central(ip_central, puerto_central, "ALERTA", id_ws, "Engine sin respuesta");
					en_averia = 1;
				}
				break; /* salimos al accept para esperar a que el Engine vuelva */
			}

			respuesta[n] = '\0';
			printf("[Monitor] Estado recibido: %s\n", respuesta);

			if (strstr(respuesta, "KO") != NULL)
			{
				/* El Engine sigue conectado pero reporta fuga */
				if (!en_averia)
				{
					printf("[Monitor] \033[0;31m[!] ALERTA: fuga detectada en %s\033[0m\n", id_ws);
					notificar_central(ip_central, puerto_central, "ALERTA", id_ws, "Fuga detectada por sensor");
					en_averia = 1;
				}
			}
			else if (en_averia)
			{
				/* Engine vuelve a responder OK tras una avería: la WS vuelve a estar disponible */
				printf("[Monitor] \033[0;32mAvería resuelta en %s\033[0m\n", id_ws);
				notificar_central(ip_central, puerto_central, "RECUPERADA", id_ws, "Averia resuelta");
				en_averia = 0;
			}

			/*
			 * Cada 5 s le decimos a CENTRAL cómo estamos. Así, si CENTRAL se reinicia (y marca todas
			 * las estaciones como DISCONNECTED), nos vuelve a ver conectados sin reiniciar el Monitor.
			 */
			segundos++;
			if (segundos % 5 == 0)
				notificar_central(ip_central, puerto_central, "ESTADO", id_ws, en_averia ? "KO" : "OK");

			sleep(1);
		}

		close(s2);
	}

	close(s_escucha);
	printf("[Monitor] Socket cerrado. Monitorización finalizada\n\r");
	return 0;
}
