/*
 * WM_Central  (sistema central)
 *
 * Uso: WM_Central <Puerto_Escucha> <IP_Broker> <Puerto_Broker>
 * Ej.: WM_Central 8080 127.0.0.1 9092
 *
 * CENTRAL tiene DOS partes que se ejecutan en procesos distintos y comparten la base de datos:
 *
 *  1) Servidor de SOCKETS (proceso principal, un hijo por conexión). Atiende a los Monitores:
 *       REGISTRO#ID#Ubicacion  -> da de alta / autentica la estación y la deja AVAILABLE
 *       ALERTA#ID#texto        -> avería: estado KO en la BD, aviso por Kafka y, si estaba regando, se corta
 *       RECUPERADA#ID#texto    -> avería resuelta: la estación vuelve a AVAILABLE
 *       ESTADO#ID#OK|KO        -> latido periódico del Monitor (reconecta tras reiniciar CENTRAL)
 *
 *  2) Consumidor de KAFKA (proceso hijo creado al arrancar). Atiende los riegos:
 *       PETICION  (topic peticiones_riego) -> valida la estación y, si está libre, ordena abrir (ordenes_ws)
 *       INICIO / RIEGO / FIN (topic eventos_ws) -> actualiza la BD y avisa al operario (notificaciones_fo)
 *
 * Estados de una estación en la BD: AVAILABLE, WATERING, KO (fuga), OUT_OF_SERVICE, DISCONNECTED.
 */
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <time.h>

#include <sqlite3.h>
#include "wm_kafka.h"

#define SEGUNDOS_SIN_NOTICIAS 15 /* si una estación "regando" no da señales en 15 s, la liberamos */

char *broker_ip;     /* IP o nombre del broker Kafka (argumento de línea de comandos) */
char *broker_puerto; /* puerto del broker Kafka */
pid_t hijo_kafka;    /* proceso consumidor de Kafka */
pid_t hijo_admin;    /* proceso del menú de administración de CENTRAL (solo si hay teclado) */
int s;               /* socket de escucha */

/* ======================= BASE DE DATOS ======================= */

/*
 * Ejecuta una sentencia SQL abriendo y cerrando la BD.
 * Devuelve el número de filas modificadas (>= 0) o -1 si hubo error.
 */
int ejecutar_sql_conf(const char *sql, int silencioso)
{
	sqlite3 *db;
	char *err_msg = 0;
	int cambios = 0;

	if (sqlite3_open("central.db", &db) != SQLITE_OK)
	{
		fprintf(stderr, "\033[0;31mError abriendo BD: %s\033[0m\n", sqlite3_errmsg(db));
		sqlite3_close(db);
		return -1;
	}
	sqlite3_busy_timeout(db, 2000); /* varios procesos escriben a la vez: esperamos si está bloqueada */

	if (sqlite3_exec(db, sql, NULL, 0, &err_msg) != SQLITE_OK)
	{
		if (!silencioso) fprintf(stderr, "\033[0;31mError SQL: %s\033[0m\n", err_msg);
		sqlite3_free(err_msg);
		cambios = -1;
	}
	else
	{
		cambios = sqlite3_changes(db);
	}
	sqlite3_close(db);
	return cambios;
}

int ejecutar_sql(const char *sql)
{
	return ejecutar_sql_conf(sql, 0);
}

/* Cambia el estado de una estación. %Q de SQLite escapa las comillas (evita roturas e inyección SQL). */
int actualizar_estado(const char *id, const char *estado)
{
	char *sql = sqlite3_mprintf("UPDATE ESTACIONES SET ESTADO = %Q WHERE ID = %Q;", estado, id);
	int rc = ejecutar_sql(sql);
	sqlite3_free(sql);
	return rc;
}

/* Lee el estado y el operario de una estación. Devuelve 1 si la estación existe y 0 si no. */
int obtener_estacion(const char *id, char *estado, int tam_estado, char *operario, int tam_operario)
{
	sqlite3 *db;
	sqlite3_stmt *st;
	int existe = 0;

	if (sqlite3_open("central.db", &db) != SQLITE_OK)
	{
		sqlite3_close(db);
		return 0;
	}
	sqlite3_busy_timeout(db, 2000);
	if (sqlite3_prepare_v2(db, "SELECT ESTADO, IFNULL(OPERARIO, '') FROM ESTACIONES WHERE ID = ?;", -1, &st, NULL) == SQLITE_OK)
	{
		sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
		if (sqlite3_step(st) == SQLITE_ROW)
		{
			snprintf(estado, tam_estado, "%s", (const char *)sqlite3_column_text(st, 0));
			snprintf(operario, tam_operario, "%s", (const char *)sqlite3_column_text(st, 1));
			existe = 1;
		}
		sqlite3_finalize(st);
	}
	sqlite3_close(db);
	return existe;
}

void inicializar_bd()
{
	sqlite3 *db;
	char *mensaje_error = 0;

	int rc = sqlite3_open("central.db", &db);
	if (rc)
	{
		fprintf(stderr, "\033[0;31mError abriendo base de datos: %s\033[0m\n", sqlite3_errmsg(db));
		exit(EXIT_FAILURE);
	}

	// Inventario de estaciones. CAUDAL y VOLUMEN se actualizan cada segundo durante un riego.
	const char *sql_estaciones =
		"CREATE TABLE IF NOT EXISTS ESTACIONES("
		"ID TEXT PRIMARY KEY NOT NULL,"
		"UBICACION TEXT NOT NULL,"
		"ESTADO TEXT NOT NULL,"
		"CAUDAL REAL DEFAULT 0,"
		"VOLUMEN REAL DEFAULT 0,"
		"OPERARIO TEXT DEFAULT '',"
		"ULTIMA_ACT INTEGER DEFAULT 0);";

	rc = sqlite3_exec(db, sql_estaciones, NULL, 0, &mensaje_error);
	if (rc != SQLITE_OK)
	{
		fprintf(stderr, "\033[0;31mError SQL: %s\033[0m\n", mensaje_error);
		sqlite3_free(mensaje_error);
	}
	else
	{
		printf("\033[0;32m[BD] Tabla ESTACIONES lista.\033[0m\n");
	}
	sqlite3_close(db);

	/* Si la BD viene de una versión anterior (sin estas columnas) las añadimos. Si ya existen, el error se ignora. */
	ejecutar_sql_conf("ALTER TABLE ESTACIONES ADD COLUMN CAUDAL REAL DEFAULT 0;", 1);
	ejecutar_sql_conf("ALTER TABLE ESTACIONES ADD COLUMN VOLUMEN REAL DEFAULT 0;", 1);
	ejecutar_sql_conf("ALTER TABLE ESTACIONES ADD COLUMN OPERARIO TEXT DEFAULT '';", 1);
	ejecutar_sql_conf("ALTER TABLE ESTACIONES ADD COLUMN ULTIMA_ACT INTEGER DEFAULT 0;", 1);

	/*
	 * Al arrancar o reiniciar, CENTRAL muestra las estaciones ya registradas como DESCONECTADAS
	 * hasta que cada una vuelva a conectar y nos diga su estado real.
	 */
	if (ejecutar_sql("UPDATE ESTACIONES SET ESTADO = 'DISCONNECTED', CAUDAL = 0, OPERARIO = '' WHERE ESTADO <> 'OUT_OF_SERVICE';") >= 0)
		printf("\033[0;33m[BD] Estaciones registradas marcadas como DISCONNECTED hasta que reconecten.\033[0m\n");
}

/* ======================= KAFKA: ENVÍO ======================= */

/*
 * Publica un mensaje. Si 'rk' no es NULL usa ese productor (que ya está abierto); si es NULL crea
 * uno temporal (lo usan los hijos del servidor de sockets, que publican muy de vez en cuando).
 */
int publicar(rd_kafka_t *rk, const char *topic, const char *clave, const char *mensaje)
{
	if (rk) return kafka_publicar(rk, topic, clave, mensaje);
	return kafka_publicar_una_vez(broker_ip, broker_puerto, topic, clave, mensaje);
}

/* Envía un mensaje al operario: operario#TIPO#texto (en el topic notificaciones_fo) */
void notificar_fo(rd_kafka_t *rk, const char *operario, const char *tipo, const char *texto)
{
	char mensaje[512];

	snprintf(mensaje, sizeof(mensaje), "%s#%s#%s", operario, tipo, texto);
	publicar(rk, TOPIC_NOTIFICACIONES, operario, mensaje);
}

/* Publica una alerta de fuga en el topic "alertas_agua". Devuelve 0 si se envió y -1 si no. */
int publicar_alerta_kafka(const char *id_estacion)
{
	char payload[256];

	snprintf(payload, sizeof(payload), "{\"id_estacion\": \"%s\", \"estado\": \"KO\", \"alerta\": \"FUGA DETECTADA\"}", id_estacion);
	if (publicar(NULL, TOPIC_ALERTAS, id_estacion, payload) == 0)
	{
		printf("\033[0;34m[KAFKA] Mensaje publicado en el topic alertas_agua: %s\033[0m\n", payload);
		return 0;
	}
	return -1;
}

/* ======================= KAFKA: PROCESO CONSUMIDOR ======================= */

const char *texto_motivo(const char *motivo)
{
	if (strcmp(motivo, "TIEMPO") == 0) return "tiempo programado cumplido";
	if (strcmp(motivo, "MANUAL") == 0) return "finalizado manualmente en la estacion";
	if (strcmp(motivo, "FUGA") == 0) return "interrumpido por una fuga";
	if (strcmp(motivo, "ORDEN_CENTRAL") == 0) return "orden de CENTRAL";
	return motivo;
}

/* PETICION#operario#ws#segundos: valida la estación y, si está libre, ordena abrir la electroválvula */
void listar_disponibles(rd_kafka_t *prod, const char *operario); /* definida más abajo */

void tratar_peticion(rd_kafka_t *prod, const char *operario, const char *ws, const char *segundos)
{
	char texto[300], estado[32] = "", operario_actual[50] = "";
	int duracion = atoi(segundos);
	if (duracion <= 0) duracion = DURACION_POR_DEFECTO;

	printf("\033[0;36m[PETICIÓN] El operario %s solicita riego en %s durante %d s\033[0m\n", operario, ws, duracion);
	snprintf(texto, sizeof(texto), "Petición recibida: riego en %s durante %d s. Validando estación...", ws, duracion);
	notificar_fo(prod, operario, "INFO", texto);

	if (!obtener_estacion(ws, estado, sizeof(estado), operario_actual, sizeof(operario_actual)))
	{
		snprintf(texto, sizeof(texto), "La estación %s no existe", ws);
		printf("\033[0;31m -> DENEGADO: %s\033[0m\n", texto);
		notificar_fo(prod, operario, "DENEGADO", texto);
		return;
	}

	if (strcmp(estado, "AVAILABLE") != 0)
	{
		if (strcmp(estado, "WATERING") == 0)
			snprintf(texto, sizeof(texto), "La estación %s ya está regando (operario %s)", ws, operario_actual);
		else if (strcmp(estado, "KO") == 0)
			snprintf(texto, sizeof(texto), "La estación %s tiene una avería (fuga) y no puede regar", ws);
		else if (strcmp(estado, "OUT_OF_SERVICE") == 0)
			snprintf(texto, sizeof(texto), "La estación %s está fuera de servicio", ws);
		else
			snprintf(texto, sizeof(texto), "La estación %s está desconectada", ws);
		printf("\033[0;31m -> DENEGADO: %s\033[0m\n", texto);
		notificar_fo(prod, operario, "DENEGADO", texto);
		return;
	}

	/*
	 * Reservamos la estación de forma atómica: el UPDATE solo cambia la fila si SIGUE en AVAILABLE.
	 * Si dos operarios piden a la vez, solo uno consigue modificar 1 fila.
	 */
	char *sql = sqlite3_mprintf(
		"UPDATE ESTACIONES SET ESTADO = 'WATERING', OPERARIO = %Q, CAUDAL = 0, VOLUMEN = 0, ULTIMA_ACT = %ld "
		"WHERE ID = %Q AND ESTADO = 'AVAILABLE';",
		operario, (long)time(NULL), ws);
	int cambios = ejecutar_sql(sql);
	sqlite3_free(sql);
	if (cambios != 1)
	{
		snprintf(texto, sizeof(texto), "La estación %s acaba de ser ocupada por otro operario", ws);
		printf("\033[0;31m -> DENEGADO: %s\033[0m\n", texto);
		notificar_fo(prod, operario, "DENEGADO", texto);
		return;
	}

	snprintf(texto, sizeof(texto), "Estación %s disponible. Solicitando autorización a la estación...", ws);
	printf("\033[0;32m -> Estación disponible. Se envía la orden de apertura a %s\033[0m\n", ws);
	notificar_fo(prod, operario, "INFO", texto);

	char orden[128];
	snprintf(orden, sizeof(orden), "ABRIR#%s#%s#%d", ws, operario, duracion);
	publicar(prod, TOPIC_ORDENES, ws, orden);
}

/* Mensajes del Engine: INICIO#ws#op | RIEGO#ws#op#caudal#volumen | FIN#ws#op#volumen#segundos#motivo */
void tratar_evento(rd_kafka_t *prod, char *campos[], int n)
{
	char texto[400];

	if (strcmp(campos[0], "INICIO") == 0 && n >= 3)
	{
		printf("\033[0;32m[%s] Riego iniciado: electroválvula ABIERTA (operario %s)\033[0m\n", campos[1], campos[2]);
		snprintf(texto, sizeof(texto), "La estación %s ha autorizado el riego. Electroválvula ABIERTA.", campos[1]);
		notificar_fo(prod, campos[2], "OK", texto);
	}
	else if (strcmp(campos[0], "RIEGO") == 0 && n >= 5)
	{
		char *sql = sqlite3_mprintf(
			"UPDATE ESTACIONES SET CAUDAL = %f, VOLUMEN = %f, ULTIMA_ACT = %ld WHERE ID = %Q AND ESTADO = 'WATERING';",
			atof(campos[3]), atof(campos[4]), (long)time(NULL), campos[1]);
		if (ejecutar_sql(sql) == 1)
			printf("[%s] REGANDO  caudal %s L/min  volumen %s L  (operario %s)\n", campos[1], campos[3], campos[4], campos[2]);
		sqlite3_free(sql);
	}
	else if (strcmp(campos[0], "FIN") == 0 && n >= 6)
	{
		// La estación vuelve a AVAILABLE solo si seguía WATERING (si hay avería, el estado KO se respeta)
		char *sql = sqlite3_mprintf(
			"UPDATE ESTACIONES SET ESTADO = 'AVAILABLE', CAUDAL = 0, OPERARIO = '' WHERE ID = %Q AND ESTADO = 'WATERING';",
			campos[1]);
		ejecutar_sql(sql);
		sqlite3_free(sql);

		if (strncmp(campos[5], "RECHAZADO", 9) == 0)
		{
			snprintf(texto, sizeof(texto), "La estación %s ha rechazado el riego (%s)", campos[1], campos[5]);
			printf("\033[0;31m[%s] Riego rechazado por la estación: %s\033[0m\n", campos[1], campos[5]);
			notificar_fo(prod, campos[2], "DENEGADO", texto);
		}
		else
		{
			snprintf(texto, sizeof(texto), "Riego en %s terminado. Volumen total %s L, duración %s s, fin por: %s",
					 campos[1], campos[3], campos[4], texto_motivo(campos[5]));
			printf("\033[0;34m[%s] Riego finalizado. %s\033[0m\n", campos[1], texto);
			notificar_fo(prod, campos[2], "RESUMEN", texto);
		}
	}
}

/* Interpreta un mensaje recibido de Kafka (se identifica por su primera palabra) */
void tratar_mensaje(rd_kafka_t *prod, const char *payload, size_t len)
{
	char buffer[512];
	char *campos[8];

	if (len >= sizeof(buffer)) len = sizeof(buffer) - 1;
	memcpy(buffer, payload, len);
	buffer[len] = '\0';

	int n = dividir_campos(buffer, campos, 8);
	if (strcmp(campos[0], "PETICION") == 0 && n >= 4)
		tratar_peticion(prod, campos[1], campos[2], campos[3]);
	else if (strcmp(campos[0], "LISTAR") == 0 && n >= 2)
		listar_disponibles(prod, campos[1]);
	else
		tratar_evento(prod, campos, n);
}


/* ======================= ADMINISTRACIÓN DESDE CENTRAL Y LISTADO PARA OPERARIOS ======================= */

#define MAX_ESTACIONES 100

/* Rellena 'ids' con los ID de todas las estaciones (o solo las de estado 'filtro' si no es NULL). Devuelve cuántas. */
int leer_estaciones(char ids[][50], char estados[][32], int max, const char *filtro)
{
	sqlite3 *db;
	sqlite3_stmt *st;
	int n = 0;

	if (sqlite3_open("central.db", &db) != SQLITE_OK)
	{
		sqlite3_close(db);
		return 0;
	}
	sqlite3_busy_timeout(db, 2000);
	if (sqlite3_prepare_v2(db, "SELECT ID, ESTADO FROM ESTACIONES ORDER BY ID;", -1, &st, NULL) == SQLITE_OK)
	{
		while (sqlite3_step(st) == SQLITE_ROW && n < max)
		{
			const char *estado = (const char *)sqlite3_column_text(st, 1);
			if (filtro && strcmp(estado, filtro) != 0) continue;
			snprintf(ids[n], 50, "%s", (const char *)sqlite3_column_text(st, 0));
			snprintf(estados[n], 32, "%s", estado);
			n++;
		}
		sqlite3_finalize(st);
	}
	sqlite3_close(db);
	return n;
}

/* Un operario pide ver las estaciones disponibles: se le envía la lista como notificación INFO */
void listar_disponibles(rd_kafka_t *prod, const char *operario)
{
	static char ids[MAX_ESTACIONES][50], estados[MAX_ESTACIONES][32];
	char texto[1024] = "";
	int i, n = leer_estaciones(ids, estados, MAX_ESTACIONES, "AVAILABLE");

	if (n == 0)
	{
		snprintf(texto, sizeof(texto), "No hay estaciones disponibles ahora mismo");
	}
	else
	{
		snprintf(texto, sizeof(texto), "Estaciones disponibles (%d):", n);
		for (i = 0; i < n && strlen(texto) < sizeof(texto) - 80; i++)
		{
			strncat(texto, " ", sizeof(texto) - strlen(texto) - 1);
			strncat(texto, ids[i], sizeof(texto) - strlen(texto) - 1);
		}
	}
	printf("[LISTAR] %s pide las estaciones disponibles -> %s\n", operario, texto);
	notificar_fo(prod, operario, "INFO", texto);
}

/* Bloquear una estación: corta el riego si lo hubiera y la deja OUT_OF_SERVICE (naranja, "Fuera de Servicio") */
void bloquear_ws(rd_kafka_t *prod, const char *id)
{
	char estado[32] = "", operario[50] = "";

	if (!obtener_estacion(id, estado, sizeof(estado), operario, sizeof(operario)))
	{
		printf("\033[0;31m[ADMIN] La estación %s no existe\033[0m\n", id);
		return;
	}
	if (strcmp(estado, "WATERING") == 0)
	{
		char orden[128], aviso[200];
		snprintf(orden, sizeof(orden), "CERRAR#%s#%s", id, operario);
		publicar(prod, TOPIC_ORDENES, id, orden);
		snprintf(aviso, sizeof(aviso), "CENTRAL ha bloqueado %s: riego interrumpido. Estación Fuera de Servicio", id);
		notificar_fo(prod, operario, "INFO", aviso);
	}
	char *sql = sqlite3_mprintf("UPDATE ESTACIONES SET ESTADO = 'OUT_OF_SERVICE', CAUDAL = 0, OPERARIO = '' WHERE ID = %Q;", id);
	ejecutar_sql(sql);
	sqlite3_free(sql);
	printf("\033[0;33m[ADMIN] %s bloqueada: FUERA DE SERVICIO\033[0m\n", id);
}

/* Activar una estación bloqueada: vuelve a AVAILABLE (verde) */
void activar_ws(const char *id)
{
	char estado[32] = "", operario[50] = "";

	if (!obtener_estacion(id, estado, sizeof(estado), operario, sizeof(operario)))
	{
		printf("\033[0;31m[ADMIN] La estación %s no existe\033[0m\n", id);
		return;
	}
	if (strcmp(estado, "OUT_OF_SERVICE") != 0)
	{
		printf("[ADMIN] %s no estaba bloqueada (estado actual: %s). No se cambia nada.\n", id, estado);
		return;
	}
	char *sql = sqlite3_mprintf("UPDATE ESTACIONES SET ESTADO = 'AVAILABLE', CAUDAL = 0, OPERARIO = '' WHERE ID = %Q;", id);
	ejecutar_sql(sql);
	sqlite3_free(sql);
	printf("\033[0;32m[ADMIN] %s activada: AVAILABLE\033[0m\n", id);
}

/* Menú de administración de CENTRAL por teclado (solo se arranca si CENTRAL tiene terminal, docker run -it) */
void bucle_admin()
{
	static char ids[MAX_ESTACIONES][50], estados[MAX_ESTACIONES][32];
	char linea[200], orden[50], destino[50];
	rd_kafka_t *prod = kafka_crear_productor(broker_ip, broker_puerto);

	signal(SIGINT, SIG_DFL);
	signal(SIGTERM, SIG_DFL);

	printf("\033[0;36m[ADMIN] Menú de CENTRAL: iniciar <WS|todas> [seg] | bloquear <WS|todas> | activar <WS|todas> | estado | ayuda\033[0m\n");

	while (fgets(linea, sizeof(linea), stdin) != NULL)
	{
		int seg = 0, i, n;
		orden[0] = destino[0] = '\0';
		if (sscanf(linea, "%49s %49s %d", orden, destino, &seg) < 1) continue;

		if (strcmp(orden, "estado") == 0)
		{
			n = leer_estaciones(ids, estados, MAX_ESTACIONES, NULL);
			for (i = 0; i < n; i++) printf("  %-10s %s\n", ids[i], estados[i]);
			if (n == 0) printf("  (no hay estaciones registradas)\n");
		}
		else if (strcmp(orden, "ayuda") == 0)
		{
			printf("  iniciar <WS-01|todas> [segundos]   inicia un riego como si lo pidiera un operario\n");
			printf("  bloquear <WS-01|todas>             corta el riego y pone la estación Fuera de Servicio\n");
			printf("  activar <WS-01|todas>              devuelve a AVAILABLE las estaciones bloqueadas\n");
			printf("  estado                             lista todas las estaciones con su estado\n");
		}
		else if ((strcmp(orden, "iniciar") == 0 || strcmp(orden, "bloquear") == 0 || strcmp(orden, "activar") == 0) && destino[0])
		{
			int todas = (strcmp(destino, "todas") == 0);
			if (todas) n = leer_estaciones(ids, estados, MAX_ESTACIONES, NULL);
			else { snprintf(ids[0], 50, "%s", destino); n = 1; }

			for (i = 0; i < n; i++)
			{
				if (strcmp(orden, "iniciar") == 0)
				{
					char segundos[16];
					snprintf(segundos, sizeof(segundos), "%d", seg);
					printf("\033[0;36m[ADMIN] CENTRAL ordena iniciar riego en %s\033[0m\n", ids[i]);
					tratar_peticion(prod, "CENTRAL", ids[i], segundos);
				}
				else if (strcmp(orden, "bloquear") == 0)
					bloquear_ws(prod, ids[i]);
				else
					activar_ws(ids[i]);
			}
			rd_kafka_poll(prod, 200);
			rd_kafka_flush(prod, 2000);
		}
		else
		{
			printf("  Orden no reconocida. Escribe 'ayuda'.\n");
		}
	}
	exit(0);
}

/* Si una estación lleva mucho tiempo "regando" sin dar señales (Engine caído...), la liberamos */
void vigilar_estaciones()
{
	char *sql = sqlite3_mprintf(
		"UPDATE ESTACIONES SET ESTADO = 'AVAILABLE', CAUDAL = 0, OPERARIO = '' WHERE ESTADO = 'WATERING' AND ULTIMA_ACT < %ld;",
		(long)time(NULL) - SEGUNDOS_SIN_NOTICIAS);
	if (ejecutar_sql(sql) > 0)
		printf("\033[0;33m[VIGILANTE] Una estación llevaba más de %d s regando sin noticias: liberada.\033[0m\n", SEGUNDOS_SIN_NOTICIAS);
	sqlite3_free(sql);
}

/* Bucle del proceso consumidor de Kafka. No termina nunca. */
void bucle_kafka()
{
	const char *topics[] = {TOPIC_PETICIONES, TOPIC_EVENTOS};
	time_t ultima_vigilancia = 0;

	signal(SIGINT, SIG_DFL);
	signal(SIGTERM, SIG_DFL);

	rd_kafka_t *prod = kafka_crear_productor(broker_ip, broker_puerto);
	rd_kafka_t *cons = kafka_crear_consumidor(broker_ip, broker_puerto, "wm_central", topics, 2);
	if (!prod || !cons)
	{
		fprintf(stderr, "\033[0;31m[KAFKA] No se pudo iniciar el consumidor de CENTRAL\033[0m\n");
		exit(1);
	}
	printf("\033[0;32m[KAFKA] CENTRAL escuchando peticiones y eventos de riego en %s:%s\033[0m\n", broker_ip, broker_puerto);

	while (1)
	{
		rd_kafka_message_t *m = rd_kafka_consumer_poll(cons, 500);
		if (m)
		{
			if (!m->err) tratar_mensaje(prod, (const char *)m->payload, m->len);
			rd_kafka_message_destroy(m);
		}
		rd_kafka_poll(prod, 0);

		if (time(NULL) - ultima_vigilancia >= 5)
		{
			ultima_vigilancia = time(NULL);
			vigilar_estaciones();
		}
	}
}

/* ======================= SERVIDOR DE SOCKETS ======================= */

void finalizar(int senyal)
{
	printf("Recibida la señal de fin (cntr-C)\n\r");
	if (hijo_kafka > 0) kill(hijo_kafka, SIGTERM); /* paramos también el consumidor de Kafka */
	if (hijo_admin > 0) kill(hijo_admin, SIGTERM);
	close(s); /* cerrar para que accept termine con un error y salir del bucle principal */
}

int main(int argc, char *argv[])
{
	char *servidor_puerto;
	char mensaje[1024];
	struct sockaddr_in dir_servidor, dir_cliente;
	unsigned int long_dir_cliente;
	int s2;
	int n, enviados, recibidos;
	int proceso;
	int contador = 0;

	/* Comprobar los argumentos: puerto de escucha + IP y puerto del broker de Kafka */
	if (argc != 4)
	{
		fprintf(stderr, "Error. Faltan argumentos\r\n");
		fprintf(stderr, "Sintaxis: %s <Puerto> <IP_Broker> <Puerto_Broker>\n\r", argv[0]);
		fprintf(stderr, "Ejemplo : %s 8080 127.0.0.1 9092\n\r", argv[0]);
		return 1;
	}

	servidor_puerto = argv[1];
	broker_ip = argv[2];
	broker_puerto = argv[3];

	/*
	 * Salida por líneas. Sin esto, cuando la salida no es un terminal (Docker, redirección a fichero)
	 * el texto pendiente en el buffer se duplica en cada hijo creado con fork() y los logs salen repetidos.
	 */
	setvbuf(stdout, NULL, _IOLBF, 0);

	inicializar_bd(); // crear tablas y marcar estaciones como DISCONNECTED

	/* Arrancamos el proceso consumidor de Kafka (antes de abrir el socket, para que no lo herede) */
	hijo_kafka = fork();
	if (hijo_kafka == -1)
	{
		fprintf(stderr, "Error creando el proceso de Kafka\n\r");
		return 1;
	}
	if (hijo_kafka == 0)
	{
		bucle_kafka();
		exit(0);
	}

	/* Menú de administración de CENTRAL (iniciar / bloquear / activar): solo si hay teclado (docker run -it) */
	if (isatty(STDIN_FILENO))
	{
		hijo_admin = fork();
		if (hijo_admin == 0)
		{
			bucle_admin();
			exit(0);
		}
	}

	/**** Paso 1: Abrir el socket ****/

	s = socket(AF_INET, SOCK_STREAM, 0); /* AF_INET = IPv4, SOCK_STREAM = TCP */
	if (s == -1)
	{
		fprintf(stderr, "Error. No se puede abrir el socket\n\r");
		kill(hijo_kafka, SIGTERM);
		return 1;
	}
	printf("Socket abierto\n\r");

	int opcion = 1;
	setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opcion, sizeof(opcion)); /* permite reiniciar CENTRAL sin esperar */

	/**** Paso 2: Establecer la dirección (puerto) de escucha ****/

	dir_servidor.sin_family = AF_INET;
	dir_servidor.sin_port = htons(atoi(servidor_puerto)); // htons() convierte el puerto al formato de red
	dir_servidor.sin_addr.s_addr = INADDR_ANY;            // acepta conexiones por cualquier IP de la máquina
	if (bind(s, (struct sockaddr *)&dir_servidor, sizeof(dir_servidor)) == -1)
	{
		fprintf(stderr, "Error. No se puede asociar el puerto al servidor\n\r");
		close(s);
		kill(hijo_kafka, SIGTERM);
		return 1;
	}
	printf("Puerto de escucha establecido\n\r");

	/**** Paso 3: Preparar el servidor para escuchar ****/

	if (listen(s, 4) == -1) // el 4 es el tamaño de la cola de conexiones pendientes
	{
		fprintf(stderr, "Error preparando servidor\n\r");
		close(s);
		kill(hijo_kafka, SIGTERM);
		return 1;
	}
	printf("Socket preparado\n\r");

	/**** Paso 4: Esperar conexiones ****/

	signal(SIGINT, finalizar);  // Ctrl+C
	signal(SIGTERM, finalizar); // docker stop
	signal(SIGCHLD, SIG_IGN);   // los procesos hijo terminados no se quedan como zombis
	signal(SIGPIPE, SIG_IGN);   // si un cliente se va, write falla en vez de matar al proceso

	while (1)
	{
		fprintf(stderr, "Esperando conexión en el puerto %s...\n\r", servidor_puerto);
		long_dir_cliente = sizeof(dir_cliente);
		s2 = accept(s, (struct sockaddr *)&dir_cliente, &long_dir_cliente);
		if (s2 == -1)
		{
			if (errno == EINTR) continue; /* nos interrumpió una señal que no es la de fin */
			break;                        /* socket cerrado por Ctrl+C: salir del bucle */
		}
		contador++;

		/* crear un nuevo proceso para que se puedan atender varias peticiones en paralelo */
		proceso = fork();
		if (proceso == -1) exit(1);
		if (proceso == 0) /* soy el hijo */
		{
			close(s); /* el hijo no necesita el socket general */

			/**** Paso 5: Leer el mensaje ****/

			n = sizeof(mensaje) - 1; /* dejamos 1 byte para el '\0' */
			recibidos = read(s2, mensaje, n);
			if (recibidos <= 0)
			{
				fprintf(stderr, "Error leyendo el mensaje\n\r");
				close(s2);
				exit(1);
			}
			mensaje[recibidos] = '\0';
			mensaje[strcspn(mensaje, "\r\n")] = '\0'; /* quitamos el salto de línea (por ejemplo, si viene de nc) */
			printf("[Conexión #%d] Trama recibida: %s\n\r", contador, mensaje);

			/**** Paso 6: Procesar la trama y enviar respuesta ****/
			char comando[50];
			char id_estacion[50];
			char ubicacion[100]; /* tercer campo de la trama: ubicación, texto de avería, OK/KO... según el comando */
			char respuesta[256];

			if (sscanf(mensaje, "%49[^#]#%49[^#]#%99[^\n]", comando, id_estacion, ubicacion) == 3)
			{
				if (strcmp(comando, "REGISTRO") == 0)
				{
					printf(" -> Registro/autenticación de la estación %s (%s)\n\r", id_estacion, ubicacion);

					// REPLACE por si la estación se reinicia y vuelve a registrarse
					char *sql = sqlite3_mprintf("REPLACE INTO ESTACIONES (ID, UBICACION, ESTADO) VALUES (%Q, %Q, 'AVAILABLE');", id_estacion, ubicacion);
					if (ejecutar_sql(sql) >= 0)
					{
						printf("\033[0;32m -> Registro guardado en central.db\033[0m\n");
						sprintf(respuesta, "STATUS#OK#Estacion registrada correctamente");
					}
					else
					{
						sprintf(respuesta, "STATUS#ERROR#No se pudo guardar el registro");
					}
					sqlite3_free(sql);
				}
				else if (strcmp(comando, "ALERTA") == 0)
				{
					printf("\033[0;31m -> [EMERGENCIA] Avería reportada en la estacion: %s (%s)\033[0m\n\r", id_estacion, ubicacion);

					// ¿Estaba regando? Si es así hay que cortar el riego y avisar al operario
					char estado_anterior[32] = "", operario[50] = "";
					obtener_estacion(id_estacion, estado_anterior, sizeof(estado_anterior), operario, sizeof(operario));

					char *sql = sqlite3_mprintf("UPDATE ESTACIONES SET ESTADO = 'KO', CAUDAL = 0 WHERE ID = %Q AND ESTADO <> 'OUT_OF_SERVICE';", id_estacion);
					ejecutar_sql(sql);
					sqlite3_free(sql);

					if (strcmp(estado_anterior, "WATERING") == 0)
					{
						char orden[128], aviso[200];
						printf("\033[0;31m -> La estación estaba regando: se ordena CERRAR y se avisa al operario %s\033[0m\n\r", operario);
						snprintf(orden, sizeof(orden), "CERRAR#%s#%s", id_estacion, operario);
						publicar(NULL, TOPIC_ORDENES, id_estacion, orden);
						snprintf(aviso, sizeof(aviso), "Avería detectada en %s: riego interrumpido inmediatamente", id_estacion);
						notificar_fo(NULL, operario, "INFO", aviso);
					}

					// LLAMADA AL PRODUCTOR DE KAFKA
					if (publicar_alerta_kafka(id_estacion) == 0)
						sprintf(respuesta, "STATUS#OK#Alerta registrada en BD y enviada a Kafka");
					else
						sprintf(respuesta, "STATUS#OK#Alerta registrada en BD (Kafka no disponible)");
				}
				else if (strcmp(comando, "ESTADO") == 0)
				{
					/* Latido periódico del Monitor; el tercer campo (aquí en 'ubicacion') vale OK o KO */
					if (strcmp(ubicacion, "OK") == 0)
					{
						// Solo saca a la estación de DISCONNECTED; no pisa otros estados (KO, WATERING...)
						char *sql = sqlite3_mprintf("UPDATE ESTACIONES SET ESTADO = 'AVAILABLE' WHERE ID = %Q AND ESTADO = 'DISCONNECTED';", id_estacion);
						ejecutar_sql(sql);
						sqlite3_free(sql);
					}
					else
					{
						char *sql = sqlite3_mprintf("UPDATE ESTACIONES SET ESTADO = 'KO' WHERE ID = %Q AND ESTADO <> 'OUT_OF_SERVICE';", id_estacion);
						ejecutar_sql(sql);
						sqlite3_free(sql);
					}
					sprintf(respuesta, "STATUS#OK#Estado recibido");
				}
				else if (strcmp(comando, "RECUPERADA") == 0)
				{
					printf("\033[0;32m -> La estación %s ha resuelto la avería y vuelve a estar disponible\033[0m\n\r", id_estacion);
					char *sql = sqlite3_mprintf("UPDATE ESTACIONES SET ESTADO = 'AVAILABLE', CAUDAL = 0, OPERARIO = '' WHERE ID = %Q AND ESTADO <> 'OUT_OF_SERVICE';", id_estacion);
					ejecutar_sql(sql);
					sqlite3_free(sql);
					sprintf(respuesta, "STATUS#OK#Estacion disponible de nuevo");
				}
				else
				{
					sprintf(respuesta, "STATUS#ERROR#Comando no reconocido");
				}
			}
			else
			{
				sprintf(respuesta, "STATUS#ERROR#Trama mal formada");
			}

			n = strlen(respuesta);
			enviados = write(s2, respuesta, n);
			if (enviados == -1 || enviados < n)
			{
				fprintf(stderr, "Error enviando la respuesta\n\r");
			}
			else
			{
				printf(" -> Respuesta enviada al cliente: %s\n\r", respuesta);
			}

			close(s2);
			exit(0); /* El hijo termina su trabajo con esta petición */
		}
		else /* --- SOY EL PADRE --- */
		{
			close(s2); /* El padre cierra su copia de s2 y vuelve a esperar en accept() */
		}
	}

	/**** Paso 7: Cerrar el socket ****/
	close(s);
	if (hijo_kafka > 0) kill(hijo_kafka, SIGTERM);
	if (hijo_admin > 0) kill(hijo_admin, SIGTERM);
	printf("Socket cerrado\n\r");
	return 0;
}
