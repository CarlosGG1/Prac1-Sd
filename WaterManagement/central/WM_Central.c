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
 *       ADMIN#TOKEN#WS|todas accion -> órdenes del panel web (iniciar/bloquear/activar); requiere ADMIN_TOKEN
 *
 *  2) Consumidor de KAFKA (proceso hijo creado al arrancar). Atiende los riegos:
 *       PETICION  (topic peticiones_riego) -> valida la estación y, si está libre, ordena abrir (ordenes_ws)
 *       INICIO / RIEGO / FIN (topic eventos_ws) -> actualiza la BD y avisa al operario (notificaciones_fo)
 *
 * Estados de una estación en la BD: AVAILABLE, WATERING, KO (fuga), OUT_OF_SERVICE, DISCONNECTED.
 */
#include <sys/types.h>
#include <sys/socket.h> // Para crear conexiones de red (Sockets TCP)
#include <netinet/in.h> // Para manejar direcciones IP y puertos
#include <errno.h>      // Para gestionar códigos de error
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>     // Para usar fork() y crear procesos paralelos
#include <string.h>
#include <signal.h>     // Para capturar Ctrl+C o cierres repentinos
#include <time.h>       // Para controlar el tiempo de los riegos

#include <sqlite3.h>    // Librería de la base de datos
#include "wm_kafka.h"   // Nuestra librería de comunicaciones

/* Constante vital: si en 15 segundos un parque no da señales, se cancela su riego */
#define SEGUNDOS_SIN_NOTICIAS 15 

/* Si el Monitor de una estación no da señales de vida (ESTADO/ALERTA/RECUPERADA) en 15 segundos,
 * se considera caído y la estación pasa a DISCONNECTED (gris). El Monitor manda ESTADO cada 5 s. */
#define SEGUNDOS_SIN_LATIDO 15

char *broker_ip;     /* IP de Kafka, se rellena al arrancar el programa */
char *broker_puerto; /* Puerto de Kafka */

/* pid_t guarda los identificadores de los "hijos". La Central se clonará a sí misma */
pid_t hijo_kafka;    /* Clon 1: Se dedicará solo a escuchar mensajes de Kafka */
pid_t hijo_admin;    /* Clon 2: Se dedicará a leer tu teclado si quieres forzar un riego */
int s;               /* El "teléfono principal" (socket) que escucha a los Monitores */










/* ======================= BASE DE DATOS ======================= */





/*
 * Ejecuta un comando SQL (INSERT, UPDATE, DELETE).
 * El parámetro 'silencioso' sirve para que, si da error, no manche la pantalla (1 = callado, 0 = chivato).
 */
int ejecutar_sql_conf(const char *sql, int silencioso)
{
    sqlite3 *db;
    char *err_msg = 0;
    int cambios = 0;

    // 1. Abre el archivo de la base de datos
    if (sqlite3_open("central.db", &db) != SQLITE_OK)
    {
        fprintf(stderr, "\033[0;31mError abriendo BD: %s\033[0m\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return -1;
    }
    
    // 2. LA LÍNEA MÁS IMPORTANTE PARA EL PROFESOR:
    // Como la Central tiene procesos paralelos, dos podrían intentar escribir a la vez.
    // Esto le dice a SQLite: "Si la BD está ocupada, no falles de golpe, espera hasta 2 segundos".
    sqlite3_busy_timeout(db, 2000); 

    // 3. Ejecuta la frase SQL que le hayamos pasado
    if (sqlite3_exec(db, sql, NULL, 0, &err_msg) != SQLITE_OK)
    {
        // Si falla y no es modo silencioso, imprime el error en rojo
        if (!silencioso) fprintf(stderr, "\033[0;31mError SQL: %s\033[0m\n", err_msg);
        sqlite3_free(err_msg);
        cambios = -1; // -1 significa error
    }
    else
    {
        // 4. Si ha ido bien, pregunta a SQLite cuántas filas ha modificado (ej: 1 válvula actualizada)
        cambios = sqlite3_changes(db);
    }
    
    // 5. Cierra la base de datos para liberar el archivo
    sqlite3_close(db);
    return cambios;
}





/* 
 * Atajo rápido: Llama a la función de arriba en modo normal (silencioso = 0),
 * para que siempre avise si hay errores.
 */
int ejecutar_sql(const char *sql)
{
    return ejecutar_sql_conf(sql, 0);
}





/* 
 * Cambia el estado de una estación en la BD.
 */
int actualizar_estado(const char *id, const char *estado)
{
    // Construye la frase SQL segura usando %Q.
    char *sql = sqlite3_mprintf("UPDATE ESTACIONES SET ESTADO = %Q WHERE ID = %Q;", estado, id);
    
    // Llama a la función del bloque anterior para ejecutarla.
    int rc = ejecutar_sql(sql);
    
    // Libera la memoria de la frase para que la RAM no se llene.
    sqlite3_free(sql);
    return rc;
}





/* Lee el estado y el operario de una estación. Devuelve 1 si existe y 0 si no. */
int obtener_estacion(const char *id, char *estado, int tam_estado, char *operario, int tam_operario)
{
    sqlite3 *db;
    sqlite3_stmt *st; // Objeto especial para "consultas preparadas"
    int existe = 0;

    if (sqlite3_open("central.db", &db) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    sqlite3_busy_timeout(db, 2000); // Espera si la BD está ocupada

    // 1. Prepara la pregunta buscando por ID (el símbolo '?' es un hueco a rellenar)
    if (sqlite3_prepare_v2(db, "SELECT ESTADO, IFNULL(OPERARIO, '') FROM ESTACIONES WHERE ID = ?;", -1, &st, NULL) == SQLITE_OK)
    {
        // 2. Rellena el hueco '?' con el ID real de la estación (ej: "WS-01")
        sqlite3_bind_text(st, 1, id, -1, SQLITE_TRANSIENT);
        
        // 3. Da un "paso" para leer la fila de resultados
        if (sqlite3_step(st) == SQLITE_ROW)
        {
            // Copia el texto de la columna 0 (Estado) a nuestra variable
            snprintf(estado, tam_estado, "%s", (const char *)sqlite3_column_text(st, 0));
            // Copia el texto de la columna 1 (Operario) a nuestra variable
            snprintf(operario, tam_operario, "%s", (const char *)sqlite3_column_text(st, 1));
            existe = 1; // Confirmamos que el parque existe
        }
        sqlite3_finalize(st); // Destruye la consulta preparada
    }
    sqlite3_close(db);
    return existe;
}





void inicializar_bd()
{
    sqlite3 *db;
    char *mensaje_error = 0;

    int rc = sqlite3_open("central.db", &db);
    if (rc) {
        fprintf(stderr, "\033[0;31mError abriendo base de datos: %s\033[0m\n", sqlite3_errmsg(db));
        exit(EXIT_FAILURE);
    }

    // 1. Define la estructura de la tabla con los tipos de datos (TEXT, REAL para decimales, INTEGER)
    const char *sql_estaciones =
        "CREATE TABLE IF NOT EXISTS ESTACIONES("
        "ID TEXT PRIMARY KEY NOT NULL,"
        "UBICACION TEXT NOT NULL,"
        "ESTADO TEXT NOT NULL,"
        "CAUDAL REAL DEFAULT 0,"
        "VOLUMEN REAL DEFAULT 0,"
        "OPERARIO TEXT DEFAULT '',"
        "ULTIMA_ACT INTEGER DEFAULT 0);";

    // Ejecuta la creación de la tabla
    rc = sqlite3_exec(db, sql_estaciones, NULL, 0, &mensaje_error);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "\033[0;31mError SQL: %s\033[0m\n", mensaje_error);
        sqlite3_free(mensaje_error);
    } else {
        printf("\033[0;32m[BD] Tabla ESTACIONES lista.\033[0m\n");
    }
    sqlite3_close(db);

    // 2. PARCHES DE ACTUALIZACIÓN: Si la BD es de una versión vieja, intenta añadirle las columnas nuevas.
    // (Usa la función 'silenciosa' para que, si las columnas ya existen, ignore el error y no asuste).
    ejecutar_sql_conf("ALTER TABLE ESTACIONES ADD COLUMN CAUDAL REAL DEFAULT 0;", 1);
    ejecutar_sql_conf("ALTER TABLE ESTACIONES ADD COLUMN VOLUMEN REAL DEFAULT 0;", 1);
    ejecutar_sql_conf("ALTER TABLE ESTACIONES ADD COLUMN OPERARIO TEXT DEFAULT '';", 1);
    ejecutar_sql_conf("ALTER TABLE ESTACIONES ADD COLUMN ULTIMA_ACT INTEGER DEFAULT 0;", 1);
    ejecutar_sql_conf("ALTER TABLE ESTACIONES ADD COLUMN ULTIMO_LATIDO INTEGER DEFAULT 0;", 1);

    // 3. MEDIDA DE RESILIENCIA AL REINICIAR (Súper importante para la nota de la defensa)
    // Pone a todas las estaciones en gris (DISCONNECTED) excepto a las que ya estaban rotas del todo (OUT_OF_SERVICE).
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





/*
 * QUÉ HACE: Es un simple traductor. Coge una palabra clave (ej: "FUGA")
 * y devuelve una frase entendible para que el operario la lea en su pantalla.
 */
const char *texto_motivo(const char *motivo)
{
    // Compara la palabra (strcmp) y devuelve la frase correspondiente.
    if (strcmp(motivo, "TIEMPO") == 0) return "tiempo programado cumplido";
    if (strcmp(motivo, "MANUAL") == 0) return "finalizado manualmente en la estacion";
    if (strcmp(motivo, "FUGA") == 0) return "interrumpido por una fuga";
    if (strcmp(motivo, "ORDEN_CENTRAL") == 0) return "orden de CENTRAL";
    
    return motivo; // Si no conoce la palabra, la devuelve tal cual.
}





// Aviso al compilador de que esta función existe más abajo
void listar_disponibles(rd_kafka_t *prod, const char *operario); 

/*
 * QUÉ HACE: Recibe la petición del jardinero, valida el parque en la BD y
 * si está todo bien, se lo reserva y ordena a la válvula que se abra.
 */
void tratar_peticion(rd_kafka_t *prod, const char *operario, const char *ws, const char *segundos)
{
    char texto[300], estado[32] = "", operario_actual[50] = "";
    
    // 1. Convierte los segundos de texto ("20") a número (20). 
    // Si da error o es cero, pone el tiempo por defecto (20s).
    int duracion = atoi(segundos);
    if (duracion <= 0) duracion = DURACION_POR_DEFECTO;

    // Imprime en la consola de la Central (para que tú lo veas)
    printf("\033[0;36m[PETICIÓN] El operario %s solicita riego en %s durante %d s\033[0m\n", operario, ws, duracion);
    
    // Le envía un mensaje INFO al operario para decirle "Tranquilo, estoy mirándolo"
    snprintf(texto, sizeof(texto), "Petición recibida: riego en %s durante %d s. Validando estación...", ws, duracion);
    notificar_fo(prod, operario, "INFO", texto);

    // 2. VALIDACIÓN 1: ¿El parque existe?
    // Llama a la función 'obtener_estacion' (del bloque anterior). Si devuelve 0, no existe.
    if (!obtener_estacion(ws, estado, sizeof(estado), operario_actual, sizeof(operario_actual)))
    {
        snprintf(texto, sizeof(texto), "La estación %s no existe", ws);
        printf("\033[0;31m -> DENEGADO: %s\033[0m\n", texto);
        notificar_fo(prod, operario, "DENEGADO", texto); // Le dice al operario que no.
        return; // Termina la función aquí.
    }

    // 3. VALIDACIÓN 2: ¿El parque está libre?
    // Si no está AVAILABLE (Verde), busca el motivo y le deniega el riego al operario.
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
        return; // Termina la función aquí.
    }

    // 4. RESERVA ATÓMICA (Súper importante para la nota)
    /*
     * TRUCO PRO: Imagina que el Operario 1 y el Operario 2 piden regar el MISMO parque 
     * en el MISMO milisegundo. Si hiciéramos un UPDATE normal, chocarían.
     * Al poner "AND ESTADO = 'AVAILABLE'" en el UPDATE, la base de datos se encarga:
     * el primero que llega cambia el estado a WATERING, y el segundo falla porque ya no está AVAILABLE.
     */
    char *sql = sqlite3_mprintf(
        "UPDATE ESTACIONES SET ESTADO = 'WATERING', OPERARIO = %Q, CAUDAL = 0, VOLUMEN = 0, ULTIMA_ACT = %ld "
        "WHERE ID = %Q AND ESTADO = 'AVAILABLE';",
        operario, (long)time(NULL), ws);
        
    int cambios = ejecutar_sql(sql); // Ejecuta el cambio. Devuelve el número de filas modificadas.
    sqlite3_free(sql);
    
    // Si 'cambios' no es 1, es que otro operario se nos adelantó en ese milisegundo.
    if (cambios != 1)
    {
        snprintf(texto, sizeof(texto), "La estación %s acaba de ser ocupada por otro operario", ws);
        printf("\033[0;31m -> DENEGADO: %s\033[0m\n", texto);
        notificar_fo(prod, operario, "DENEGADO", texto);
        return;
    }

    // 5. ¡VÍA LIBRE!
    snprintf(texto, sizeof(texto), "Estación %s disponible. Solicitando autorización a la estación...", ws);
    printf("\033[0;32m -> Estación disponible. Se envía la orden de apertura a %s\033[0m\n", ws);
    notificar_fo(prod, operario, "INFO", texto);

    // Construye el mensaje final (ej: "ABRIR#WS-01#OP-1#20") y lo manda por Kafka al topic 'ordenes_ws'
    char orden[128];
    snprintf(orden, sizeof(orden), "ABRIR#%s#%s#%d", ws, operario, duracion);
    publicar(prod, TOPIC_ORDENES, ws, orden);
}





/* 
 * QUÉ HACE: Lee los mensajes que llegan desde el Parque (Engine). 
 * Puede ser INICIO, RIEGO (los litros que caen) o FIN.
 */
void tratar_evento(rd_kafka_t *prod, char *campos[], int n)
{
    char texto[400];

    // CASO 1: La válvula dice "INICIO" (ha abierto el agua correctamente)
    if (strcmp(campos[0], "INICIO") == 0 && n >= 3)
    {
        printf("\033[0;32m[%s] Riego iniciado: electroválvula ABIERTA (operario %s)\033[0m\n", campos[1], campos[2]);
        snprintf(texto, sizeof(texto), "La estación %s ha autorizado el riego. Electroválvula ABIERTA.", campos[1]);
        // Le dice al FO (el operario) que todo va OK
        notificar_fo(prod, campos[2], "OK", texto);
    }
    // CASO 2: La válvula dice "RIEGO" (mensaje que llega CADA SEGUNDO)
    else if (strcmp(campos[0], "RIEGO") == 0 && n >= 5)
    {
        /* 
         * atof() convierte texto ("10.5") a números decimales (10.5).
         * time(NULL) coge la hora exacta de ahora mismo.
         * Actualiza en la base de datos el caudal, el volumen y la "última vez que respiró" (ULTIMA_ACT).
         */
        char *sql = sqlite3_mprintf(
            "UPDATE ESTACIONES SET CAUDAL = %f, VOLUMEN = %f, ULTIMA_ACT = %ld WHERE ID = %Q AND ESTADO = 'WATERING';",
            atof(campos[3]), atof(campos[4]), (long)time(NULL), campos[1]);
            
        // Si el UPDATE funciona (devuelve 1), imprime los datos por pantalla
        if (ejecutar_sql(sql) == 1)
            printf("[%s] REGANDO  caudal %s L/min  volumen %s L  (operario %s)\n", campos[1], campos[3], campos[4], campos[2]);
        sqlite3_free(sql);
    }
    // CASO 3: La válvula dice "FIN" (ha terminado o se ha roto)
    else if (strcmp(campos[0], "FIN") == 0 && n >= 6)
    {
        /* 
         * TRUCO DE RESILIENCIA: Solo vuelve a poner el estado en AVAILABLE si estaba WATERING. 
         * Si el parque se había roto (KO), este UPDATE falla a propósito para que no se ponga verde.
         */
        char *sql = sqlite3_mprintf(
            "UPDATE ESTACIONES SET ESTADO = 'AVAILABLE', CAUDAL = 0, OPERARIO = '' WHERE ID = %Q AND ESTADO = 'WATERING';",
            campos[1]);
        ejecutar_sql(sql);
        sqlite3_free(sql);

        // Si el motivo del fin es "RECHAZADO" (porque había una fuga antes de empezar, por ejemplo)
        if (strncmp(campos[5], "RECHAZADO", 9) == 0)
        {
            snprintf(texto, sizeof(texto), "La estación %s ha rechazado el riego (%s)", campos[1], campos[5]);
            printf("\033[0;31m[%s] Riego rechazado por la estación: %s\033[0m\n", campos[1], campos[5]);
            notificar_fo(prod, campos[2], "DENEGADO", texto);
        }
        else // Si ha terminado de forma normal o por tiempo
        {
            // Construye la frase de resumen final usando el traductor que vimos antes (texto_motivo)
            snprintf(texto, sizeof(texto), "Riego en %s terminado. Volumen total %s L, duración %s s, fin por: %s",
                     campos[1], campos[3], campos[4], texto_motivo(campos[5]));
            printf("\033[0;34m[%s] Riego finalizado. %s\033[0m\n", campos[1], texto);
            // Avisa al operario con el resumen (esto rompe el bucle de espera de la tablet del operario)
            notificar_fo(prod, campos[2], "RESUMEN", texto);
        }
    }
}





/* 
 * QUÉ HACE: Es la primera función que se llama cuando Kafka escupe un mensaje nuevo.
 * Lo trocea y decide a qué función específica llamar.
 */
void tratar_mensaje(rd_kafka_t *prod, const char *payload, size_t len)
{
    char buffer[512];
    char *campos[8];

    // 1. Limpieza y seguridad: copia el mensaje a un buffer temporal y le pone el '\0' final
    if (len >= sizeof(buffer)) len = sizeof(buffer) - 1;
    memcpy(buffer, payload, len);
    buffer[len] = '\0';

    // 2. Trocea el mensaje por las almohadillas (#)
    int n = dividir_campos(buffer, campos, 8);
    
    // 3. Enrutamiento: Mira la primera palabra (campos[0]) para saber de qué trata
    if (strcmp(campos[0], "PETICION") == 0 && n >= 4)
        tratar_peticion(prod, campos[1], campos[2], campos[3]); // Llama a la función del bloque anterior
    else if (strcmp(campos[0], "LISTAR") == 0 && n >= 2)
        listar_disponibles(prod, campos[1]); // Envía la lista de parques libres
    else
        tratar_evento(prod, campos, n); // Si no es Petición ni Listar, asume que es un evento del agua
}










/* ======================= ADMINISTRACIÓN DESDE CENTRAL Y LISTADO PARA OPERARIOS ======================= */





#define MAX_ESTACIONES 100

/* 
 * QUÉ HACE: Va a la base de datos y extrae una lista de estaciones.
 * Permite filtrar: si le pasas "AVAILABLE", solo saca las que están libres.
 */
int leer_estaciones(char ids[][50], char estados[][32], int max, const char *filtro)
{
    sqlite3 *db;
    sqlite3_stmt *st;
    int n = 0; // Contador de estaciones encontradas

    if (sqlite3_open("central.db", &db) != SQLITE_OK) {
        sqlite3_close(db);
        return 0;
    }
    sqlite3_busy_timeout(db, 2000);
    
    // Prepara la consulta para sacar TODAS las estaciones ordenadas por ID
    if (sqlite3_prepare_v2(db, "SELECT ID, ESTADO FROM ESTACIONES ORDER BY ID;", -1, &st, NULL) == SQLITE_OK)
    {
        // Bucle: va leyendo fila a fila (parque a parque) hasta llegar al tope máximo
        while (sqlite3_step(st) == SQLITE_ROW && n < max)
        {
            const char *estado = (const char *)sqlite3_column_text(st, 1);
            
            // Si nos han pedido un filtro (ej: "AVAILABLE") y este parque NO lo cumple, lo saltamos
            if (filtro && strcmp(estado, filtro) != 0) continue;
            
            // Copia el ID y el estado a las matrices que nos han pasado como parámetro
            snprintf(ids[n], 50, "%s", (const char *)sqlite3_column_text(st, 0));
            snprintf(estados[n], 32, "%s", estado);
            n++; // Suma 1 al contador
        }
        sqlite3_finalize(st);
    }
    sqlite3_close(db);
    return n; // Devuelve cuántas ha encontrado
}





/* 
 * QUÉ HACE: Construye un mensaje de texto con todos los parques libres 
 * y se lo envía al operario que lo ha pedido.
 */
void listar_disponibles(rd_kafka_t *prod, const char *operario)
{
    static char ids[MAX_ESTACIONES][50], estados[MAX_ESTACIONES][32];
    char texto[1024] = "";
    
    // Llama a la función de arriba pidiendo solo las "AVAILABLE"
    int i, n = leer_estaciones(ids, estados, MAX_ESTACIONES, "AVAILABLE");

    if (n == 0)
    {
        snprintf(texto, sizeof(texto), "No hay estaciones disponibles ahora mismo");
    }
    else
    {
        snprintf(texto, sizeof(texto), "Estaciones disponibles (%d):", n);
        
        // Bucle para ir pegando (concatenando) los nombres de los parques libres en una sola frase
        for (i = 0; i < n && strlen(texto) < sizeof(texto) - 80; i++)
        {
            strncat(texto, " ", sizeof(texto) - strlen(texto) - 1);
            strncat(texto, ids[i], sizeof(texto) - strlen(texto) - 1);
        }
    }
    printf("[LISTAR] %s pide las estaciones disponibles -> %s\n", operario, texto);
    
    // Envía la frase resultante al operario a través de Kafka (canal notificaciones_fo)
    notificar_fo(prod, operario, "INFO", texto);
}





/* 
 * QUÉ HACE: Ordena de emergencia que un parque deje de funcionar.
 * Lo deja en estado OUT_OF_SERVICE (naranja en la web).
 */
void bloquear_ws(rd_kafka_t *prod, const char *id)
{
    char estado[32] = "", operario[50] = "";

    // 1. Comprueba si el parque existe
    if (!obtener_estacion(id, estado, sizeof(estado), operario, sizeof(operario)))
    {
        printf("\033[0;31m[ADMIN] La estación %s no existe\033[0m\n", id);
        return;
    }
    
    // 2. LA PARTE CLAVE: Si estaba regando, hay que apagar el agua físicamente
    if (strcmp(estado, "WATERING") == 0)
    {
        char orden[128], aviso[200];
        
        // Manda la orden de cerrar la válvula por Kafka
        snprintf(orden, sizeof(orden), "CERRAR#%s#%s", id, operario);
        publicar(prod, TOPIC_ORDENES, id, orden);
        
        // Avisa al operario de que le han cancelado el riego a medias
        snprintf(aviso, sizeof(aviso), "CENTRAL ha bloqueado %s: riego interrumpido. Estación Fuera de Servicio", id);
        notificar_fo(prod, operario, "INFO", aviso);
    }
    
    // 3. Actualiza la base de datos para que nadie más pueda usarla
    char *sql = sqlite3_mprintf("UPDATE ESTACIONES SET ESTADO = 'OUT_OF_SERVICE', CAUDAL = 0, OPERARIO = '' WHERE ID = %Q;", id);
    ejecutar_sql(sql);
    sqlite3_free(sql);
    
    printf("\033[0;33m[ADMIN] %s bloqueada: FUERA DE SERVICIO\033[0m\n", id);
}





/* 
 * QUÉ HACE: Devuelve a la vida un parque que estaba bloqueado.
 * Lo pasa de OUT_OF_SERVICE a AVAILABLE.
 */
void activar_ws(const char *id)
{
    char estado[32] = "", operario[50] = "";

    if (!obtener_estacion(id, estado, sizeof(estado), operario, sizeof(operario)))
    {
        printf("\033[0;31m[ADMIN] La estación %s no existe\033[0m\n", id);
        return;
    }
    
    // Comprueba de que realmente estaba bloqueada antes de hacer nada
    if (strcmp(estado, "OUT_OF_SERVICE") != 0)
    {
        printf("[ADMIN] %s no estaba bloqueada (estado actual: %s). No se cambia nada.\n", id, estado);
        return;
    }
    
    // Lo vuelve a poner Disponible (verde)
    char *sql = sqlite3_mprintf("UPDATE ESTACIONES SET ESTADO = 'AVAILABLE', CAUDAL = 0, OPERARIO = '' WHERE ID = %Q;", id);
    ejecutar_sql(sql);
    sqlite3_free(sql);
    
    printf("\033[0;32m[ADMIN] %s activada: AVAILABLE\033[0m\n", id);
}





/* 
 * QUÉ HACE: Es un subprograma (que correrá en un proceso hijo) dedicado exclusivamente 
 * a leer el teclado en la terminal de la Central.
 */
void bucle_admin()
{
    // Variables para guardar la lista de parques y lo que escriba el usuario
    static char ids[MAX_ESTACIONES][50], estados[MAX_ESTACIONES][32];
    char linea[200], orden[50], destino[50];
    
    // Crea su propio "altavoz" de Kafka para mandar órdenes
    rd_kafka_t *prod = kafka_crear_productor(broker_ip, broker_puerto);

    // TRUCO: Configura qué pasa si pulsas Ctrl+C en esta ventana.
    // SIG_DFL significa "Default": si pulsas Ctrl+C, este proceso hijo morirá de forma normal.
    signal(SIGINT, SIG_DFL);
    signal(SIGTERM, SIG_DFL);

    // Pinta el menú de ayuda en azul cyan (\033[0;36m)
    printf("\033[0;36m[ADMIN] Menú de CENTRAL: iniciar <WS|todas> [seg] | bloquear <WS|todas> | activar <WS|todas> | estado | ayuda\033[0m\n");

    // BUCLE INTERACTIVO: Se queda bloqueado aquí esperando a que pulses Enter
    while (fgets(linea, sizeof(linea), stdin) != NULL)
    {
        int seg = 0, i, n;
        orden[0] = destino[0] = '\0'; // Limpia las variables
        
        // Extrae las palabras de lo que has escrito. Ej: de "iniciar WS-01 10" saca 3 trozos.
        if (sscanf(linea, "%49s %49s %d", orden, destino, &seg) < 1) continue;

        // COMANDO 1: 'estado' -> Lista todos los parques y cómo están (Verde, Rojo...)
        if (strcmp(orden, "estado") == 0)
        {
            n = leer_estaciones(ids, estados, MAX_ESTACIONES, NULL); // El NULL significa "dámelas todas"
            for (i = 0; i < n; i++) printf("  %-10s %s\n", ids[i], estados[i]);
            if (n == 0) printf("  (no hay estaciones registradas)\n");
        }
        // COMANDO 2: 'ayuda' -> Imprime el manual de uso
        else if (strcmp(orden, "ayuda") == 0)
        {
            printf("  iniciar <WS-01|todas> [segundos]   inicia un riego como si lo pidiera un operario\n");
            printf("  bloquear <WS-01|todas>             corta el riego y pone la estación Fuera de Servicio\n");
            printf("  activar <WS-01|todas>              devuelve a AVAILABLE las estaciones bloqueadas\n");
            printf("  estado                             lista todas las estaciones con su estado\n");
        }
        // COMANDOS DE ACCIÓN: Iniciar, Bloquear o Activar
        else if ((strcmp(orden, "iniciar") == 0 || strcmp(orden, "bloquear") == 0 || strcmp(orden, "activar") == 0) && destino[0])
        {
            // ¿Has escrito la palabra mágica "todas"?
            int todas = (strcmp(destino, "todas") == 0);
            
            // Si pusiste "todas", saca la lista completa de la BD. Si no, solo coge la que has escrito (n=1).
            if (todas) n = leer_estaciones(ids, estados, MAX_ESTACIONES, NULL);
            else { snprintf(ids[0], 50, "%s", destino); n = 1; }

            // Bucle mágico: Ejecuta la acción una vez (si era un parque concreto) o N veces (si eran "todas")
            for (i = 0; i < n; i++)
            {
                // Si ordenaste "iniciar", usa la función tratar_peticion haciéndose pasar por un operario llamado "CENTRAL"
                if (strcmp(orden, "iniciar") == 0)
                {
                    char segundos[16];
                    snprintf(segundos, sizeof(segundos), "%d", seg);
                    printf("\033[0;36m[ADMIN] CENTRAL ordena iniciar riego en %s\033[0m\n", ids[i]);
                    tratar_peticion(prod, "CENTRAL", ids[i], segundos);
                }
                // Si ordenaste "bloquear", llama a la función de emergencia que corta el agua
                else if (strcmp(orden, "bloquear") == 0)
                    bloquear_ws(prod, ids[i]);
                // Si ordenaste "activar", la devuelve a la normalidad
                else
                    activar_ws(ids[i]);
            }
            
            // Obliga a Kafka a enviar todos los mensajes que se hayan generado en este bucle AHORA MISMO
            rd_kafka_poll(prod, 200);
            rd_kafka_flush(prod, 2000);
        }
        else
        {
            printf("  Orden no reconocida. Escribe 'ayuda'.\n");
        }
    }
    // Si sales del bucle (ej: pulsando Ctrl+D), este proceso hijo termina
    exit(0);
}





/* 
 * QUÉ HACE: Busca en la base de datos parques que estén "WATERING" pero que 
 * lleven más de 15 segundos (SEGUNDOS_SIN_NOTICIAS) sin mandar una actualización de agua.
 * Si encuentra alguno, lo libera (AVAILABLE) por seguridad.
 */
void vigilar_estaciones()
{
    // Construye la orden SQL. 
    // time(NULL) es la hora actual. Le resta 15 segundos. 
    // Si la "ULTIMA_ACT" (última actualización) es más antigua que eso, ¡lo caza!
    char *sql = sqlite3_mprintf(
        "UPDATE ESTACIONES SET ESTADO = 'AVAILABLE', CAUDAL = 0, OPERARIO = '' WHERE ESTADO = 'WATERING' AND ULTIMA_ACT < %ld;",
        (long)time(NULL) - SEGUNDOS_SIN_NOTICIAS);
        
    // Ejecuta el SQL. Si ejecutar_sql devuelve un número mayor que 0, es que ha liberado alguna estación.
    if (ejecutar_sql(sql) > 0)
        printf("\033[0;33m[VIGILANTE] Una estación llevaba más de %d s regando sin noticias: liberada.\033[0m\n", SEGUNDOS_SIN_NOTICIAS);
        
    sqlite3_free(sql);

    // Segunda vigilancia: ¿algún Monitor ha dejado de dar señales de vida? Entonces su estación pasa a DISCONNECTED.
    // No se toca una estación que esté regando (el riego termina solo) ni una bloqueada por el administrador.
    sql = sqlite3_mprintf(
        "UPDATE ESTACIONES SET ESTADO = 'DISCONNECTED', CAUDAL = 0, OPERARIO = '' "
        "WHERE ESTADO NOT IN ('DISCONNECTED', 'OUT_OF_SERVICE', 'WATERING') AND ULTIMO_LATIDO < %ld;",
        (long)time(NULL) - SEGUNDOS_SIN_LATIDO);
    if (ejecutar_sql(sql) > 0)
        printf("\033[0;33m[VIGILANTE] Monitor sin señales de vida desde hace más de %d s: estación DESCONECTADA.\033[0m\n", SEGUNDOS_SIN_LATIDO);
    sqlite3_free(sql);
}





/* 
 * QUÉ HACE: Es un subprograma infinito que corre en un "proceso hijo" paralelo. 
 * Su única misión en la vida es leer mensajes de Kafka sin parar y llamar al vigilante 
 * cada 5 segundos.
 */
void bucle_kafka()
{
    // Solo le interesa escuchar estos dos canales (peticiones de operarios y eventos de válvulas)
    const char *topics[] = {TOPIC_PETICIONES, TOPIC_EVENTOS};
    time_t ultima_vigilancia = 0; // Temporizador para el vigilante

    // Si matan el proceso (Ctrl+C), que muera limpiamente
    signal(SIGINT, SIG_DFL);
    signal(SIGTERM, SIG_DFL);

    // Crea su altavoz (productor) y su micrófono (consumidor)
    // Se identifica en Kafka con el grupo "wm_central"
    rd_kafka_t *prod = kafka_crear_productor(broker_ip, broker_puerto);
    rd_kafka_t *cons = kafka_crear_consumidor(broker_ip, broker_puerto, "wm_central", topics, 2);
    
    if (!prod || !cons)
    {
        fprintf(stderr, "\033[0;31m[KAFKA] No se pudo iniciar el consumidor de CENTRAL\033[0m\n");
        exit(1);
    }
    printf("\033[0;32m[KAFKA] CENTRAL escuchando peticiones y eventos de riego en %s:%s\033[0m\n", broker_ip, broker_puerto);

    // BUCLE INFINITO: De aquí no sale nunca (hasta que se apaga la Central)
    while (1)
    {
        // 1. Asoma la oreja a Kafka durante medio segundo (500ms)
        rd_kafka_message_t *m = rd_kafka_consumer_poll(cons, 500);
        
        // 2. Si ha llegado un mensaje y no tiene errores de red...
        if (m)
        {
            if (!m->err) 
                tratar_mensaje(prod, (const char *)m->payload, m->len); // ¡Se lo pasa al cartero que vimos antes!
            rd_kafka_message_destroy(m); // Borra el mensaje de la RAM
        }
        
        // 3. Procesa confirmaciones de los mensajes que nosotros hayamos enviado
        rd_kafka_poll(prod, 0);

        // 4. EL TEMPORIZADOR DEL VIGILANTE: 
        // Comprueba si han pasado 5 segundos desde la última vez que miró la base de datos
        if (time(NULL) - ultima_vigilancia >= 5)
        {
            ultima_vigilancia = time(NULL); // Resetea el cronómetro
            vigilar_estaciones(); // Ejecuta la rutina de limpieza
        }
    }
}










/* ======================= SERVIDOR DE SOCKETS ======================= */





/* 
 * QUÉ HACE: Se dispara cuando apagas la Central (Ctrl+C o docker stop). 
 * Se asegura de matar a sus procesos hijos antes de morir para no dejar "zombis" en la memoria.
 */
void finalizar(int senyal)
{
    printf("Recibida la señal de fin (cntr-C)\n\r");
    if (hijo_kafka > 0) kill(hijo_kafka, SIGTERM); /* Mata al hijo que escucha Kafka */
    if (hijo_admin > 0) kill(hijo_admin, SIGTERM); /* Mata al hijo que lee el teclado */
    close(s); /* Cierra el teléfono principal */
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

    /* Comprueba que le pasas el puerto, la IP de Kafka y el puerto de Kafka al arrancarlo */
    if (argc != 4)
    {
        fprintf(stderr, "Error. Faltan argumentos\r\n");
        fprintf(stderr, "Sintaxis: %s <Puerto> <IP_Broker> <Puerto_Broker>\n\r", argv[0]);
        fprintf(stderr, "Ejemplo : %s 8080 127.0.0.1 9092\n\r", argv[0]);
        return 1;
    }
    servidor_puerto = argv[1]; broker_ip = argv[2]; broker_puerto = argv[3];

    // Evita que los logs salgan duplicados en Docker al usar procesos hijo
    setvbuf(stdout, NULL, _IOLBF, 0);

    inicializar_bd(); // Crea la base de datos central.db si no existe

    /* 
     * ¡LÍNEAS CLAVE! LA CLONACIÓN
     * fork() divide el programa en dos. El hijo se va a la función 'bucle_kafka' y
     * nunca sale de ahí. El padre continúa hacia abajo.
     */
    hijo_kafka = fork();
    if (hijo_kafka == 0)
    {
        bucle_kafka();
        exit(0);
    }

    /* 
     * Segunda clonación. Si el programa detecta que tiene una consola interactiva 
     * (isatty), arranca el menú de administrador.
     */
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
				/* Cualquier mensaje del Monitor cuenta como señal de vida (lo vigila vigilar_estaciones) */
				if (strcmp(comando, "ESTADO") == 0 || strcmp(comando, "ALERTA") == 0 || strcmp(comando, "RECUPERADA") == 0)
				{
					char *sql_vida = sqlite3_mprintf("UPDATE ESTACIONES SET ULTIMO_LATIDO = %ld WHERE ID = %Q;", (long)time(NULL), id_estacion);
					ejecutar_sql(sql_vida);
					sqlite3_free(sql_vida);
				}

				if (strcmp(comando, "REGISTRO") == 0)
				{
					printf(" -> Registro/autenticación de la estación %s (%s)\n\r", id_estacion, ubicacion);

					// REPLACE por si la estación se reinicia y vuelve a registrarse
					char *sql = sqlite3_mprintf("REPLACE INTO ESTACIONES (ID, UBICACION, ESTADO, ULTIMO_LATIDO) VALUES (%Q, %Q, 'AVAILABLE', %ld);", id_estacion, ubicacion, (long)time(NULL));
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
				else if (strcmp(comando, "ADMIN") == 0)
				{
					/*
					 * Administración remota (la manda el panel web de CENTRAL):
					 *     ADMIN#<TOKEN>#<WS|todas> <iniciar [seg]|bloquear|activar|ping>
					 * Hace lo mismo que el menú de teclado de CENTRAL y reutiliza sus funciones.
					 * Solo funciona si CENTRAL se arrancó con la variable de entorno ADMIN_TOKEN
					 * y el token de la trama coincide (en 'id_estacion' viene el token).
					 */
					static char ids[MAX_ESTACIONES][50], estados[MAX_ESTACIONES][32];
					const char *token = getenv("ADMIN_TOKEN");
					char destino[50] = "", accion[50] = "";
					int seg = 0, total, i;

					if (token == NULL || token[0] == '\0')
					{
						sprintf(respuesta, "STATUS#ERROR#Administracion remota desactivada (falta ADMIN_TOKEN)");
					}
					else if (strcmp(id_estacion, token) != 0)
					{
						printf("\033[0;31m -> [ADMIN] Token incorrecto: orden rechazada\033[0m\n\r");
						sprintf(respuesta, "STATUS#ERROR#Token de administracion incorrecto");
					}
					else if (sscanf(ubicacion, "%49s %49s %d", destino, accion, &seg) < 2)
					{
						sprintf(respuesta, "STATUS#ERROR#Orden ADMIN mal formada");
					}
					else if (strcmp(accion, "ping") == 0)
					{
						total = leer_estaciones(ids, estados, MAX_ESTACIONES, NULL);
						sprintf(respuesta, "STATUS#OK#CENTRAL operativa (%d estaciones)", total);
					}
					else if (strcmp(accion, "iniciar") != 0 && strcmp(accion, "bloquear") != 0 && strcmp(accion, "activar") != 0)
					{
						sprintf(respuesta, "STATUS#ERROR#Accion no valida");
					}
					else
					{
						rd_kafka_t *prod = kafka_crear_productor(broker_ip, broker_puerto);

						if (strcmp(destino, "todas") == 0)
						{
							total = leer_estaciones(ids, estados, MAX_ESTACIONES, NULL);
						}
						else
						{
							snprintf(ids[0], 50, "%s", destino);
							total = 1;
						}
						printf("\033[0;36m[ADMIN] Orden '%s' desde el panel web sobre %s\033[0m\n\r", accion, destino);

						for (i = 0; i < total; i++)
						{
							if (strcmp(accion, "iniciar") == 0)
							{
								char segundos[16];
								snprintf(segundos, sizeof(segundos), "%d", seg);
								tratar_peticion(prod, "CENTRAL", ids[i], segundos);
							}
							else if (strcmp(accion, "bloquear") == 0)
								bloquear_ws(prod, ids[i]);
							else
								activar_ws(ids[i]);
						}
						if (prod)
						{
							rd_kafka_poll(prod, 200);
							rd_kafka_flush(prod, 2000);
							rd_kafka_destroy(prod);
						}
						sprintf(respuesta, "STATUS#OK#Orden %s enviada a %d estacion(es)", accion, total);
					}
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
