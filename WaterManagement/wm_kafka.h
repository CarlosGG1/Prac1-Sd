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

/* CONSTANTES DE LOS CANALES: Aquí se definen las 5 salas de chat de Kafka */
#define TOPIC_PETICIONES     "peticiones_riego"
#define TOPIC_NOTIFICACIONES "notificaciones_fo"
#define TOPIC_ORDENES        "ordenes_ws"
#define TOPIC_EVENTOS        "eventos_ws"
#define TOPIC_ALERTAS        "alertas_agua"

#define DURACION_POR_DEFECTO 20 /* Tiempo por defecto si el operario no pone los segundos */










//* Crea un productor de Kafka. Devuelve NULL si falla. */
__attribute__((unused)) static rd_kafka_t *kafka_crear_productor(const char *ip, const char *puerto)
{
    char errstr[512], brokers[256];
    rd_kafka_t *rk;
    
    // 1. Crea un objeto de configuración vacío
    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    // 2. Le dice a Kafka dónde está el servidor (IP:Puerto)
    snprintf(brokers, sizeof(brokers), "%s:%s", ip, puerto);
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "Error config Kafka: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return NULL;
    }
    
    // 3. TRUCO DE RESILIENCIA: Limita el tiempo de espera a 5 segundos (5000 ms).
    // Así, si Kafka se cae, el programa no se queda bloqueado infinitamente intentando conectar.
    rd_kafka_conf_set(conf, "message.timeout.ms", "5000", errstr, sizeof(errstr));

    // 4. Crea el productor final con la configuración que hemos preparado
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk)
    {
        fprintf(stderr, "Error creando productor Kafka: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return NULL;
    }
    return rk; // Devuelve el "altavoz" listo para usarse
}










/* Publica 'mensaje' en 'topic' con la clave indicada. Devuelve 0 si se encoló bien y -1 si no. */
__attribute__((unused)) static int kafka_publicar(rd_kafka_t *rk, const char *topic, const char *clave, const char *mensaje)
{
    rd_kafka_resp_err_t err;
    int intentos = 0;

    // Bucle de reintento por si la red está saturada
    do
    {
        // Esta es la función nativa de la librería que inyecta el mensaje en Kafka
        err = rd_kafka_producev(rk,
                                RD_KAFKA_V_TOPIC(topic),
                                RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY), // Hace una copia en memoria segura
                                RD_KAFKA_V_KEY((void *)clave, strlen(clave)), // Clave para mantener el orden
                                RD_KAFKA_V_VALUE((void *)mensaje, strlen(mensaje)), // El texto del mensaje
                                RD_KAFKA_V_END);
                                
        // Si Kafka nos dice que su memoria intermedia (cola) está llena, esperamos 100ms
        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
            rd_kafka_poll(rk, 100); 
            
    } while (err == RD_KAFKA_RESP_ERR__QUEUE_FULL && ++intentos < 5); // Máximo 5 intentos

    // Se asegura de procesar las confirmaciones de que el mensaje ha llegado
    rd_kafka_poll(rk, 0); 
    
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        fprintf(stderr, "[KAFKA] Error publicando en %s: %s\n", topic, rd_kafka_err2str(err));
        return -1;
    }
    return 0; // Éxito
}










/*
 * Crea un productor temporal, publica un mensaje, espera a que salga y lo destruye.
 * Para procesos que publican muy de vez en cuando (por ejemplo, los hijos de CENTRAL).
 */
__attribute__((unused)) static int kafka_publicar_una_vez(const char *ip, const char *puerto, const char *topic, const char *clave, const char *mensaje)
{
    // 1. Crea el altavoz
    rd_kafka_t *rk = kafka_crear_productor(ip, puerto);
    if (!rk) return -1;

    // 2. Publica el mensaje
    int rc = kafka_publicar(rk, topic, clave, mensaje);
    
    // 3. FLUSH: Fuerza a que el mensaje salga físicamente por el cable de red ahora mismo (espera max 2000ms)
    rd_kafka_flush(rk, 2000);
    
    // 4. Si la cola de salida no está a 0, significa que no pudo llegar al destino
    if (rd_kafka_outq_len(rk) > 0)
    {
        fprintf(stderr, "[KAFKA] No se pudo entregar el mensaje (broker %s:%s no responde)\n", ip, puerto);
        rc = -1;
    }
    
    // 5. Destruye el altavoz para liberar memoria RAM
    rd_kafka_destroy(rk);
    return rc;
}










/*
 * Crea un consumidor del grupo 'grupo', lo suscribe a los topics indicados y ESPERA a que Kafka
 * le asigne las particiones. 
 */
__attribute__((unused)) static rd_kafka_t *kafka_crear_consumidor(const char *ip, const char *puerto, const char *grupo, const char *topics[], int ntopics)
{
    char errstr[512], brokers[256];
    rd_kafka_t *rk;
    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    int i;

    snprintf(brokers, sizeof(brokers), "%s:%s", ip, puerto);
    
    // Configura IP y el "Grupo" (quiénes somos leyendo)
    if (rd_kafka_conf_set(conf, "bootstrap.servers", brokers, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK ||
        rd_kafka_conf_set(conf, "group.id", grupo, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "Error config Kafka: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return NULL;
    }
    
    // "latest" significa: no me leas mensajes antiguos de ayer, solo lo que llegue a partir de AHORA.
    rd_kafka_conf_set(conf, "auto.offset.reset", "latest", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "allow.auto.create.topics", "true", errstr, sizeof(errstr));

    rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) { /*... manejo de error ...*/ return NULL; }
    
    // Redirige los eventos internos a la cola principal
    rd_kafka_poll_set_consumer(rk);

    // Se suscribe a la lista de topics que le hayamos pasado
    rd_kafka_topic_partition_list_t *lista = rd_kafka_topic_partition_list_new(ntopics);
    for (i = 0; i < ntopics; i++)
        rd_kafka_topic_partition_list_add(lista, topics[i], RD_KAFKA_PARTITION_UA);
    rd_kafka_resp_err_t err = rd_kafka_subscribe(rk, lista);
    rd_kafka_topic_partition_list_destroy(lista);
    
    if (err) { /*... manejo de error ...*/ return NULL; }

    /* ESPERA CRÍTICA: Bucle para dar tiempo a Kafka a asignarnos una conexión real */
    for (i = 0; i < 40; i++)
    {
        // Hace un chequeo rápido a la red (500ms) para forzar la sincronización
        rd_kafka_message_t *m = rd_kafka_consumer_poll(rk, 500); 
        if (m) rd_kafka_message_destroy(m);

        rd_kafka_topic_partition_list_t *asignadas = NULL;
        // Pregunta: "¿Ya me has asignado una partición para empezar a escuchar?"
        int listo = (rd_kafka_assignment(rk, &asignadas) == RD_KAFKA_RESP_ERR_NO_ERROR && asignadas != NULL && asignadas->cnt > 0);
        if (asignadas) rd_kafka_topic_partition_list_destroy(asignadas);
        
        // Si ya estamos conectados, rompemos el bucle y empezamos a funcionar
        if (listo) break; 
    }
    if (i == 40)
        fprintf(stderr, "[KAFKA] Aviso: el broker %s no ha asignado particiones todavía\n", brokers);

    return rk;
}










/*
 * Divide 'texto' por '#' sobre el propio buffer (lo modifica) y deja en campos[] los punteros
 * a cada trozo. Devuelve cuántos campos hay.
 */
__attribute__((unused)) static int dividir_campos(char *texto, char *campos[], int max)
{
    int n = 0;
    char *p = texto;

    // Recorre la frase de texto buscando el símbolo '#'
    while (n < max)
    {
        campos[n++] = p; // Guarda la posición de la palabra actual
        char *sep = (n < max) ? strchr(p, '#') : NULL; // Busca el próximo '#'
        if (!sep) break; // Si no hay más, termina
        
        *sep = '\0'; // TRUCO C: Sustituye el '#' por un fin de cadena (intro oculto)
        p = sep + 1; // Avanza a la siguiente palabra
    }
    return n; // Devuelve cuántos trozos ha encontrado
}

#endif
