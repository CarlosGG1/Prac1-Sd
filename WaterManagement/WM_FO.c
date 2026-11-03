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

#define ESPERA_ENTRE_SERVICIOS 4 /* Segundos de pausa obligatoria entre riegos de un fichero */
#define MARGEN_RESPUESTA 25      /* Tiempo extra de espera por si la Central va lenta */

char *operario;                  /* Guarda el nombre de este operario, ej: OP-1 */
rd_kafka_t *productor;           /* El "altavoz" para pedir riegos */
rd_kafka_t *consumidor;          /* El "micrófono" para escuchar a la Central */
volatile int terminado = 0;      /* Interruptor: se pone a 1 cuando la Central dice que el riego ha acabado */










/*
 * QUÉ HACE: Es un "vigilante" que funciona en segundo plano. Escucha todo lo que llega
 * por Kafka y, si el mensaje es para este operario, lo pinta en la pantalla con colores.
 */
void *recibir_mensajes(void *arg)
{
	while (1)
	{
		// Escucha Kafka durante medio segundo. Si no hay nada, vuelve a empezar (continue).
		rd_kafka_message_t *m = rd_kafka_consumer_poll(consumidor, 500);
		if (!m) continue;

		if (!m->err)
		{
			char buffer[512];
			char *campos[6];
			size_t len = m->len;

			// Copia el mensaje recibido a una variable segura (buffer) y le pone un fin de texto '\0'
			if (len >= sizeof(buffer)) len = sizeof(buffer) - 1;
			memcpy(buffer, m->payload, len);
			buffer[len] = '\0';
			
			// Usa la función de wm_kafka.h para trocear el mensaje por las almohadillas '#'
			int n = dividir_campos(buffer, campos, 6);

			// Si el mensaje es "RIEGO" y es para este operario, pinta los litros gastados
			if (strcmp(campos[0], "RIEGO") == 0 && n >= 5)
			{
				if (strcmp(campos[2], operario) == 0)
					printf("   [%s] REGANDO  caudal %s L/min | volumen acumulado %s L\n", campos[1], campos[3], campos[4]);
			}
			// Si el mensaje empieza con el nombre de ESTE operario (Notificación directa de la Central)
			else if (strcmp(campos[0], operario) == 0 && n >= 3)
			{
				const char *tipo = campos[1]; // Puede ser OK, DENEGADO o RESUMEN
				const char *color = "\033[0m"; // Sin color por defecto

				// Pinta de verde si es OK, rojo si es DENEGADO, azul si es el RESUMEN final
				if (strcmp(tipo, "OK") == 0) color = "\033[0;32m";
				else if (strcmp(tipo, "DENEGADO") == 0) color = "\033[0;31m";
				else if (strcmp(tipo, "RESUMEN") == 0) color = "\033[0;34m";

				printf("%s[CENTRAL] %s: %s\033[0m\n", color, tipo, campos[2]);

				// Si la Central nos deniega el riego o nos da el resumen, el servicio ha terminado
				if (strcmp(tipo, "DENEGADO") == 0 || strcmp(tipo, "RESUMEN") == 0)
					terminado = 1; 
			}
		}
		// Borra el mensaje de la memoria para que no se sature la RAM
		rd_kafka_message_destroy(m);
	}
	return NULL;
}










/*
 * QUÉ HACE: Construye el mensaje de petición (ej: PETICION#OP-1#WS-01#20),
 * lo lanza a Kafka y paraliza el programa hasta que la Central conteste que ha terminado.
 */
void solicitar_riego(const char *ws, int segundos)
{
	char peticion[128];
	int esperas;

	// Si el usuario no pone segundos, usa 20 por defecto
	if (segundos <= 0) segundos = DURACION_POR_DEFECTO;

	terminado = 0; // Ponemos el interruptor a 0 (ocupado)
	snprintf(peticion, sizeof(peticion), "PETICION#%s#%s#%d", operario, ws, segundos);
	printf("\n[FO %s] Solicitando riego en %s durante %d s...\n", operario, ws, segundos);
	
	// Envía el mensaje por el topic de peticiones
	if (kafka_publicar(productor, TOPIC_PETICIONES, operario, peticion) != 0)
	{
		printf("[FO %s] No se pudo enviar la petición (¿Kafka caído?)\n", operario);
		return;
	}

	/* 
	 * BUCLE DE ESPERA: Como máximo espera los segundos del riego + 25 s de margen.
	 * Cada vuelta duerme 0.1 segundos (usleep 100000). Si 'terminado' pasa a 1 (porque
	 * el hilo de arriba recibió el RESUMEN), el bucle se rompe y continuamos.
	 */
	for (esperas = 0; esperas < (segundos + MARGEN_RESPUESTA) * 10 && !terminado; esperas++)
		usleep(100000);

	// Si pasó todo el tiempo y la Central no dijo ni mu...
	if (!terminado)
		printf("\033[0;33m[FO %s] CENTRAL no ha concluido la petición a tiempo. Se pasa al siguiente servicio.\033[0m\n", operario);
}










/*
 * QUÉ HACE: Abre un archivo de texto (servicios.txt), lee línea por línea (ej: WS-01#10)
 * y va pidiendo los riegos uno a uno haciendo una pausa de 4 segundos entre ellos.
 */
void ejecutar_fichero(const char *ruta)
{
	char linea[128], ws[64];
	int segundos;
	FILE *f = fopen(ruta, "r"); // Abre el archivo en modo lectura ("r")

	if (!f)
	{
		printf("[FO %s] No se puede abrir el fichero '%s'\n", operario, ruta);
		return;
	}
	
	// Lee línea a línea hasta que se acabe el archivo
	while (fgets(linea, sizeof(linea), f) != NULL)
	{
		linea[strcspn(linea, "\r\n")] = '\0'; // Quita el salto de línea al final
		
		// Si la línea está vacía o es un comentario (//), la ignora
		if (linea[0] == '\0' || strncmp(linea, "//", 2) == 0) continue;

		segundos = 0;
		// Extrae el nombre del parque y los segundos. Si no cuadra, salta de línea.
		if (sscanf(linea, "%63[^# \t]#%d", ws, &segundos) < 1) continue;

		solicitar_riego(ws, segundos); // Llama a la función de arriba
		
		printf("[FO %s] Esperando %d s antes del siguiente servicio...\n", operario, ESPERA_ENTRE_SERVICIOS);
		sleep(ESPERA_ENTRE_SERVICIOS); // Duerme 4 segundos como se pide
	}
	fclose(f); // Cierra el archivo
}










/*
 * QUÉ HACE: Es el punto de arranque. Conecta con Kafka, arranca el hilo vigía
 * en segundo plano y muestra un menú interactivo para que escribas comandos.
 */
int main(int argc, char *argv[])
{
	char *ip_broker, *puerto_broker;
	char grupo[80], linea[128], ws[64], ruta[100];
	int segundos;
	pthread_t hilo; // Variable para crear el hilo

	// Validación de argumentos iniciales (IP, Puerto, Nombre)
	if (argc != 4 && argc != 5)
	{
		fprintf(stderr, "Sintaxis: %s <IP_Broker> <Puerto_Broker> <ID_Operario> [fichero_servicios]\n", argv[0]);
		return 1;
	}

	ip_broker = argv[1];
	puerto_broker = argv[2];
	operario = argv[3];

	setvbuf(stdout, NULL, _IOLBF, 0); // Imprime el texto de inmediato (útil para Docker)

	// 1. Crea el emisor de Kafka
	productor = kafka_crear_productor(ip_broker, puerto_broker);

	// 2. Crea el receptor de Kafka. 
	// TRUCO: Al grupo le añade la hora (time) y el ID del proceso (getpid). 
	// Así, si el operario cierra y vuelve a abrir la app, Kafka lo ve como un usuario nuevo
	// y no le bombardea con mensajes viejos que se quedaron atascados.
	snprintf(grupo, sizeof(grupo), "wm_fo_%s_%ld_%d", operario, (long)time(NULL), (int)getpid());
	const char *topics[] = {TOPIC_NOTIFICACIONES, TOPIC_EVENTOS}; // Solo escucha estos 2 canales
	consumidor = kafka_crear_consumidor(ip_broker, puerto_broker, grupo, topics, 2);
	
	// 3. Arranca el vigilante en segundo plano (ejecuta la función recibir_mensajes)
	pthread_create(&hilo, NULL, recibir_mensajes, NULL);

	// Si al arrancar le pasaste un archivo txt, lo ejecuta de golpe
	if (argc == 5)
		ejecutar_fichero(argv[4]);

	// BUCLE INTERACTIVO: Lee lo que escribes por teclado en tiempo real
	while (fgets(linea, sizeof(linea), stdin) != NULL)
	{
		linea[strcspn(linea, "\r\n")] = '\0'; // Limpia el salto de línea

		if (linea[0] == '\0') continue;
		if (strcmp(linea, "q") == 0) break; // Con 'q', sale del programa

		// Con 'l', pide a la Central que le envíe la lista de parques libres
		if (strcmp(linea, "l") == 0)
		{
			char peticion[128];
			snprintf(peticion, sizeof(peticion), "LISTAR#%s", operario);
			kafka_publicar(productor, TOPIC_PETICIONES, operario, peticion);
			continue;
		}

		// Con 'f ruta.txt', ejecuta un archivo de servicios manualmente
		if (sscanf(linea, "f %99s", ruta) == 1)
		{
			ejecutar_fichero(ruta);
			continue;
		}

		// Si escribes "WS-01 15", saca los datos y pide el riego
		segundos = 0;
		if (sscanf(linea, "%63s %d", ws, &segundos) >= 1)
			solicitar_riego(ws, segundos);
	}

	return 0; // Fin del programa
}
