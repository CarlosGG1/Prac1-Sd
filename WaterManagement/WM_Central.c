#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>

#include <sqlite3.h>
#include <librdkafka/rdkafka.h>

void inicializar_bd() {
    sqlite3 *db;
    char *mensaje_error = 0;
    
    // Abrir o crear el archivo de la base de datos
    int rc = sqlite3_open("central.db", &db);
    if (rc) {
        fprintf(stderr, "\033[0;31mError abriendo base de datos: %s\033[0m\n", sqlite3_errmsg(db));
        exit(EXIT_FAILURE);
    }

    // Consulta SQL para el inventario de estaciones
    const char *sql_estaciones = 
        "CREATE TABLE IF NOT EXISTS ESTACIONES("
        "ID TEXT PRIMARY KEY NOT NULL,"
        "UBICACION TEXT NOT NULL,"
        "ESTADO TEXT NOT NULL);";

    // Ejecutar la consulta
    rc = sqlite3_exec(db, sql_estaciones, NULL, 0, &mensaje_error);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "\033[0;31mError SQL: %s\033[0m\n", mensaje_error);
        sqlite3_free(mensaje_error);
    } else {
        printf("\033[0;32m[BD] Tabla ESTACIONES lista.\033[0m\n");
    }

    sqlite3_close(db);
}

void publicar_alerta_kafka(const char* id_estacion) {
    char errstr[512];
    rd_kafka_t *rk;
    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    // 1. Apuntamos al puerto local 9092 donde escucha tu contenedor de Kafka
    if (rd_kafka_conf_set(conf, "bootstrap.servers", "127.0.0.1:9092", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "\033[0;31mError config Kafka: %s\033[0m\n", errstr);
        return;
    }

    // 2. Creamos la instancia del Productor
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr, "\033[0;31mError creando productor Kafka: %s\033[0m\n", errstr);
        return;
    }

    // 3. Formateamos el mensaje como un JSON para que el Operador web lo procese fácilmente
    char payload[256];
    sprintf(payload, "{\"id_estacion\": \"%s\", \"estado\": \"KO\", \"alerta\": \"FUGA DETECTADA\"}", id_estacion);

    // 4. Enviamos el mensaje al topic "alertas_agua"
    rd_kafka_producev(rk,
                      RD_KAFKA_V_TOPIC("alertas_agua"),
                      RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
                      RD_KAFKA_V_VALUE(payload, strlen(payload)),
                      RD_KAFKA_V_OPAQUE(NULL),
                      RD_KAFKA_V_END);

    // 5. Forzamos el envío inmediato y cerramos
    rd_kafka_flush(rk, 1000);
    rd_kafka_destroy(rk);
    printf("\033[0;34m[KAFKA] Mensaje publicado en el topic: %s\033[0m\n", payload);
}

int s; /* socket */

void
finalizar (int senyal)
{
	printf("Recibida la se�al de fin (cntr-C)\n\r");
	close(s); /* cerrar para que accept termine con un error y salir del bucle principal */
}			//Este void hace que si le doy a ctrl + C, se cierra el socket 

int
main (int argc, char *argv[])
{
	char *servidor_puerto;
	char mensaje[1024];
	struct sockaddr_in dir_servidor, dir_cliente;
	unsigned int long_dir_cliente;
	int s2;
	int n, enviados, recibidos;
	int proceso;
	int contador=0;

	/* Comprobar los argumentos */
	if (argc != 2)  //si o si necesitamos un puerto en el que escuchar, por lo que se necesitan 2 argumentos
					//si hay diferentes salta el error
	{
		fprintf(stderr, "Error. Debe indicar el puerto del servidor\r\n");
		fprintf(stderr, "Sintaxis: %s <puerto>\n\r", argv[0]);
		fprintf(stderr, "Ejemplo : %s 8574\"\n\r", argv[0]);
		return 1;
	}

	/* Tomar los argumentos */		
	servidor_puerto = argv[1];

    inicializar_bd(); //inicializar la base de datos

	/**** Paso 1: Abrir el socket ****/

	s = socket(AF_INET, SOCK_STREAM, 0); /* creo el socket */
					//se crea el socket (un extremo de la comunicación). 
					//AF_INET indica que se usará las direcciones IPv4
					//SOCK_STREAM indica que sera una conexión TCP
	if (s == -1)
	{
		fprintf(stderr, "Error. No se puede abrir el socket\n\r");
		return 1;
	}
	printf("Socket abierto\n\r");

	/**** Paso 2: Establecer la direcci�n (puerto) de escucha ****/

	dir_servidor.sin_family = AF_INET;		
	dir_servidor.sin_port = htons(atoi(servidor_puerto));    //htons() convierte el puerto al formato de red
	dir_servidor.sin_addr.s_addr = INADDR_ANY; //El servidor aceptará cualquier IP de la máquina
	if (bind(s, (struct sockaddr *)&dir_servidor, sizeof(dir_servidor)) == -1) //bind() ata el socket s al puerto especificado por el usuario
	{
		fprintf(stderr, "Error. No se puede asociar el puerto al servidor\n\r");
		close(s);
		return 1;
	}
	printf("Puerto de escucha establecido\n\r");

	/**** Paso 3: Preparar el servidor para escuchar ****/

	if (listen(s, 4) == -1) //pone el socket en modo de escucha, y el 4 es el tamaño de la cola de conexiones
	{
		fprintf(stderr, "Error preparando servidor\n\r");
		close(s);
		return 1;
	}
	printf("Socket preparado\n\r");

	/**** Paso 4: Esperar conexiones ****/

	signal(SIGINT, finalizar); // Asocia Ctrl+C a la función finalizar

	while (1)
	{
		fprintf(stderr, "Esperando conexi�n en el puerto %s...\n\r", servidor_puerto);
		long_dir_cliente = sizeof (dir_cliente);
		s2 = accept (s, (struct sockaddr *)&dir_cliente, &long_dir_cliente);
		contador++;      //accept() lo que hace es bloquear el servidor esperando a que se conecte un cliente
						 //cuando un cliente llama a connect() accept() se desbloquea y devuelve un socket nuevo s2
		/* s2 es el socket para comunicarse con el cliente */
		/* s puede seguir siendo usado para comunicarse con otros clientes */
		if (s2 == -1)
		{
			break; /* salir del bucle */
		}
		/* crear un nuevo proceso para que se pueda atender varias peticiones en paralelo */
		proceso = fork();
		if (proceso == -1) exit(1);
		if (proceso == 0) /* soy el hijo */
		{
			close(s); /* el hijo no necesita el socket general */

			/**** Paso 5: Leer el mensaje ****/

			n = sizeof(mensaje);
			recibidos = read(s2, mensaje, n); //el hijo se bloquea hasta que recibe datos
			if (recibidos == -1)
			{
				fprintf(stderr, "Error leyendo el mensaje\n\r");
				exit(1);
			}
			mensaje[recibidos] = '\0'; /* pongo el final de cadena */
            printf("[Estación #%d] Trama recibida: %s\n\r", contador, mensaje);

			/**** Paso 6: Enviar respuesta ****/
            char comando[50];
            char id_estacion[50];
            char ubicacion[100];
            char respuesta[256];

            // Intentamos extraer los 3 campos separados por '#'
            if (sscanf(mensaje, "%49[^#]#%49[^#]#%99[^\n]", comando, id_estacion, ubicacion) == 3) 
            {
                if (strcmp(comando, "REGISTRO") == 0) 
                {
                    // Mostramos la información de la estación conectada por consola
                    printf(" -> ¡Estación registrada con éxito!\n\r");
                    printf("    ID: %s\n\r", id_estacion);
                    printf("    Ubicación: %s\n\r", ubicacion);

                    // --- NUEVO CÓDIGO SQLITE PARA GUARDAR EL REGISTRO ---
                    sqlite3 *db;
                    char *err_msg = 0;

                    // 1. Abrir la conexión a la base de datos en el proceso hijo
                    if (sqlite3_open("central.db", &db) == SQLITE_OK) {
                        char sql_insert[512];
    
                        // 2. Construir la consulta. Usamos REPLACE por si la estación se reinicia y vuelve a registrarse
                        sprintf(sql_insert, "REPLACE INTO ESTACIONES (ID, UBICACION, ESTADO) VALUES ('%s', '%s', 'AVAILABLE');", id_estacion, ubicacion);
    
                        // 3. Ejecutar la consulta
                        if (sqlite3_exec(db, sql_insert, NULL, 0, &err_msg) != SQLITE_OK) {
                            fprintf(stderr, "\033[0;31mError guardando en BD: %s\033[0m\n", err_msg);
                            sqlite3_free(err_msg);
                        } else {
                            printf("\033[0;32m -> Registro guardado en central.db\033[0m\n");
                        }
    
                        // 4. Cerrar la conexión
                        sqlite3_close(db);
                    }
                    // Preparamos la respuesta estructurada de éxito
                    sprintf(respuesta, "STATUS#OK#Estacion registrada correctamente");
                } else if (strcmp(comando, "ALERTA") == 0) {
                
                    printf("\033[0;31m -> [EMERGENCIA] Fuga reportada en la estacion: %s\033[0m\n\r", id_estacion);
                    
                    sqlite3 *db;
                    if (sqlite3_open("central.db", &db) == SQLITE_OK) {
                        char sql_update[512];
                        sprintf(sql_update, "UPDATE ESTACIONES SET ESTADO = 'KO' WHERE ID = '%s';", id_estacion);
                        sqlite3_exec(db, sql_update, NULL, 0, NULL);
                        sqlite3_close(db);
                    }

                    // LLAMADA AL PRODUCTOR DE KAFKA
                    publicar_alerta_kafka(id_estacion);

                    sprintf(respuesta, "STATUS#OK#Alerta registrada en BD y enviada a Kafka");
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
            exit(0); /* El hijo termina su trabajo con esta estación */
        }
        else /* --- SOY EL PADRE --- */
        {
            close(s2); /* El padre cierra su copia de s2 y vuelve a esperar en accept() */
        }
    }
	

	/**** Paso 7: Cerrar el socket ****/
	close(s);
	printf("Socket cerrado\n\r");
	return 0;
}
