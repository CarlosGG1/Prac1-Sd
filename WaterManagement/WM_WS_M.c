
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>

int s; /* socket */

void
finalizar (int senyal)
{
	printf("Recibida la se�al de fin (cntr-C)\n\r");
	close(s); /* cerrar para que accept termine con un error y salir del bucle principal */
}			//Este void hace que si le doy a ctrl + C, se cierra el socket 

int estado_salud = 1; /* 1 = OK, 0 = KO (Fuga) */

void simular_fuga(int senyal)
{
	estado_salud = 0; /* Cambiamos el estado a averiado */
	printf("\n[!] ¡Señal recibida! Fuga de agua simulada. El próximo estado será KO.\n\r");
}

int enviar_a_central(char *ip, char *puerto, char *trama) {
    int sock_c;
    struct sockaddr_in dir_central;
    char buffer[256];

    // 1. Crear socket cliente
    sock_c = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_c == -1) {
        perror("Error al abrir socket hacia la Central");
        return -1;
    }

    // 2. Configurar dirección de la Central
    dir_central.sin_family = AF_INET;
    dir_central.sin_addr.s_addr = inet_addr(ip);
    dir_central.sin_port = htons(atoi(puerto));

    // 3. Conectar con la Central
    if (connect(sock_c, (struct sockaddr *)&dir_central, sizeof(dir_central)) == -1) {
        perror("Error al conectar con la Central");
        close(sock_c);
        return -1;
    }

    // 4. Enviar la trama (ej: REGISTRO o FUGA)
    write(sock_c, trama, strlen(trama));

    // 5. Leer respuesta de la Central
    int n = read(sock_c, buffer, sizeof(buffer) - 1);
    if (n > 0) {
        buffer[n] = '\0';
        printf("[Central Response] %s\n", buffer);
    }

    // 6. Cerrar conexión
    close(sock_c);
    return 0;
}

int main (int argc, char *argv[])
{
	char *servidor_puerto;
	char mensaje[1024];
	struct sockaddr_in dir_servidor, dir_cliente;
	unsigned int long_dir_cliente;
	int s2;
	int n, enviados, recibidos;
	int proceso;
	int contador=0;
	char *puerto_engine;
	char *ip_central;
	char *puerto_central;
	char *id_estacion;

	/* Comprobar los argumentos */
	/* Comprobar los argumentos (ahora necesitamos 4 argumentos adicionales) */
	if (argc != 5)  
	{
		fprintf(stderr, "Error. Faltan argumentos\r\n");
		fprintf(stderr, "Sintaxis: %s <Puerto_Engine> <IP_Central> <Puerto_Central> <ID_Estacion>\n\r", argv[0]);
		fprintf(stderr, "Ejemplo : %s 8574 127.0.0.1 5000 WS-04\n\r", argv[0]);
		return 1;
	}
	/* Tomar los argumentos */
	puerto_engine = argv[1];
	ip_central = argv[2];
	puerto_central = argv[3];
	id_estacion = argv[4];

	servidor_puerto = puerto_engine;

	/**** Paso 1: Registrarse en WM_Central al arrancar ****/
	char trama_registro[256];
	snprintf(trama_registro, sizeof(trama_registro), "REGISTRO#%s#River Park", id_estacion);
	printf("Registrando estación %s en la Central (%s:%s)...\n\r", id_estacion, ip_central, puerto_central);
	enviar_a_central(ip_central, puerto_central, trama_registro);
	
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
	signal(SIGTSTP, simular_fuga); // Asocia Ctrl+Z a la función simular_fuga

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

            char respuesta[256];
            
			/**** Paso 5 y 6: Bucle de lectura y respuesta ****/
			while (1) 
			{
				n = sizeof(mensaje);
				recibidos = read(s2, mensaje, n); 
				if (recibidos <= 0) {
					printf("Monitor desconectado. Fin del hijo.\n\r");
					break; /* Salimos del bucle si el monitor se desconecta */
				}
				
				mensaje[recibidos] = '\0'; 
				
				if (strcmp(mensaje, "HEARTBEAT") == 0) 
				{
					// Dependiendo de si hemos pulsado Ctrl+Z o no, mandamos OK o KO
					if (estado_salud == 1) {
						sprintf(respuesta, "STATUS#OK");
					} else {
						sprintf(respuesta, "STATUS#KO#FUGA_DETECTADA");
						/* Si hay fuga, avisamos también a la Central mediante sockets */
						char trama_fuga[256];
						snprintf(trama_fuga, sizeof(trama_fuga), "FUGA#%s#Fuga detectada por sensor", id_estacion);
						enviar_a_central(ip_central, puerto_central, trama_fuga);
					}
					
					enviados = write(s2, respuesta, strlen(respuesta)); 
				}
			}

            close(s2);
            exit(0); /* El hijo termina su trabajo */
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
