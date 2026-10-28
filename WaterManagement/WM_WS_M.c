#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

int
main (int argc, char *argv[])
{
	char *servidor_ip;
	char *servidor_puerto;
	char trama[256];
    char respuesta[256];
	struct sockaddr_in direccion;
	int s;
	int n, enviados, recibidos;

	/* Comprobar los argumentos */
	if (argc !=  4)  //el programa pide si o si 3 argumentos: La IP, el Puerto, el Mensaje a enviar
	{
		fprintf(stderr, "Error. Debe indicar la direccion del servidor (IP y Puerto) y el mensaje a enviar\r\n");   //REGISTRO#<ID_ESTACION>#<UBICACION>
        fprintf(stderr, "Sintaxis: %s <IP_Servidor> <Puerto> <REGISTRO#ID#Ubicacion>\n\r", argv[0]);
        fprintf(stderr, "Ejemplo : %s 127.0.0.1 5000 REGISTRO#WS-04#River Park\n\r", argv[0]);
		return 1;
	}

	/* Tomar los argumentos */		
	servidor_ip = argv[1];
	servidor_puerto = argv[2];

    snprintf(trama, sizeof(trama), "%s", argv[3]);

	/**** Paso 1: Abrir el socket ****/

	s = socket(AF_INET, SOCK_STREAM, 0); /* creo el socket */
						//Hace exactamente lo mismo que el servidor
					//se crea el socket (un extremo de la comunicación). 
					//AF_INET indica que se usará las direcciones IPv4
					//SOCK_STREAM indica que sera una conexión TCP
	if (s == -1)
	{
		fprintf(stderr, "Error. No se puede abrir el socket\n\r");
		return 1;
	}
	printf("Socket abierto\n\r");

	/**** Paso 2: Conectar al servidor ****/		

	/* Cargar la direcci�n */
	direccion.sin_family = AF_INET; /* socket familia INET */
	direccion.sin_addr.s_addr = inet_addr(servidor_ip); //inet_addr convierte la Ip de formato a texto al binario que entiende la red
	direccion.sin_port = htons(atoi(servidor_puerto)); //Transforma el puerto de texto a numero entero y a formato de red
	
	if (connect(s, (struct sockaddr *)&direccion, 	sizeof (direccion)) == -1)
	// connect llama al servidor (ip + puerto) para establecer la conexión
	{
		fprintf(stderr, "Error. No se puede conectar al servidor\n\r");
		close(s);
		return 1;
	}
	printf("Conexi�n establecida\n\r");

	/**** Paso 3 y 4: Bucle de Heartbeat (Ping-Pong) ****/
	printf("Iniciando monitorización continua (Heartbeat)...\n\r");

	while (1)
	{
		// 1. Preparamos el latido
		snprintf(trama, sizeof(trama), "HEARTBEAT");
		n = strlen(trama);
		enviados = write(s, trama, n);
		
		if (enviados == -1 || enviados < n) {
			fprintf(stderr, "Error enviando el latido. Conexión perdida.\n\r");
			break;
		}

		// 2. Esperamos la respuesta
		n = sizeof(respuesta) - 1;
		recibidos = read(s, respuesta, n);
		
		if (recibidos <= 0) {
			fprintf(stderr, "El Engine se ha desconectado.\n\r");
			break;
		}
		
		respuesta[recibidos] = '\0';
		printf("[Monitor] Estado recibido: %s\n", respuesta);

		// 3. Comprobar si hay avería
		if (strstr(respuesta, "KO") != NULL) {
			printf("[!] ALERTA: Fuga detectada en la estación. Avisando a CENTRAL...\n");
			// TODO: Aquí enviaréis el mensaje de avería a WM_Central mediante otro socket
		}

		// 4. Esperar 1 segundo antes de volver a preguntar
		sleep(1); 
	}

	/**** Paso 5: Cerrar el socket ****/
	close(s);
	printf("Socket cerrado. Comunicación finalizada\n\r");

	return 0;
}
