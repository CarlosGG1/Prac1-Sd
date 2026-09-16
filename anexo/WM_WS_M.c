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

	/**** Paso 3: Enviar mensaje ****/

	n = strlen(trama);
	enviados = write(s, trama, n);
	if (enviados == -1 || enviados < n)
	{
		fprintf(stderr, "Error enviando el mensaje\n\r");
		close(s);
		return 1;
	}

	printf("Mensaje enviado\n\r");

	/**** Paso 4: Recibir respuesta ****/

	n = sizeof(respuesta) - 1;
	recibidos = read(s, respuesta, n);
	//read lo que hace es que el cliente se queda esperando a que el servidor le responda
	if (recibidos == -1)
	{
		fprintf(stderr, "Error recibiendo respuesta\n\r");
		close(s);
		return 1;
	}
	respuesta[recibidos] = '\0';
	printf("Respuesta [%d bytes]: %s\n\r", recibidos, respuesta);

	/**** Paso 5: Cerrar el socket ****/
	close(s);
	printf("Socket cerrado. Comunicaci�n finalizada\n\r");

	return 0;
}



