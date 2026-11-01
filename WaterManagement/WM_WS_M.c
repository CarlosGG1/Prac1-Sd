#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

int main (int argc, char *argv[])
{
	char *servidor_ip;
	char *servidor_puerto;
	char trama[256];
    char respuesta[256];
	struct sockaddr_in direccion;
	int s;
	int n, enviados, recibidos;

	if (argc != 3)  
	{
		fprintf(stderr, "Error. Faltan argumentos\r\n");
        fprintf(stderr, "Sintaxis: %s <IP_Engine> <Puerto_Engine>\n\r", argv[0]);
        fprintf(stderr, "Ejemplo : %s 127.0.0.1 8574\n\r", argv[0]);
		return 1;
	}

	servidor_ip = argv[1];
	servidor_puerto = argv[2];

	s = socket(AF_INET, SOCK_STREAM, 0);
	if (s == -1)
	{
		fprintf(stderr, "Error abriendo socket\n\r");
		return 1;
	}

	direccion.sin_family = AF_INET;
	direccion.sin_addr.s_addr = inet_addr(servidor_ip);
	direccion.sin_port = htons(atoi(servidor_puerto));
	
	if (connect(s, (struct sockaddr *)&direccion, sizeof(direccion)) == -1)
	{
		fprintf(stderr, "Error conectando con el Engine\n\r");
		close(s);
		return 1;
	}
	printf("Conexión establecida con el Engine. Iniciando latidos...\n\r");

	while (1)
	{
		snprintf(trama, sizeof(trama), "HEARTBEAT");
		enviados = write(s, trama, strlen(trama));
		
		if (enviados <= 0) {
			fprintf(stderr, "Conexión perdida con el Engine.\n\r");
			break;
		}

		recibidos = read(s, respuesta, sizeof(respuesta) - 1);
		if (recibidos <= 0) {
			fprintf(stderr, "El Engine ha cerrado la conexión.\n\r");
			break;
		}
		
		respuesta[recibidos] = '\0';
		printf("[Monitor] Estado recibido: %s\n", respuesta);

		if (strstr(respuesta, "KO") != NULL) {
			printf("[!] ALERTA: Fuga detectada en la estación monitorizada.\n");
		}

		sleep(1); 
	}

	close(s);
	printf("Socket cerrado. Monitorización finalizada\n\r");

	return 0;
}
