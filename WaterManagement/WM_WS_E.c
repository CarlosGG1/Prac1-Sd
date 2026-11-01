#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>

int s; /* socket global */
int estado_salud = 1; /* 1 = OK, 0 = KO (Fuga) */
int fuga_notificada = 0; 

void finalizar (int senyal)
{
	printf("\nRecibida la señal de fin (Ctrl+C)\n\r");
	close(s);
	exit(0);
}

void simular_fuga(int senyal)
{
	estado_salud = 0; 
	printf("\n[!] ¡Señal recibida! Fuga de agua simulada (Ctrl+Z). El estado pasa a KO.\n\r");
}

int enviar_a_central(char *ip, char *puerto, char *trama) {
    int sock_c;
    struct sockaddr_in dir_central;
    char buffer[256];

    sock_c = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_c == -1) return -1;

    dir_central.sin_family = AF_INET;
    dir_central.sin_addr.s_addr = inet_addr(ip);
    dir_central.sin_port = htons(atoi(puerto));

    if (connect(sock_c, (struct sockaddr *)&dir_central, sizeof(dir_central)) == -1) {
        close(sock_c);
        return -1;
    }

    write(sock_c, trama, strlen(trama));
    int n = read(sock_c, buffer, sizeof(buffer) - 1);
    if (n > 0) {
        buffer[n] = '\0';
        printf("[Central Response] %s\n", buffer);
    }

    close(sock_c);
    return 0;
}

int main (int argc, char *argv[])
{
	char *puerto_engine;
	char *ip_central;
	char *puerto_central;
	char *id_estacion;
	char mensaje[1024];
	struct sockaddr_in dir_servidor, dir_cliente;
	unsigned int long_dir_cliente;
	int s2;
	int n, enviados, recibidos;
	int proceso;

	/* Comprobar argumentos del Engine */
	if (argc != 5)  
	{
		fprintf(stderr, "Error. Faltan argumentos\r\n");
		fprintf(stderr, "Sintaxis: %s <Puerto_Engine> <IP_Central> <Puerto_Central> <ID_Estacion>\n\r", argv[0]);
		fprintf(stderr, "Ejemplo : %s 8574 127.0.0.1 5000 WS-04\n\r", argv[0]);
		return 1;
	}

	puerto_engine = argv[1];
	ip_central = argv[2];
	puerto_central = argv[3];
	id_estacion = argv[4];

	/* Registrar estación en la Central al arrancar */
	char trama_registro[256];
	snprintf(trama_registro, sizeof(trama_registro), "REGISTRO#%s#River Park", id_estacion);
	printf("Registrando estación %s en la Central (%s:%s)...\n\r", id_estacion, ip_central, puerto_central);
	enviar_a_central(ip_central, puerto_central, trama_registro);
	
	s = socket(AF_INET, SOCK_STREAM, 0);
	if (s == -1) {
		fprintf(stderr, "Error abriendo socket\n\r");
		return 1;
	}

	dir_servidor.sin_family = AF_INET;		
	dir_servidor.sin_port = htons(atoi(puerto_engine));
	dir_servidor.sin_addr.s_addr = INADDR_ANY;
	
	if (bind(s, (struct sockaddr *)&dir_servidor, sizeof(dir_servidor)) == -1) {
		fprintf(stderr, "Error en bind\n\r");
		close(s);
		return 1;
	}

	if (listen(s, 4) == -1) {
		fprintf(stderr, "Error en listen\n\r");
		close(s);
		return 1;
	}
	printf("Engine preparado y escuchando en el puerto %s...\n\r", puerto_engine);

	signal(SIGINT, finalizar); 
	signal(SIGTSTP, simular_fuga); /* Ctrl+Z simula la fuga */

	while (1)
	{
		long_dir_cliente = sizeof(dir_cliente);
		s2 = accept(s, (struct sockaddr *)&dir_cliente, &long_dir_cliente);
		if (s2 == -1) break;

		proceso = fork();
		if (proceso == -1) exit(1);
		if (proceso == 0) /* Hijo */
		{
			close(s);
            char respuesta[256];
            
			while (1) 
			{
				recibidos = read(s2, mensaje, sizeof(mensaje) - 1); 
				if (recibidos <= 0) break;
				
				mensaje[recibidos] = '\0';
				mensaje[strcspn(mensaje, "\r\n")] = 0; 
				
				if (strcmp(mensaje, "HEARTBEAT") == 0) 
				{
					if (estado_salud == 1) {
						snprintf(respuesta, sizeof(respuesta), "STATUS#OK");
					} else {
						snprintf(respuesta, sizeof(respuesta), "STATUS#KO#FUGA_DETECTADA");
						
						if (!fuga_notificada) {
							char trama_fuga[256];
							snprintf(trama_fuga, sizeof(trama_fuga), "FUGA#%s#Fuga detectada por sensor", id_estacion);
							enviar_a_central(ip_central, puerto_central, trama_fuga);
							fuga_notificada = 1;
						}
					}
					write(s2, respuesta, strlen(respuesta)); 
				}
				else if (strncmp(mensaje, "COMANDO#", 8) == 0) 
				{
					// Procesar comandos de control (ej: COMANDO#ABRIR)
					printf("[Engine] Comando recibido: %s\n", mensaje);
					if (strcmp(mensaje, "COMANDO#ABRIR") == 0) {
						snprintf(respuesta, sizeof(respuesta), "STATUS#OK#Valvula abierta");
					} else {
						snprintf(respuesta, sizeof(respuesta), "STATUS#OK#Comando procesado");
					}
					write(s2, respuesta, strlen(respuesta));
				}
				else {
					snprintf(respuesta, sizeof(respuesta), "STATUS#ERROR#Comando desconocido");
					write(s2, respuesta, strlen(respuesta));
				}
			}

            close(s2);
            exit(0);
        }
        else {
            close(s2);
        }
    }
	
	close(s);
	return 0;
}
