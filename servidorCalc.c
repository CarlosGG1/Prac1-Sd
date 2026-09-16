#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
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

int
main (int argc, char *argv[])
{
	char *servidor_puerto;
	char mensaje[1024];
	struct sockaddr_in dir_servidor, dir_cliente;
	unsigned int long_dir_cliente;
	int s2;
	int n, enviados, recibidos;
    int num1, num2, resultado;
	int proceso;
	int contador=0;
    char operador;
    int operacion_valida = 1;

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
            mensaje[recibidos] = '\0';

            if (sscanf(mensaje, "%d %c %d", &num1, &operador, &num2) == 3) 
            {
                switch (operador) 
                {
                    case '+':
                        resultado = num1 + num2;
                        break;
                    case '-':
                        resultado = num1 - num2;
                        break;
                    case '*':
                        resultado = num1 * num2;
                        break;
                    case '/':
                        if (num2 != 0) {
                            resultado = num1 / num2;
                        } else {
                            operacion_valida = 0; 
                        }
                        break;
                    default:
                        operacion_valida = 0; 
                        break;
                }
            } 
            else 
            {
                operacion_valida = 0; // Si el formato no coincide
            }

            char respuesta_str[100];
            if (operacion_valida) 
            {
                sprintf(respuesta_str, "El resultado es: %d", resultado);
            } 
            else 
            {
                sprintf(respuesta_str, "Error: Operación no válida o división por cero.");
            }

			/**** Paso 6: Enviar respuesta ****/

			n = strlen(respuesta_str); 
			printf("Enviar respuesta ( %d ) de [ %d bytes ]: %s\n\r", contador, n, respuesta_str);
			enviados = write(s2, respuesta_str, n); 
			if (enviados == -1 || enviados < n)
			{
				fprintf(stderr, "Error enviando la respuesta (%d)\n\r",enviados);
				close(s);
				return 1;
			}

			printf("Respuesta enviada\n\r");

			close(s2);
			exit(0); /* el hijo ya no tiene que hacer nada */
		}
		else /* soy el padre */
		{
			close(s2); /* el padre no usa esta conexi�n */
		}

	}

	/**** Paso 7: Cerrar el socket ****/
	close(s);
	printf("Socket cerrado\n\r");
	return 0;
}

