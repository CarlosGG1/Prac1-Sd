/*
 * WM_WS_M  (Monitor de la Watering Station)
 *
 * Papel en el sistema (según el enunciado):
 *   - Al arrancar se conecta a WM_Central para AUTENTICARSE / REGISTRARSE.
 *   - Después actúa de SERVIDOR de sockets: espera a que se conecte su Engine (WM_WS_E).
 *   - Cada segundo le envía un HEARTBEAT al Engine. Si el Engine responde KO,
 *     no responde, o se desconecta, avisa a CENTRAL de la avería (ALERTA).
 *   - Cuando el Engine vuelve a estar OK avisa a CENTRAL (RECUPERADA).
 *
 * Uso: WM_WS_M <Puerto_Monitor> <IP_Central> <Puerto_Central> <ID_WS> [Ubicacion]
 * Ej.: WM_WS_M 5000 127.0.0.1 8080 WS-01 "River Park"
 */
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>

// TRUCO DE RESILIENCIA: Tiempos máximos de espera
#define SEGUNDOS_TIMEOUT_ENGINE 2   /* Si la válvula tarda más de 2s en contestar, está rota */
#define SEGUNDOS_TIMEOUT_CENTRAL 3  /* Si la Central tarda más de 3s en contestar, se ha caído */

// Se declara global para que la función 'finalizar' pueda cerrarlo si pulsas Ctrl+C
int s_escucha = -1; 





/*
 * QUÉ HACE: Es el "testamento" del programa. Si alguien pulsa Ctrl+C, 
 * en vez de morir dejando el puerto ocupado en Ubuntu, lo cierra limpiamente.
 */
void finalizar(int senyal)
{
	printf("\n[Monitor] Recibida la señal de fin (Ctrl+C)\n");
	if (s_escucha != -1) close(s_escucha);
	exit(0);
}





/*
 * QUÉ HACE: Intenta abrir una conexión TCP con una máquina destino (host:puerto).
 * Si la máquina no contesta en 'timeout_seg' segundos, aborta la llamada.
 */
int conectar(const char *host, const char *puerto, int timeout_seg)
{
	struct addrinfo pistas, *resultado, *p;
	int sock = -1; // -1 significa "sin conexión"

	// 1. Prepara las pistas: Queremos conexión IPv4 (AF_INET) y por TCP (SOCK_STREAM)
	memset(&pistas, 0, sizeof(pistas));
	pistas.ai_family = AF_INET;
	pistas.ai_socktype = SOCK_STREAM;

	// 2. Traduce el nombre del host (IP o Dominio) a un formato de red válido
	if (getaddrinfo(host, puerto, &pistas, &resultado) != 0) return -1;

	// 3. Bucle: Intenta conectarse a las posibles direcciones que devuelva el paso anterior
	for (p = resultado; p != NULL; p = p->ai_next)
	{
		// Crea el "teléfono" (socket)
		sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
		if (sock == -1) continue;

		// 4. LÍNEAS DE RESILIENCIA (TIMEOUTS)
		// Configura el socket para que las funciones de leer (RCVTIMEO) y enviar (SNDTIMEO)
		// no se queden bloqueadas infinitamente si se corta el cable de red.
		struct timeval tv;
		tv.tv_sec = timeout_seg; // Usa los 2 o 3 segundos que definimos arriba
		tv.tv_usec = 0;
		setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
		setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

		// 5. Intenta hacer la llamada real
		if (connect(sock, p->ai_addr, p->ai_addrlen) == 0) break; /* ¡Llamada conectada con éxito! Rompe el bucle */
		
		// Si falla, cuelga este intento y prueba con el siguiente de la lista
		close(sock);
		sock = -1;
	}
	
	// Libera la memoria de la traducción
	freeaddrinfo(resultado);
	
	// Devuelve el teléfono ya conectado (o -1 si fracasó)
	return sock;
}





/*
 * QUÉ HACE: Abre el teléfono, manda un mensaje a la Central, 
 * escucha su respuesta y cuelga. Devuelve 0 si todo fue bien, o -1 si falló.
 */
int hablar_con_central(const char *ip, const char *puerto, const char *trama, char *respuesta, int tam)
{
    // 1. Llama a la Central usando la función del bloque anterior.
    // Si la Central tarda más de 3 segundos (SEGUNDOS_TIMEOUT_CENTRAL), falla.
    int sock = conectar(ip, puerto, SEGUNDOS_TIMEOUT_CENTRAL);
    if (sock == -1) return -1;

    // 2. Envía (write) el mensaje (trama). Si devuelve 0 o menos, es que se cortó el cable.
    if (write(sock, trama, strlen(trama)) <= 0)
    {
        close(sock); // Cuelga
        return -1;
    }

    // 3. Escucha (read) la respuesta de la Central y la guarda en la variable 'respuesta'
    int n = read(sock, respuesta, tam - 1);
    
    // 4. ¡CUELGA EL TELÉFONO!
    close(sock);
    
    // Si no leyó nada, devuelve error
    if (n <= 0) return -1;

    // Le pone el fin de línea de C para que el texto se lea bien
    respuesta[n] = '\0';
    return 0; // Éxito
}





/* 
 * QUÉ HACE: Es un atajo para construir mensajes rápidos de aviso (como ALERTA o RECUPERADA) 
 * y enviarlos a la Central sin tener que escribir todo el código anterior.
 */
void notificar_central(const char *ip, const char *puerto, const char *comando, const char *id, const char *texto)
{
    char trama[256], respuesta[256];

    // 1. Construye el mensaje con el formato que entiende la Central: COMANDO#ID#texto
    // Ejemplo: ALERTA#WS-01#Engine sin respuesta
    snprintf(trama, sizeof(trama), "%s#%s#%s", comando, id, texto);
    
    // 2. Llama a la función de arriba para enviarlo
    if (hablar_con_central(ip, puerto, trama, respuesta, sizeof(respuesta)) == 0)
    {
        // Si la Central contestó, lo imprime por pantalla
        printf("[Monitor] -> CENTRAL (%s): %s\n", comando, respuesta);
    }
    else
    {
        // Si la Central estaba caída (ej: apagaste el contenedor de Docker), avisa del error
        fprintf(stderr, "[Monitor] No se pudo avisar a CENTRAL (%s). Se perdió la notificación.\n", comando);
    }
}





int main(int argc, char *argv[])
{
    char *puerto_monitor, *ip_central, *puerto_central, *id_ws, *ubicacion;
    char trama[256], respuesta[256];
    struct sockaddr_in dir_servidor, dir_cliente;
    unsigned int long_dir_cliente;
    int s2, n;
    // 'en_averia' es un interruptor: sirve para no mandar 100 mensajes por minuto a la Central 
    // si la válvula se rompe. Solo avisará 1 vez cuando se rompa y 1 vez cuando se arregle.
    int en_averia = 0; 

    // Comprueba que al encender el programa le hemos pasado todos los datos
    if (argc != 5 && argc != 6)
    {
        fprintf(stderr, "Error. Faltan argumentos\r\n");
        fprintf(stderr, "Sintaxis: %s <Puerto_Monitor> <IP_Central> <Puerto_Central> <ID_WS> [Ubicacion]\n\r", argv[0]);
        fprintf(stderr, "Ejemplo : %s 5000 127.0.0.1 8080 WS-01 \"River Park\"\n\r", argv[0]);
        return 1;
    }

    puerto_monitor = argv[1];
    ip_central = argv[2];
    puerto_central = argv[3];
    id_ws = argv[4];
    ubicacion = (argc == 6) ? argv[5] : "Sin ubicacion"; // Si no pasas ubicación, pone texto por defecto

    setvbuf(stdout, NULL, _IOLBF, 0); // Para que los prints salgan al instante en Docker

    signal(SIGINT, finalizar); // Si haces Ctrl+C, llama a la función de cerrar limpio

    /* TRUCO PRO: IGNORAR SIGPIPE */
    /* 
     * Si la válvula (Engine) se apaga de golpe, el cable virtual (socket) se rompe.
     * En Linux, si intentas escribir (write) en un cable roto, el sistema te manda
     * una señal SIGPIPE que MATA tu programa instantáneamente.
     * Poniendo SIG_IGN (Ignore), le decimos a Linux que no nos mate, que ya gestionaremos nosotros
     * el error en el código.
     */
    signal(SIGPIPE, SIG_IGN);

	/**** Paso 1: Autenticarse / registrarse en CENTRAL ****/

    // Construye el mensaje de presentación (ej: REGISTRO#WS-01#River Park)
    snprintf(trama, sizeof(trama), "REGISTRO#%s#%s", id_ws, ubicacion);
    printf("[Monitor] Autenticando %s en CENTRAL (%s:%s)...\n", id_ws, ip_central, puerto_central);

    // Bucle infinito: Intenta conectar hasta conseguirlo
    while (1)
    {
        // Llama a la función que vimos en el bloque anterior
        if (hablar_con_central(ip_central, puerto_central, trama, respuesta, sizeof(respuesta)) == 0)
        {
            // Si la Central contesta "STATUS#OK", ¡estamos dentro!
            if (strncmp(respuesta, "STATUS#OK", 9) == 0)
            {
                printf("[Monitor] CENTRAL ha validado la estación: %s\n", respuesta);
                break; // Rompe el bucle para continuar con el programa
            }
            
            // Si la Central contesta pero dice "ERROR" (ej: formato mal hecho), nos rendimos y el programa muere.
            fprintf(stderr, "[Monitor] CENTRAL rechaza la autenticación: %s\n", respuesta);
            return 1;
        }
        
        // Si la función 'hablar_con_central' devolvió -1, es que no hay conexión. Espera 3s y repite.
        fprintf(stderr, "[Monitor] CENTRAL no disponible. Reintentando en 3 s...\n");
        sleep(3);
    }

	/**** Paso 2: Servidor de sockets para el Engine ****/
    // Ahora que la Central nos conoce, abrimos nuestra propia centralita local para la válvula.

    s_escucha = socket(AF_INET, SOCK_STREAM, 0); // Crea el socket TCP
    if (s_escucha == -1) { /* error */ return 1; }

    int opcion = 1;
    setsockopt(s_escucha, SOL_SOCKET, SO_REUSEADDR, &opcion, sizeof(opcion)); /* permite reiniciar sin esperar */

    // Configura nuestro puerto (ej: 5000)
    dir_servidor.sin_family = AF_INET;
    dir_servidor.sin_port = htons(atoi(puerto_monitor));
    dir_servidor.sin_addr.s_addr = INADDR_ANY;

    // Asocia el puerto al programa (bind)
    if (bind(s_escucha, (struct sockaddr *)&dir_servidor, sizeof(dir_servidor)) == -1)
    {
        fprintf(stderr, "Error en bind (¿puerto %s ocupado?)\n\r", puerto_monitor);
        close(s_escucha);
        return 1;
    }
    
    // Empieza a escuchar. '4' es el tamaño de la cola (aunque en teoría solo se conectará 1 válvula)
    if (listen(s_escucha, 4) == -1)
    {
        fprintf(stderr, "Error en listen\n\r");
        close(s_escucha);
        return 1;
    }

	/**** Paso 3: Esperar al Engine y monitorizarlo ****/

    /* Mientras espera al Engine, accept() se despierta cada 5 s para seguir dando señales de vida a CENTRAL.
     * Sin esto, CENTRAL creería que el Monitor ha caído (DISCONNECTED) en vez de ver la estación averiada (KO). */
    struct timeval tv_espera;
    tv_espera.tv_sec = 5;
    tv_espera.tv_usec = 0;
    setsockopt(s_escucha, SOL_SOCKET, SO_RCVTIMEO, &tv_espera, sizeof(tv_espera));
    int aviso_espera = 0;

    /* Bucle de latidos: un HEARTBEAT por segundo */
    while (1) // BUCLE EXTERIOR: espera a que se conecte el Engine (y a que vuelva si se cae)
    {
        if (!aviso_espera)
        {
            printf("[Monitor] Esperando al Engine en el puerto %s...\n", puerto_monitor);
            aviso_espera = 1;
        }
        long_dir_cliente = sizeof(dir_cliente);
        s2 = accept(s_escucha, (struct sockaddr *)&dir_cliente, &long_dir_cliente);
        if (s2 == -1)
        {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                /* Nadie se ha conectado en 5 s: el Monitor sigue vivo y lo dice (KO si el Engine se había caído) */
                notificar_central(ip_central, puerto_central, "ESTADO", id_ws, en_averia ? "KO" : "OK");
                continue;
            }
            break;
        }
        aviso_espera = 0;

        /* Si el Engine no responde en SEGUNDOS_TIMEOUT_ENGINE, read() falla y lo tratamos como avería */
        struct timeval tv;
        tv.tv_sec = SEGUNDOS_TIMEOUT_ENGINE;
        tv.tv_usec = 0;
        setsockopt(s2, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        printf("[Monitor] Engine conectado. Le asigno el ID %s.\n", id_ws);

        /* El Engine no conoce su ID: se lo da el Monitor (el enunciado lo exige así) */
        snprintf(trama, sizeof(trama), "ASIGNACION#%s", id_ws);
        write(s2, trama, strlen(trama));
        n = read(s2, respuesta, sizeof(respuesta) - 1);
        if (n <= 0)
        {
            fprintf(stderr, "[Monitor] El Engine no confirmó la asignación. Cierro la conexión.\n");
            close(s2);
            continue;
        }

        int segundos = 0;
        while (1) // BUCLE INTERNO (solo se rompe si la válvula muere)
        {
            // Manda el latido
            if (write(s2, "HEARTBEAT", 9) <= 0)
            {
                n = -1; // Fallo al escribir (cable cortado)
            }
            else
            {
                // Escucha la respuesta
                n = read(s2, respuesta, sizeof(respuesta) - 1);
            }

            // CASO A: LA VÁLVULA HA MUERTO O SE HA DESCONECTADO
            if (n <= 0)
            {
                fprintf(stderr, "[Monitor] \033[0;31mEngine sin respuesta o desconectado\033[0m\n");
                
                // Si la variable en_averia era 0 (todo iba bien), avisa a la Central
                if (!en_averia)
                {
                    notificar_central(ip_central, puerto_central, "ALERTA", id_ws, "Engine sin respuesta");
                    en_averia = 1; // Pone el interruptor en modo avería
                }
                break; /* ¡Rompe el bucle de latidos! Sale al accept() exterior para esperar que vuelva */
            }

            // CASO B: LA VÁLVULA CONTESTA
            respuesta[n] = '\0';
            printf("[Monitor] Estado recibido: %s\n", respuesta);

            // B.1: La válvula contesta pero reporta fuga (han pulsado la 'k' en el teclado)
            if (strstr(respuesta, "KO") != NULL)
            {
                if (!en_averia) // Solo avisa si no estábamos ya en avería
                {
                    printf("[Monitor] \033[0;31m[!] ALERTA: fuga detectada en %s\033[0m\n", id_ws);
                    notificar_central(ip_central, puerto_central, "ALERTA", id_ws, "Fuga detectada por sensor");
                    en_averia = 1;
                }
            }
            // B.2: La válvula contesta OK, y antes estábamos en avería (han pulsado la 'r' para reparar)
            else if (en_averia)
            {
                printf("[Monitor] \033[0;32mAvería resuelta en %s\033[0m\n", id_ws);
                notificar_central(ip_central, puerto_central, "RECUPERADA", id_ws, "Averia resuelta");
                en_averia = 0; // Volvemos a la normalidad
            }

			/*
             * EL MENSAJE DE SUPERVIVENCIA:
             * Cada 5 s le decimos a CENTRAL cómo estamos. 
             */
            segundos++;
            if (segundos % 5 == 0)
                notificar_central(ip_central, puerto_central, "ESTADO", id_ws, en_averia ? "KO" : "OK");

            sleep(1); // Duerme 1 segundo y vuelve a empezar el bucle de latidos
        }

        close(s2); // Cierra el socket roto antes de volver al accept()
    }

    close(s_escucha); // Se ejecuta si sales del bucle exterior (ej: Ctrl+C)
    printf("[Monitor] Socket cerrado. Monitorización finalizada\n\r");
    return 0;
}
