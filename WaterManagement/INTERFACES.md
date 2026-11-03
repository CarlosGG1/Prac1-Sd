# Interfaces web de WaterManagement

La práctica pide un panel de la CENTRAL (Fig. 1 y 2 del enunciado). Además se han hecho otras dos interfaces
gráficas, una para el **operario** y otra para la **estación**, y el panel de la central admite **órdenes de
administración**. Las tres son páginas web y se ejecutan en contenedores Docker como el resto del sistema.

## Página única (un solo enlace)
`app_web.py` monta las tres interfaces en un único servidor y puerto (8000): `/` es un portal con pestañas
**Panel de CENTRAL | Operario | Estación** y un botón **Ver todo** (las tres a la vez). Cada interfaz sigue en su
ruta (`/central/`, `/operario/`, `/estacion/`). Imagen Docker: `wm_web` (`Dockerfile.web`).
Variable `MODULOS` (por defecto `central,operario,estacion`) para activar solo algunas, por ejemplo
`MODULOS=operario,estacion` en un PC del laboratorio.

En EC2 (sustituye al contenedor `panel`, así el enlace es `http://13.60.78.197:5000` y no hay que abrir puertos):
```bash
sudo docker rm -f panel
sudo docker run -d --name web --restart unless-stopped -p 5000:8000 \
  -v /home/ubuntu/central-data:/data --add-host=host.docker.internal:host-gateway \
  -e DB_PATH=/data/central.db -e BROKER_HOST=13.60.78.197 \
  -e CENTRAL_HOST=host.docker.internal -e CENTRAL_PORT=8080 -e ADMIN_TOKEN=clave123 \
  -e STATION_ID=WS-01 -e STATION_LOCATION="Lab 2 - Jardin" -e OPERARIO=OP-1 \
  carggil/wm_web
```

## Idea de diseño: "pieles" sobre el código en C

La lógica de la práctica (Kafka, sockets, BD, reservas atómicas, vigilante...) está **solo en los programas en C**.
Las webs no la reimplementan: ejecutan el programa en C tal cual (dentro de un pseudo-terminal, como si alguien lo
hubiera abierto en una consola), le envían las mismas líneas que escribiría una persona y **leen lo que imprime**
para dibujarlo. Es la parte `skin.py`.

```
 navegador  <--HTTP/JSON-->  app_*.py (Flask)  <--teclado/pantalla-->  programa en C  <--Kafka/sockets-->  CENTRAL
```

| Interfaz | Fichero Python | Programa(s) en C que maneja | Puerto web |
|---|---|---|---|
| Panel de CENTRAL | `app.py` | ninguno: lee `central.db` (solo lectura) y manda órdenes `ADMIN` por socket | 5000 |
| Operario | `app_operario.py` | `WM_FO` | 5100 |
| Estación | `app_estacion.py` | `WM_WS_M` (Monitor) + `WM_WS_E` (Engine) | 5200 |

### Único cambio en C
`WM_Central.c` tiene un comando nuevo en el servidor de sockets: `ADMIN#TOKEN#destino accion [segundos]`
(destino = `WS-xx` o `todas`; acción = `iniciar`, `bloquear`, `activar`, `ping`). Solo funciona si la CENTRAL se
arranca con la variable `ADMIN_TOKEN` y el token coincide. Reutiliza las mismas funciones que el menú de
administración de la CENTRAL (`tratar_peticion`, `bloquear_ws`, `activar_ws`). Sin `ADMIN_TOKEN` la CENTRAL
responde "administración remota desactivada" y todo funciona como antes.

## 1. Panel de CENTRAL (`app.py`)
- Resumen con contadores por estado y caudal total.
- Estaciones en **tarjetas** o en **mapa** agrupadas por ubicación (como la Fig. 2).
  Colores del enunciado: verde = disponible, verde parpadeando = regando, rojo = avería (KO),
  naranja = fuera de servicio, gris = desconectada.
- Panel de detalle con botones **Iniciar riego / Bloquear / Activar** para una estación y para *todas*.
- Registro de sucesos (el panel compara la BD cada segundo y escribe qué ha cambiado) y estado de CENTRAL
  (cada 30 s hace `ADMIN ... ping`).
- Sin `ADMIN_TOKEN` es de solo lectura (los botones salen desactivados).

## 2. Operario (`app_operario.py`)
- Escribe la estación y los segundos (o las elige entre las disponibles) y pulsa **Regar**.
- Muestra fase (solicitando / regando / completado / denegado), caudal, volumen, tiempo, barra de progreso,
  gráfica del caudal, mensajes de CENTRAL e historial.
- Puede lanzar un **fichero de servicios** (`ID_WS#segundos`, una línea por servicio) como `WM_FO` con `f`.
- Un contenedor = un operario (`OPERARIO=OP-1`, `OP-2`...). Varios operarios en el mismo PC: distinto
  `OPERARIO` y distinto `WEB_PORT`.

## 3. Estación (`app_estacion.py`)
- Arranca el Monitor y 2 s después el Engine y los conecta.
- Muestra conexiones (CENTRAL, Monitor, Engine, Kafka), estado, electroválvula, medidor de caudal, volumen,
  tiempo, sucesos, consola de ambos programas e historial de riegos.
- Botones = teclas del Engine: **Iniciar riego manual** (`i`), **Finalizar** (`f`), **Simular fuga** (`k`),
  **Fuga reparada** (`r`).
- Una estación por contenedor (`STATION_ID`, `STATION_LOCATION`). Varias en el mismo PC: distinto
  `STATION_ID`, `MONITOR_PORT` y `WEB_PORT`.

## Despliegue

El workflow `.github/workflows/deploy.yml` ahora construye 7 imágenes (`wm_central`, `wm_monitor`, `wm_engine`,
`wm_fo`, `wm_panel`, `wm_operario_web`, `wm_estacion_web`). Hay que hacer `git push` y esperar a que GitHub
Actions termine en verde. Los repos nuevos de Docker Hub deben quedar **Public**.

### En la nube (EC2)
Recrear CENTRAL y panel (los comandos que ya usas, añadiendo las variables nuevas y manteniendo tus `-v`):
```bash
sudo docker pull carggil/wm_central && sudo docker pull carggil/wm_panel
sudo docker rm -f central panel
# central: añade  -e ADMIN_TOKEN=<clave>
# panel (en la misma máquina que la CENTRAL):
sudo docker run -d --name panel --restart unless-stopped --network host \
  -e ADMIN_TOKEN=<clave> -e CENTRAL_HOST=127.0.0.1 -e CENTRAL_PORT=8080 \
  <tus -v de siempre> carggil/wm_panel
```
La clave es la misma en los dos. Panel: `http://13.60.78.197:5000`.

### En un PC del laboratorio
```bash
# Operario
sudo docker run -d --name op1 --network host -e BROKER_HOST=13.60.78.197 -e OPERARIO=OP-1 -e WEB_PORT=5100 carggil/wm_operario_web
# Estación
sudo docker run -d --name ws1 --network host -e BROKER_HOST=13.60.78.197 -e CENTRAL_HOST=13.60.78.197 -e CENTRAL_PORT=8080 \
  -e STATION_ID=WS-01 -e STATION_LOCATION="Lab 2 - Jardin" -e MONITOR_PORT=5000 -e WEB_PORT=5200 carggil/wm_estacion_web
```
Abrir `http://localhost:5100` (operario) y `http://localhost:5200` (estación). Segunda estación en el mismo PC:
`--name ws2 ... STATION_ID=WS-02 MONITOR_PORT=5001 WEB_PORT=5201`.
Las imágenes originales (`wm_fo`, `wm_monitor`, `wm_engine`) siguen sirviendo como antes por consola.

### Sin Docker (para probar)
```bash
sudo apt install gcc librdkafka-dev python3-flask
gcc WM_FO.c -o WM_FO -lrdkafka -pthread   # idem WM_WS_E.c (-lrdkafka -pthread) y WM_WS_M.c (-lrdkafka)
WM_BIN_DIR=. BROKER_HOST=13.60.78.197 OPERARIO=OP-1 python3 app_operario.py
```

## Qué contar si te preguntan
- "¿Dónde está la lógica?" En los programas en C; la web es una capa de presentación.
- "¿Cómo sabe la web lo que pasa?" Lee lo que imprime el programa (expresiones regulares en `al_linea`) y actualiza un estado.
- "¿Cómo manda órdenes?" Escribe la misma tecla/línea que escribiría el usuario; el panel de CENTRAL usa un socket con token.
- "¿Es seguro?" El panel no escribe en la BD ni habla con Kafka; las órdenes pasan por CENTRAL, que valida el token. Para
  producción habría que usar HTTPS y no exponer los puertos 5100/5200.

## Limitaciones
- Probado con los binarios reales sobre una Kafka simulada (sin Docker ni broker real); la construcción de las dos
  imágenes nuevas y el panel contra la Kafka de EC2 se verifican al hacer el primer despliegue.
- Las webs de operario y estación son de un solo usuario por contenedor.
