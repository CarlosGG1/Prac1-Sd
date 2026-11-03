"""skin.py - "piel" web para los programas en C (WM_FO, WM_WS_M, WM_WS_E).

Idea: NO se reescribe ninguna lógica. El programa en C se ejecuta tal cual, dentro de un
pseudo-terminal (como si alguien lo hubiera abierto en una terminal), y esta librería:

  1) guarda en un buffer todo lo que el programa escribe por pantalla (con su color),
  2) le puede enviar "teclas" (las mismas líneas que escribiría una persona),
  3) avisa a la web (callback) de cada línea nueva para que interprete el estado.

Así la web enseña lo que hace el programa de verdad y lo que se defiende sigue siendo el código en C.
"""
import os
import pty
import re
import signal
import subprocess
import termios
import threading
import time
from collections import deque

ANSI_COLOR = re.compile(r"\x1b\[([0-9;]*)m")
ANSI_TODO = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
CLASES = {"0;31": "rojo", "0;32": "verde", "0;33": "amarillo", "0;34": "azul", "0;36": "cian"}


class Consola:
    """Buffer de líneas compartido (puede recibir líneas de varios procesos, en orden cronológico)."""

    def __init__(self, max_lineas=2000):
        self.lineas = deque(maxlen=max_lineas)
        self.seq = 0
        self.cerrojo = threading.Lock()

    def anadir(self, fuente, bruto, clase=None):
        """Añade una línea. 'bruto' puede llevar códigos de color ANSI: se convierten en una clase."""
        if clase is None:
            m = ANSI_COLOR.search(bruto)
            clase = CLASES.get(m.group(1), "") if m else ""
        limpio = ANSI_TODO.sub("", bruto).rstrip()
        if not limpio.strip():
            return None
        with self.cerrojo:
            self.seq += 1
            linea = {"n": self.seq, "t": time.time(), "src": fuente, "txt": limpio, "cls": clase}
            self.lineas.append(linea)
        return linea

    def desde(self, n):
        with self.cerrojo:
            return [l for l in self.lineas if l["n"] > n]

    @property
    def ultimo(self):
        with self.cerrojo:
            return self.seq


class Proceso:
    """Un programa en C ejecutándose bajo un pseudo-terminal."""

    def __init__(self, nombre, argv, consola, al_linea=None):
        self.nombre = nombre
        self.argv = argv
        self.consola = consola
        self.al_linea = al_linea  # función(linea_dict) llamada por cada línea nueva
        self.proc = None
        self.master = None
        self.codigo = None
        self.arrancado = 0.0
        self._parcial = ""

    # ------------------------------------------------------------------ ciclo de vida
    def iniciar(self):
        if self.vivo():
            return
        maestro, esclavo = pty.openpty()
        atributos = termios.tcgetattr(esclavo)
        atributos[3] &= ~termios.ECHO  # lo que enviamos no se repite en la salida
        termios.tcsetattr(esclavo, termios.TCSANOW, atributos)
        self._parcial = ""
        self.codigo = None
        self.proc = subprocess.Popen(
            self.argv, stdin=esclavo, stdout=esclavo, stderr=esclavo,
            close_fds=True, start_new_session=True,
        )
        os.close(esclavo)
        self.master = maestro
        self.arrancado = time.time()
        self._emitir("[web] Arrancado: " + " ".join(self.argv), "sistema")
        threading.Thread(target=self._leer, args=(self.proc, maestro), daemon=True).start()

    def vivo(self):
        return self.proc is not None and self.proc.poll() is None

    def detener(self):
        if self.proc is not None and self.proc.poll() is None:
            try:
                os.killpg(self.proc.pid, signal.SIGTERM)
                self.proc.wait(timeout=3)
            except Exception:
                try:
                    os.killpg(self.proc.pid, signal.SIGKILL)
                except Exception:
                    pass

    def reiniciar(self):
        self.detener()
        time.sleep(0.3)
        self.iniciar()

    # ------------------------------------------------------------------ entrada y salida
    def enviar(self, texto):
        """Escribe una línea en la 'teclado' del programa. Devuelve False si no está en marcha."""
        if not self.vivo():
            return False
        texto = texto.replace("\r", " ").replace("\n", " ").strip()
        try:
            os.write(self.master, (texto + "\n").encode("utf-8"))
        except OSError:
            return False
        self._emitir("» " + texto, "entrada")
        return True

    def _emitir(self, texto, clase=None):
        linea = self.consola.anadir(self.nombre, texto, clase)
        if linea and self.al_linea:
            try:
                self.al_linea(linea)
            except Exception as e:  # una web nunca debe tumbar el lector
                print("[skin] error interpretando la línea:", e, flush=True)

    def _leer(self, proc, maestro):
        while True:
            try:
                datos = os.read(maestro, 4096)
            except OSError:
                break
            if not datos:
                break
            self._parcial += datos.decode("utf-8", "replace").replace("\r", "")
            while "\n" in self._parcial:
                linea, self._parcial = self._parcial.split("\n", 1)
                self._emitir(linea)
        if self._parcial.strip():
            self._emitir(self._parcial)
            self._parcial = ""
        self.codigo = proc.wait()
        try:
            os.close(maestro)
        except OSError:
            pass
        self._emitir("[web] El programa ha terminado (código %s)" % self.codigo, "sistema")


def entorno(nombre, defecto):
    """Lee una variable de entorno con valor por defecto (vacío cuenta como no definida)."""
    valor = os.environ.get(nombre, "").strip()
    return valor if valor else defecto


def binario(nombre):
    """Ruta del ejecutable: WM_BIN_DIR/nombre si existe esa variable, o el nombre a secas (PATH)."""
    carpeta = os.environ.get("WM_BIN_DIR", "").strip()
    return os.path.join(carpeta, nombre) if carpeta else nombre
