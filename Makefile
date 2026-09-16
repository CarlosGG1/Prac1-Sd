PROGRAMAS = clienteCalc servidorCalc

CFLAGS = -Wall

todo: $(PROGRAMAS)

borrar:
	rm -f *.o $(PROGRAMAS) *~

