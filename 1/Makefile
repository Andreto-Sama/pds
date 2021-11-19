CC=gcc
CFLAGS = -O3

default: 

serial:
	gcc -O3 serial.c mmio.c -o serial.out


openCilk:
	 OpenCilk-10.0.1-Linux/bin/clang -O3 -fopencilk opencilk.c mmio.c -o opencilk.out

openMP:
	gcc -O3 -fopenmp mmio.c openMP.c -o openMP.out
	
Pthread:
	gcc -O3 -pthread pthreads.c mmio.c -o pthreads.out


