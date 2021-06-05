CC = mpicc #compiler
N ?= 4 #number of process
TARGET = main #target file name

all:
	$(CC) -g -Wall -o $(TARGET) final.c -pthread

run:
	mpirun -n $(N) ./$(TARGET)

clean:
	rm $(TARGET)
