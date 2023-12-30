# Set the compiler and compiler flags
CC = gcc
CFLAGS = -g -Wall
LIBS = -lm

# Targets
EXECUTABLES = test_1 test_2
# Define the object files required for the targets
OBJECTS = test_assign2_1.o test_assign2_2.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o

# Main target to run all
run: clean all run_test_1 run_test_2

# Remove all executables, object files, temporary files, and binary files
clean:
	$(RM) $(EXECUTABLES) $(OBJECTS) *~ *.bin

all: $(EXECUTABLES)

# To create the executable file
test_1: test_assign2_1.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) $(CFLAGS) -o test_1 test_assign2_1.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o $(LIBS)

# To create the executable file
test_2: test_assign2_2.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o
	$(CC) $(CFLAGS) -o test_2 test_assign2_2.o storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o $(LIBS)

# To create the object file
test_assign2_1.o: test_assign2_1.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h
	$(CC) $(CFLAGS) -c test_assign2_1.c $(LIBS)

# To create the object file
test_assign2_2.o: test_assign2_2.c dberror.h storage_mgr.h test_helper.h buffer_mgr.h buffer_mgr_stat.h
	$(CC) $(CFLAGS) -c test_assign2_2.c $(LIBS)

# To create the object file
storage_mgr.o: storage_mgr.c storage_mgr.h
	$(CC) $(CFLAGS) -c storage_mgr.c $(LIBS)

# To create the object file
dberror.o: dberror.c dberror.h
	$(CC) $(CFLAGS) -c dberror.c $(LIBS)

# To create the object file
buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h buffer_mgr.h
	$(CC) $(CFLAGS) -c buffer_mgr_stat.c

# To create the object file
buffer_mgr.o: buffer_mgr.c buffer_mgr.h dt.h storage_mgr.h
	$(CC) $(CFLAGS) -c buffer_mgr.c

# Run the test_1
run_test_1:
	./test_1

# Run the test_2
run_test_2:
	./test_2
