gcc -c mdb.c midl.c
g++ -o btest mdb.o midl.o btest.cpp -I .
