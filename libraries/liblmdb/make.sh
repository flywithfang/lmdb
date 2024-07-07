gcc -DMDB_DEBUG=10 -Werror -c -g mdb.c midl.c;g++ -DMDB_DEBUG -g -Werror -c btest.cpp -I .;g++ mdb.o midl.o btest.o -o btest

#gcc -Werror -c  mdb.c midl.c;g++  -Werror -c btest.cpp -I .;g++ mdb.o midl.o btest.o -o btest
