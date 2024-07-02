#gcc -DMDB_DEBUG -Werror -c -g mdb.c midl.c
#g++ -DMDB_DEBUG -o btest -g -Werror mdb.o midl.o btest.cpp -I .

gcc  -Werror -c  mdb.c midl.c
g++  -o btest -g -Werror mdb.o midl.o btest.cpp -I .