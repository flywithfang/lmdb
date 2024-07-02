gcc -DMDB_DEBUG -Werror -c -g mdb.c midl.c
g++ -DMDB_DEBUG -g -Werror -c btest.cpp -I .
g++ mdb.o midl.o btest.o -o btest
#ld -o btest mdb.o midl.o btest.o -lc -L/usr/lib/x86_64-linux-gnu/libstdc++.so.6 -L/usr/lib/gcc/x86_64-linux-gnu/13/libgcc_s.so /usr/lib/x86_64-linux-gnu/crt1.o /usr/lib/x86_64-linux-gnu/crti.o /usr/lib/x86_64-linux-gnu/crtn.o -dynamic-linker /lib64/ld-linux-x86-64.so.2

#gcc  -Werror -c  mdb.c midl.c
#g++  -o btest -g -Werror mdb.o midl.o btest.cpp -I .