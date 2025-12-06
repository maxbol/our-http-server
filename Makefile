CC=clang

our-http-server: our-http-server.c
	${CC} -o our-http-server our-http-server.c

.PHONY: test clean all
test:
clean:

all:
	our-http-server

