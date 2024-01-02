target:	server client

server: server.c
		gcc server.c -o server -lpthread -Wall

client: client.c
		gcc client.c -o client -Wall

clean:
		rm server client