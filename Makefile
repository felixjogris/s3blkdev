CFLAGS=-W -Wall -O3 -pipe
LDFLAGS=-s
LIBS=-lpthread

s3nbd:	s3nbd.c
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) -o $@ $<

clean:	; -rm s3nbd

.PHONY:	clean
