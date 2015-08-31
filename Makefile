CFLAGS=-W -Wall -O3 -pipe
LDFLAGS=-s
LIBS=-lpthread

all:	s3nbd lock_chunk

s3nbd:	s3nbd.c
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) -o $@ $<

lock_chunk:	lock_chunk.c
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) -o $@ $<

test:	test.c
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) -o $@ $<

clean:	; -rm s3nbd lock_chunk test

.PHONY:	all clean
