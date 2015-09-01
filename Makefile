CFLAGS=-W -Wall -O3 -pipe
LDFLAGS=-s
LIBS=-lpthread

all:	s3nbd locktool syncer

s3nbd:	s3nbd.c s3nbd.h
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) -o $@ $<

locktool:	locktool.c s3nbd.h
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

syncer:	syncer.c s3nbd.h
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

test:	test.c
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) -o $@ $<

clean:	; -rm s3nbd locktool syncer test

.PHONY:	all clean
