CFLAGS=-W -Wall -O3 -pipe
LDFLAGS=-s

TARGETS=s3nbd locktool syncer

all:	$(TARGETS)

s3nbd:	s3nbd.o config.o
	$(CC) $(LDFLAGS) -o $@ $^ -lsnappy -lgnutls -lpthread -lnettle

syncer:	syncer.o config.o
	$(CC) $(LDFLAGS) -o $@ $^ -lsnappy -lgnutls -lpthread -lnettle

test:	test.o config.o
	$(CC) $(LDFLAGS) -o $@ $^ -lsnappy -lgnutls -lpthread -lnettle

locktool:	locktool.o
	$(CC) $(LDFLAGS) -o $@ $^

%.o:	%.c s3nbd.h
	$(CC) $(CFLAGS) -c -o $@ $<

clean:	; -rm *.o $(TARGETS) test

.PHONY:	all clean
