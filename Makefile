CFLAGS=-W -Wall -O3 -pipe
LDFLAGS=-s

TARGETS=s3nbd locktool syncer

all:	$(TARGETS)

s3nbd:	s3nbd.o config.o
	$(CC) $(LDFLAGS) -o $@ $^ -lsnappy -lgnutls -lpthread

syncer:	syncer.o config.o
	$(CC) $(LDFLAGS) -o $@ $^ -lsnappy -lgnutls -lpthread

locktool:	locktool.o
	$(CC) $(LDFLAGS) -o $@ $^

%.o:	%.c s3nbd.h
	$(CC) $(CFLAGS) -c -o $@ $<

clean:	; -rm *.o $(TARGETS)

.PHONY:	all clean
