CFLAGS=-W -Wall -O3 -pipe
LDFLAGS=-s

TARGETS=s3nbdd locktool s3nbd-sync

all:	$(TARGETS)

s3nbdd:	s3nbdd.o config.o
	$(CC) $(LDFLAGS) -o $@ $^ -lsnappy -lgnutls -lpthread -lnettle

s3nbd-sync:	s3nbd-sync.o config.o
	$(CC) $(LDFLAGS) -o $@ $^ -lsnappy -lgnutls -lpthread -lnettle

test:	test.o config.o
	$(CC) $(LDFLAGS) -o $@ $^ -lsnappy -lgnutls -lpthread -lnettle

locktool:	locktool.o
	$(CC) $(LDFLAGS) -o $@ $^

%.o:	%.c s3nbd.h
	$(CC) $(CFLAGS) -c -o $@ $<

install:	s3nbdd s3nbd-sync s3nbd.conf.dist
	install -m 0755 s3nbdd s3nbd-sync /usr/local/sbin/
	install -m 0644 s3nbd.conf.dis /usr/local/etc/

clean:	; -rm *.o $(TARGETS) test

.PHONY:	all install clean
