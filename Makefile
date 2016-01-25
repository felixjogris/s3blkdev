CFLAGS=-W -Wall -Wextra -march=native -O3 -pipe
LDFLAGS=-s

TARGETS=s3blkdevd locktool s3blkdev-sync

all:	$(TARGETS)

s3blkdevd:	s3blkdevd.o config.o
	$(CC) $(LDFLAGS) -o $@ $^ -lsnappy -lgnutls -lpthread -lnettle

s3blkdev-sync:	s3blkdev-sync.o config.o
	$(CC) $(LDFLAGS) -o $@ $^ -lsnappy -lgnutls -lpthread -lnettle

test:	test.o config.o
	$(CC) $(LDFLAGS) -o $@ $^ -lsnappy -lgnutls -lpthread -lnettle

locktool:	locktool.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

nbdrw:	nbdrw.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

%.o:	%.c s3blkdev.h
	$(CC) $(CFLAGS) -c -o $@ $<

install:	s3blkdevd s3blkdev-sync s3blkdev.conf.dist s3blkdev.js
	install -d -m 0755 /usr/local/etc /usr/local/sbin
	install -m 0755 s3blkdevd s3blkdev-sync s3blkdev.js /usr/local/sbin/
	install -m 0644 s3blkdev.conf.dist /usr/local/etc/
	-install -m 0644 scripts/s3blkdevd.service scripts/nbd@.service scripts/s3blkdevjs.service /lib/systemd/system/

clean:	; -rm *.o $(TARGETS) test nbdrw

.PHONY:	all install clean
