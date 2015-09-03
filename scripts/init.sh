#!/bin/sh

ADDRESS="/tmp/s3nbd.sock"
DEVNAME="pommes"
BLOCKSIZE="4096"
DEVICE="/dev/nbd0"
JOURNALDEV="/dev/vgcore7/lvnbd0"

nbd-client -l -u "$ADDRESS" && \
nbd-client -N "$DEVNAME" -b "$BLOCKSIZE" -p -u "$ADDRESS" "$DEVICE" && \
mkfs.ext4 -O journal_dev "$JOURNALDEV" && \
mkfs.ext4 -J device="$JOURNALDEV" "$DEVICE" && \
fsck -fv "$DEVICE" && \
nbd-client -d "$DEVICE"
