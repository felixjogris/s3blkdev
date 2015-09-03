#!/bin/sh

ADDRESS="/tmp/s3nbd.sock"
DEVNAME="pommes"
BLOCKSIZE="4096"
DEVICE="/dev/nbd0"
MNTPOINT="/mnt"

nbd-client -l -u "$ADDRESS" && \
nbd-client -N "$DEVNAME" -b "$BLOCKSIZE" -p -u "$ADDRESS" "$DEVICE" && \
fsck -fv "$DEVICE" && \
resize2fs -Fp "$DEVICE" && \
mount -t ext4 -o journal_async_commit "$DEVICE" "$MNTPOINT"
