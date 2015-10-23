#!/bin/sh

ADDRESS="/tmp/s3blkdevd.sock"
DEVNAME="device1"
BLOCKSIZE="4096"
DEVICE="/dev/nbd0"
MNTPOINT="/mnt"

# connect to nbd
nbd-client -N "$DEVNAME" -b "$BLOCKSIZE" -p -u "$ADDRESS" "$DEVICE" && \
# file system check!
fsck -fv "$DEVICE" && \
# maybe s3blkdev was resized
resize2fs -Fp "$DEVICE" && \
# mount it
mount -t ext4 -o journal_async_commit "$DEVICE" "$MNTPOINT"
