#!/bin/sh

ADDRESS="/tmp/s3blkdevd.sock"
DEVNAME="device1"
BLOCKSIZE="4096"
DEVICE="/dev/nbd0"
JOURNALDEV="/dev/vg0/lvnbdjournal1"

# connect to nbd
nbd-client -N "$DEVNAME" -b "$BLOCKSIZE" -p -u "$ADDRESS" "$DEVICE" && \
# prepare external ext4 journal, preferably on an SSD
mkfs.ext4 -O journal_dev "$JOURNALDEV" && \
# create ext4 filesystem on nbd, place journal on external device
mkfs.ext4 -J device="$JOURNALDEV" "$DEVICE" && \
# file system check!
fsck -fv "$DEVICE" && \
# disconnect from nbd
nbd-client -d "$DEVICE"
