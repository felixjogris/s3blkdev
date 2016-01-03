#!/bin/sh

ADDRESS="/tmp/s3blkdevd.sock"
DEVNAME="device1"
BLOCKSIZE="4096"
DEVICE="/dev/nbd0"
JOURNALDEV="/dev/vg0/lvjournal1"

# temp file to get UUID of external ext4 journal
TMPFILE=`mktemp`
[ -z "$TMPFILE" ] && exit 1

# connect to nbd
nbd-client -N "$DEVNAME" -b "$BLOCKSIZE" -p -u "$ADDRESS" "$DEVICE" && \
# prepare external ext4 journal, preferably on an SSD
mkfs.ext4 -b 4096 -O journal_dev "$JOURNALDEV" | tee "$TMPFILE" && \
# create ext4 filesystem on nbd, place journal on external device
mkfs.ext4 -J device=UUID=`awk '{if (/UUID/) { print $3 }}' "$TMPFILE"` -E stride=2048,stripe_width=2048 "$DEVICE" && \
# file system check!
fsck -v "$DEVICE" && \
# disconnect from nbd
nbd-client -d "$DEVICE" && \
# remove temp file
rm -v "$TMPFILE"
