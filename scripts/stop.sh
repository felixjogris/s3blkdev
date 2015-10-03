#!/bin/sh

MNTPOINT="/mnt"
DEVICE="/dev/nbd0"

# unmount file system
umount "$MNTPOINT" && \
# disconnect from nbd
nbd-client -d "$DEVICE"
