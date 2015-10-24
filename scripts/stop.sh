#!/bin/sh

DEVICE="/dev/nbd0"
MNTPOINT="/mnt"

# unmount file system
umount "$MNTPOINT" && \
# disconnect from nbd
nbd-client -d "$DEVICE"
