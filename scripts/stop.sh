#!/bin/sh

MNTPOINT="/mnt"
DEVICE="/dev/nbd0"

umount "$MNTPOINT" && \
nbd-client -d "$DEVICE"
