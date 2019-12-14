#!/bin/bash

echo 3 | sudo tee /proc/sys/vm/drop_caches

device=$1
for i in {1..5}
do
    echo 'Device:' $device
    echo 'Write:'
    dd if=/dev/zero of=$device bs=1M count=1024 conv=fdatasync,notrunc status=progress

    echo 3 | sudo tee /proc/sys/vm/drop_caches
    echo 'Read:'
    dd if=$device of=/dev/null bs=1M count=1024 status=progress

    echo 'Buffered read:'
    dd if=$device of=/dev/null bs=1M count=1024 status=progress

    echo 3 | sudo tee /proc/sys/vm/drop_caches
done
