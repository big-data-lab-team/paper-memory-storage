#!/usr/bin/env python

import os, json
from matplotlib import pyplot as plt 

makespans = []
for cond in ["in-mem-1", "nvme-1", "pmem-1", "disk-1"]:
    with open(os.path.join(cond,"makespan_seconds")) as f:
        m = int(f.read())
    makespans.append(m)

plt.bar([1,2,3,4], makespans, tick_label=["tmpfs", "optane", "optaneAD", "disk"])
plt.ylabel("Makespan (s)")
plt.ylim((0,3000))
plt.savefig("96cores.pdf")

plt.clf()

makespans = []
for cond in ["in-mem-4", "nvme-4", "pmem-4", "disk-4"]:
    with open(os.path.join(cond,"makespan_seconds")) as f:
        m = int(f.read())
    makespans.append(m)

plt.bar([1,2,3,4], makespans, tick_label=["tmpfs", "optane", "optaneAD", "disk"])
plt.ylabel("Makespan (s)")
plt.ylim((0,3000))
plt.savefig("25cores.pdf")





def time_to_secs(t):
    s = t.split('m')
    mins = int(s[0])
    secs = float(s[1].replace('s',''))
    return secs + mins*60

plt.clf()
cpus = []
ios = []
for cond in ["in-mem-1", "nvme-1", "pmem-1", "disk-1"]:

    with open(os.path.join(cond, 'tasks.json')) as f:
        tasks = json.load(f)
    cpu = 0
    io = 0
    for t in tasks['tasks']:
        c = time_to_secs(t['user time']) + time_to_secs(t['system time'])
        r = time_to_secs(t['real time'])
        cpu += c
        io += (r - c)
    cpus.append(cpu)
    ios.append(io)

p1 = plt.bar([1, 2, 3, 4], ios, tick_label=["tmpfs", "optane", "optaneAD", "disk"])
p2 = plt.bar([1, 2, 3, 4], cpus, bottom=ios)
plt.ylim(0, 130000)
plt.ylabel("Total time (s)")
plt.legend((p1[0], p2[0]), ('CPU', 'I/O'))
plt.savefig('96cores-sum.pdf')

plt.clf()
cpus = []
ios = []
for cond in ["in-mem-4", "nvme-4", "pmem-4", "disk-4"]:

    with open(os.path.join(cond, 'tasks.json')) as f:
        tasks = json.load(f)
    cpu = 0
    io = 0
    for t in tasks['tasks']:
        c = time_to_secs(t['user time']) + time_to_secs(t['system time'])
        r = time_to_secs(t['real time'])
        cpu += c
        io += (r - c)
    cpus.append(cpu)
    ios.append(io)

p1 = plt.bar([1, 2, 3, 4], ios, tick_label=["tmpfs", "optane", "optaneAD", "disk"])
p2 = plt.bar([1, 2, 3, 4], cpus, bottom=ios)
plt.ylim(0, 130000)
plt.ylabel("Total time (s)")
plt.legend((p1[0], p2[0]), ('CPU', 'I/O'))
plt.savefig('25cores-sum.pdf')