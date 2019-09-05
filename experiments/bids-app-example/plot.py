#!/usr/bin/env python

import os, json
from matplotlib import pyplot as plt 

conditions_96_jobs = ["in-mem-1", "nvme-1", "pmem-1", "disk-1", "isilon-1"]
conditions_25_jobs = ["in-mem-4", "nvme-4", "pmem-4", "disk-4", "isilon-4"]
condition_ticks = ["tmpfs", "optane", "optaneAD", "disk", "isilon"]

def get_makespans(conditions):
    makespans = []
    for cond in conditions:
        with open(os.path.join(cond,"makespan_seconds")) as f:
            m = int(f.read())
        makespans.append(m)
    return makespans 


plt.ylabel("Makespan (s)")
plt.ylim((0,5000))
plt.bar([1, 2, 3, 4, 5], get_makespans(conditions_96_jobs), tick_label=condition_ticks)
plt.savefig("96cores.pdf")

plt.clf()

plt.ylabel("Makespan (s)")
plt.ylim((0,5000))
plt.bar([1, 2, 3, 4, 5], get_makespans(conditions_25_jobs), tick_label=condition_ticks)
plt.savefig("25cores.pdf")

def time_to_secs(t):
    s = t.split('m')
    mins = int(s[0])
    secs = float(s[1].replace('s',''))
    return secs + mins*60

def get_cpu_io_times(conditions):
    cpus = []
    ios = []
    for cond in conditions:
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
    return cpus, ios

plt.clf()

plt.ylabel("Total time (s)")
plt.ylim(0, 400000)
cpus, ios = get_cpu_io_times(conditions_96_jobs)
print(cpus, ios)
p1 = plt.bar([1, 2, 3, 4, 5], ios, tick_label=condition_ticks)
p2 = plt.bar([1, 2, 3, 4, 5], cpus, bottom=ios)

plt.legend((p1[0], p2[0]), ('I/O', 'CPU'))
plt.savefig('96cores-sum.pdf')

plt.clf()

plt.ylabel("Total time (s)")
plt.ylim(0, 400000)
cpus, ios = get_cpu_io_times(conditions_25_jobs)

p1 = plt.bar([1, 2, 3, 4, 5], ios, tick_label=condition_ticks)
p2 = plt.bar([1, 2, 3, 4, 5], cpus, bottom=ios)
plt.legend((p1[0], p2[0]), ('I/O', 'CPU'))
plt.savefig('25cores-sum.pdf')