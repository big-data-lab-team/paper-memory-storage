#!/usr/bin/env python

import os, json
from matplotlib import pyplot as plt 
from statistics import mean, stdev

conditions_96_jobs = ["in-mem-1", "pmem-1", "disk-1", "isilon-1"]
conditions_25_jobs = ["in-mem-4", "pmem-4", "disk-4", "isilon-4"]
condition_ticks = ["tmpfs", "Optane", "disk", "Isilon"]
width = 0.35

def get_makespans(conditions):
    mean_makespans = []
    std_makespans = []

    def get_reps(n_cpus):
        start = '1'
        ids = range(1, 4)
        run_mksp = []

        if n_cpus == 25:
            start = '4'
            ids = range(4, 7)

        for i in ids:
            with open(os.path.join(cond.replace(start, str(i)),"makespan_seconds")) as f:
                m = int(f.read())
            run_mksp.append(m)

        return run_mksp

    for cond in conditions:
        run_mksp = []
        if '1' in cond:
            run_mksp = get_reps(96) 
        elif '4' in cond:
            run_mksp = get_reps(25)

        mean_makespans.append(mean(run_mksp))
        std_makespans.append(stdev(run_mksp))

    return mean_makespans, std_makespans 

mksp_96 = get_makespans(conditions_96_jobs)
plt.ylabel("Makespan (s)")
plt.ylim((0,5000))
plt.bar([1, 2, 3, 4], mksp_96[0], tick_label=condition_ticks, width=width, yerr=mksp_96[1], alpha=0.4, color='gray')
plt.savefig("96cores.pdf")

plt.clf()

mksp_25 = get_makespans(conditions_25_jobs)
plt.ylabel("Makespan (s)")
plt.ylim((0,5000))
plt.bar([1, 2, 3, 4], mksp_25[0], tick_label=condition_ticks, width=width, yerr=mksp_25[1], alpha=0.4, color='gray')
plt.savefig("25cores.pdf")

def time_to_secs(t):
    s = t.split('m')
    mins = int(s[0])
    secs = float(s[1].replace('s',''))
    return secs + mins*60

def get_cpu_io_times(conditions):
    mean_cpus = []
    std_cpus = []
    mean_ios = []
    std_ios = []


    def get_reps(n_cpus):
        start = '1'
        ids = range(1, 4)
        run_cpus = []
        run_ios = []

        if n_cpus == 25:
            start = '4'
            ids = range(4, 7)

        for i in ids:
            with open(os.path.join(cond.replace(start, str(i)), 'tasks.json')) as f:
                tasks = json.load(f)
            cpu = 0
            io = 0
            for t in tasks['tasks']:
                if t['Exit code'] != "0":
                    raise("Task {} failed!".format(t['Name'])) 
                c = time_to_secs(t['user time']) + time_to_secs(t['system time'])
                r = time_to_secs(t['real time'])
                cpu += c
                io += (r - c)
            run_cpus.append(cpu)
            run_ios.append(io)

        return run_cpus, run_ios


    for cond in conditions:
        if '1' in cond:
            run_cpus, run_ios = get_reps(96)
        elif '4' in cond:
            run_cpus, run_ios = get_reps(25)

        mean_cpus.append(mean(run_cpus))
        std_cpus.append(stdev(run_cpus))
        mean_ios.append(mean(run_ios))
        std_ios.append(stdev(run_ios))

    return mean_cpus, std_cpus, mean_ios, std_ios

plt.clf()
res_96 = get_cpu_io_times(conditions_96_jobs)
plt.ylabel("Total time (s)")
plt.ylim(0, 400000)
p1 = plt.bar([1, 2, 3, 4], res_96[2], tick_label=condition_ticks, width=width,
        yerr=res_96[3], alpha=0.4, color='red', hatch='+++', edgecolor='green')
p2 = plt.bar([1, 2, 3, 4], res_96[0], bottom=res_96[2], width=width,
        yerr=res_96[1], alpha=0.4, color='blue')

plt.legend((p1[0], p2[0]), ('I/O', 'CPU'))
plt.savefig('96cores-sum.pdf')

plt.clf()

plt.ylabel("Total time (s)")
plt.ylim(0, 400000)
res_25 = get_cpu_io_times(conditions_25_jobs)

p1 = plt.bar([1, 2, 3, 4], res_25[2], tick_label=condition_ticks, width=width,
        yerr=res_25[3], alpha=0.4, color='red', hatch='+++', edgecolor='green')
p2 = plt.bar([1, 2, 3, 4], res_25[0], bottom=res_25[2], width=width, yerr=res_25[1],
        alpha=0.4, color='blue')
plt.legend((p1[0], p2[0]), ('I/O', 'CPU'))
plt.savefig('25cores-sum.pdf')
