#!/usr/bin/env python3

import sys
import pandas as pd
import matplotlib.pyplot as plt
from glob import glob
from os import path as op
import numpy as np


def makespan(df, task="task_duration"):

    labels = ['tmpfs', 'Optane', 'local disk', 'Isilon']
    ind = np.arange(len(labels))
    ind_mem = np.delete(ind, 1)
    dft = df[df["Task"] == "task_duration"].groupby(["filename"])

    df_end = dft.End.max()
    df_start = dft.Start.min()
    df_td = pd.concat([df_start, df_end], axis=1)
    df_td['makespan'] = df_td.End - df_td.Start
    df_td['disk'] = df_td.index.map(lambda x: x.split('_')[0].split('-')[-1])
    
    s_mean = df_td.rename(columns={'makespan': 'mean'}).set_index('disk').groupby(level=0).mean()
    s_std = df_td.rename(columns={'makespan': 'std'}).set_index('disk').groupby(level=0).std()

    df_res = pd.concat([s_mean['mean'], s_std['std']], axis=1)


    df_res_mem = df_res[df_res.index.map(lambda x: x in ['tmpfs', 'local', 'isilon'])]
    df_res_mem = df_res_mem.sort_index(ascending=False)

    df_res_ad = df_res[df_res.index.map(lambda x: 'AD' in x)]
    df_res_ad = df_res_ad.sort_index(ascending=False)
    df_res_ad.rename(index={'tmpfsAD':'tmpfs', 'optaneAD': 'optane', 'localAD': 'local', 'isilonAD':'isilon'}, inplace=True)

    print(df_res_mem)
    print(df_res_ad)

    width = 0.35

    fig, ax = plt.subplots()
    pmem_read = ax.bar(ind_mem - width/2, df_res_mem['mean'], width, alpha=0.4, color='b', yerr=df_res_mem['std'], label="Memory mode")
    pad_read = ax.bar(ind + width/2, df_res_ad['mean'], width, alpha=0.4, color='orange', yerr=df_res_ad['std'], label="App Direct mode")

    #plt.ylim(0, 13000)
    ax.set_ylabel('Mean makespan (s)')
    ax.set_xlabel('Storage type')
    ax.set_xticks(ind)
    ax.set_xticklabels(labels)
    plt.legend()
    plt.savefig('makespan-{}.pdf'.format(sys.argv[2]))
    

print(sys.argv[1])
all_files = glob(op.abspath(sys.argv[1]))
disks = ['tmpfs', 'tmpfsAD', 'optaneAD', 'local', 'localAD', 'isilon', 'isilonAD']
all_files.sort(key=lambda x: disks.index(op.basename(x).split('_')[0].split('-')[1]))

df = pd.concat((pd.read_csv(f, delim_whitespace=True, names=["Task", "Start", "End", "File", "ThreadId"]) 
                  .assign(filename='{0}-{1}'.format(i, op.basename(f)),
                          disk=op.basename(f).split('_')[0].split('-')[-1]) 
                  for i, f in enumerate(all_files)))

df["Duration"] = df.End - df.Start
makespan(df)
#makespan(df, "read_file")
