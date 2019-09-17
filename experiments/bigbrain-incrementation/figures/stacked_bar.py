#!/usr/bin/env python3

import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatch
from glob import glob
from os import path as op
import numpy as np


def add_disk_col(df):
    df['disk'] = df.index.map(lambda x: x.split('_')[0].split('-')[-1])


def stacked_bar(df):

    labels = ['tmpfs', 'Optane', 'local disk', 'Isilon']
    ind = np.arange(len(labels))
    ind_mem = np.delete(ind, 1)

    dfr = df[df["Task"] == "read_file"].rename(columns={'Duration': 'read_time'}).groupby(["filename"])["read_time"].sum()
    dfi = df[df["Task"] == "increment_file"].rename(columns={'Duration': 'increment_time'}).groupby(["filename"])["increment_time"].sum()
    dfw = df[df["Task"] == "write_file"].rename(columns={'Duration': 'write_time'}).groupby(["filename"])["write_time"].sum()

    df_sum = pd.concat([dfr, dfi, dfw], axis=1)
    
    add_disk_col(df_sum)

    df_mean = df_sum.set_index('disk').groupby('disk').mean()
    df_std = df_sum.set_index('disk').groupby('disk').std()
    print('\n\nMean:')
    print(df_mean)
    print('\n\nSTD:')
    print(df_std)

    def prep_mem_df(df):
        mem_labels = ['tmpfs', 'local', 'isilon']
        df = df[df.index.map(lambda x: x in mem_labels)]
        return df.sort_index(ascending=False)

    df_mean_mem = prep_mem_df(df_mean)
    df_std_mem = prep_mem_df(df_std)

    def prep_ad_df(df):
        df = df[df.index.map(lambda x: 'AD' in x)]
        df = df.sort_index(ascending=False)
        df.rename(index={'tmpfsAD':'tmpfs', 'optaneAD': 'optane', 'localAD': 'local', 'isilonAD':'isilon'}, inplace=True)
        return df

    df_mean_ad = prep_ad_df(df_mean)
    df_std_ad = prep_ad_df(df_std)

    width = 0.35

    alpha = 0.4

    figure, ax = plt.subplots()
    p_read_mem = ax.bar(ind_mem - width/2, df_mean_mem['read_time'], width, color='r', label="read", alpha=alpha, yerr=df_std_mem['read_time'], edgecolor='black')
    p_read_ad = ax.bar(ind + width/2, df_mean_ad['read_time'], width, color='r', hatch='//', alpha=alpha, yerr=df_std_ad['read_time'], edgecolor='black')
    p_inc_mem = ax.bar(ind_mem - width/2, df_mean_mem['increment_time'], width, bottom=np.array(df_mean_mem['read_time']), color='b', label="increment", alpha=alpha, yerr=df_std_mem['increment_time'], edgecolor='black')
    p_inc_ad = ax.bar(ind + width/2, df_mean_ad['increment_time'], width, bottom=np.array(df_mean_ad['read_time']), color='b', hatch='//', alpha=alpha, yerr=df_std_ad['increment_time'], edgecolor='black')
    p_write_mem = ax.bar(ind_mem - width/2, df_mean_mem['write_time'], width, 
                         bottom=np.array(df_mean_mem['read_time']) + np.array(df_mean_mem['increment_time']), color='g', label="write", alpha=alpha, yerr=df_std_mem['write_time'], edgecolor='black')
    p_write_ad = ax.bar(ind + width/2, df_mean_ad['write_time'], width, 
                         bottom=np.array(df_mean_ad['read_time']) + np.array(df_mean_ad['increment_time']), color='g', hatch='//', alpha=alpha, yerr=df_std_ad['write_time'], edgecolor='black')

    r_read = mpatch.Patch(facecolor='r', alpha=alpha, label='read')
    r_inc = mpatch.Patch(facecolor='b', alpha=alpha, label='increment')
    r_write = mpatch.Patch(facecolor='g', alpha=alpha, label='write')
    r_mem = mpatch.Patch(facecolor='gray', alpha=alpha, label='Memory mode')
    r_ad = mpatch.Patch(facecolor='gray', alpha=alpha, hatch=r'///', label='App Direct mode')

    ax.set_ylabel('Average total execution time (s)')
    ax.set_xlabel('Storage type')
    ax.set_xticks(ind)
    ax.set_xticklabels(labels)
    ax.legend(handles=[r_mem, r_ad, r_read, r_inc, r_write])

    plt.savefig('stacked-{}.pdf'.format(sys.argv[2]))
    

all_files = glob(op.abspath(sys.argv[1]))
disks = ['tmpfs', 'tmpfsAD', 'optane', 'optaneAD', 'local', 'localAD', 'isilon', 'isilonAD']
all_files.sort(key=lambda x: disks.index(op.basename(x).split('_')[0].split('-')[1]))

df = pd.concat((pd.read_csv(f, delim_whitespace=True, names=["Task", "Start", "End", "File", "ThreadId"]) \
                  .assign(filename='{0}-{1}'.format(i, op.basename(f)), disk=op.basename(f).split('_')[0].split('-')[-1])) for i, f in enumerate(all_files))

df["Duration"] = df.End - df.Start
stacked_bar(df)


