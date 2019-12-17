#!/usr/bin/env python3

import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from glob import glob
from os import path as op
import numpy as np


ad_bench = '../../system_conf/ad_diskbench.out'
mem_bench = '../../system_conf/mem_diskbench.out'
data_size = 614 * 125
total_1000 = 617 * 1000
bb20 = '20bb' in sys.argv[1]
disks = ["tmpfs", "tmpfsAD", "optaneAD", "local", "localAD", "isilon", "isilonAD"]

if bb20:
    data_size = total_1000


def configure_df(df):
    df_end = df.End.apply(lambda x: x.max())
    df_start = df.Start.apply(lambda x: x.min())

    #print(df.End.apply(lambda x: x.max()))

    df_data = pd.concat([df_start, df_end], axis=1)
    print(df_data.head(5))
    df_data["makespan"] = df_data.End - df_data.Start
    df_data["disk"] = df_data.index.map(
        lambda x: x.split("_")[0].split("-")[-1]
    )
    s_mean = (
        df_data.rename(columns={"makespan": "mean"})
        .set_index("disk")
        .groupby(level=0)
        .mean()
    )
    s_std = (
        df_data.rename(columns={"makespan": "std"})
        .set_index("disk")
        .groupby(level=0)
        .std()
    )

    df_res = pd.concat([s_mean["mean"], s_std["std"]], axis=1)

    return df_res



def makespan(df, task="task_duration", spark=False):

    labels = ["DRAM", "Optane DCPMM", "local SSD*", "Isilon"]
    if not spark:
        dfp = df[df["Task"] == "task_duration"].groupby(["filename"])
    else:
        dfp = df[(~df["filename"].str.contains('spark')) & (df["Task"] == "task_duration") & ~(df["filename"].str.contains('em'))].groupby(["filename"])
        dfs = df[df["filename"].str.contains('spark') & ~(df["filename"].str.contains('em'))].groupby(["filename"])

    ind = np.arange(len(labels))

    df_pres = configure_df(dfp)

    if not spark:
        df_pres_mem = df_pres[
            df_pres.index.map(lambda x: x in ["tmpfs", "local", "isilon"])
        ]
        df_pres_mem = df_pres_mem.sort_index(ascending=False)
        df_pres_ad = df_pres[df_pres.index.map(lambda x: "AD" in x)]
        df_pres_ad = df_pres_ad.sort_index(ascending=False)
        #df_pres_ad.rename(
        #    index={
        #        "tmpfsAD-real": "tmpfs",
        #        "optaneAD-real": "optane",
        #        "localAD-real": "local",
        #        "isilonAD-real": "isilon",
        #    },
        #    inplace=True,
        #)
        print(df_pres)
    else:
        df_sres = configure_df(dfs)

        print(df_pres)
        print(df_sres)


        df_pres_mem = df_pres[
            df_pres.index.map(lambda x: 'AD' not in x)
        ]
        df_pres_mem = df_pres_mem.sort_index(ascending=False)

        df_sres_mem = df_sres[
            df_sres.index.map(lambda x: 'AD' not in x)
        ]
        df_sres_mem = df_sres_mem.sort_index(ascending=False)

        df_pres_ad = df_pres[
            df_pres.index.map(lambda x: 'AD' not in x)
        ]
        df_pres_ad = df_pres_ad.sort_index(ascending=False)
        df_pres_ad.rename(index={'tmpfsAD':'tmpfs', 'optaneAD': 'optane', 'localAD': 'local', 'isilonAD':'isilon'}, inplace=True)

        df_sres_ad = df_sres[df_sres.index.map(lambda x: 'AD' in x)]
        df_sres_ad = df_sres_ad.sort_index(ascending=False)
        df_sres_ad.rename(index={'tmpfsAD':'tmpfs', 'optaneAD': 'optane', 'localAD': 'local', 'isilonAD':'isilon'}, inplace=True)


    df_adb = pd.read_csv(ad_bench)
    convert_gb = lambda x: float(x.strip(' GB/s')) * 1024 if 'GB/s' in x else float(x.strip(' MB/s'))
    df_adb['Write'] = df_adb['Write'].apply(convert_gb)
    df_adb['Read'] = df_adb['Read'].apply(convert_gb)

    if bb20:
        df_adb = df_adb[df_adb['Device'].str.contains('isilon') | df_adb['Device'].str.contains('optane')]

    df_adb['WriteTime'] = data_size / df_adb['Write']
    df_adb['ReadTime'] = data_size / df_adb['Read']
    df_adb_mean = df_adb.groupby(['Device']).mean().sort_index(ascending=False)
    df_adb_std = df_adb.groupby(['Device']).std().sort_index(ascending=False)

    df_memb = pd.read_csv(mem_bench)
    convert_gb = lambda x: float(x.strip(' GB/s')) * 1024 if 'GB/s' in x else float(x.strip(' MB/s'))
    df_memb['Write'] = df_memb['Write'].apply(convert_gb)
    df_memb['Read'] = df_memb['Read'].apply(convert_gb)

    if bb20:
        df_memb = df_memb[df_memb['Device'].str.contains('isilon') | df_memb['Device'].str.contains('tmpfs')]

    df_memb['WriteTime'] = data_size / df_memb['Write']
    df_memb['ReadTime'] = data_size / df_memb['Read']
    df_memb_mean = df_memb.groupby(['Device']).mean().sort_index(ascending=False)
    df_memb_std = df_memb.groupby(['Device']).std().sort_index(ascending=False)

    if not bb20:
        df_memb_mean.at['local', 'WriteTime'] = df_memb_mean.at['tmpfs', 'WriteTime']
        df_memb_std.at['local', 'WriteTime'] = df_memb_std.at['tmpfs', 'WriteTime']
        df_adb_mean.at['localAD', 'WriteTime'] = df_adb_mean.at['tmpfsAD', 'WriteTime']
        df_adb_std.at['localAD', 'WriteTime'] = df_adb_std.at['tmpfsAD', 'WriteTime']

    if bb20:
        ind = np.asarray([0, 1])
        ind_mem = ind
        labels = ['Optane DCPMM', 'Isilon']

    print(df_memb_mean)
    print(df_memb_std)
    print(df_adb_mean)
    print(df_adb_std)
    
    width = 0.15

    fig, ax = plt.subplots()
    if not bb20:
        ind_mem = np.delete(ind, 0)
    if spark:
        df_pres_mem = df_sres_mem
        df_pres_ad = df_sres_ad


    df_pres_mem['Mode'] = 'Memory mode'
    df_pres_ad['Mode'] = 'App Direct mode'

    all_data = pd.concat([df_pres_mem, df_pres_ad], axis=0)
    
    all_data['processes'] = 25 if '25cpu' in sys.argv[1] else 96
    all_data['image'] = '40bb' if '40bb' in sys.argv[1] else '20bb'
    all_data['spark'] = spark

    all_data.rename(
            index={
                "tmpfsAD": "DRAM",
                "tmpfs": "Optane"
            },
            inplace=True,
        )

    csv_file = "../makespan.csv"
    all_data.to_csv('../makespan.csv', header=(not op.isfile(csv_file)), mode='a+')
    pmem_m = ax.bar(
        ind_mem - 3*width / 2,
        df_pres_mem["mean"],
        width,
        alpha=0.4,
        color="gray",
        yerr=df_pres_mem["std"],
        label="Memory mode",
    )
    pad_m = ax.bar(
        ind + width / 2,
        df_pres_ad["mean"],
        width,
        alpha=0.4,
        color="gray",
        hatch="//",
        yerr=df_pres_ad["std"],
        label="App Direct mode",
    )
    pmem_read = ax.bar(
        ind_mem - width / 2,
        df_memb_mean["ReadTime"],
        width,
        alpha=0.4,
        color="red",
        label="Anticipated Read Duration",
        yerr=df_memb_std["ReadTime"],
    )
    pmem_write = ax.bar(
        ind_mem - width / 2,
        df_memb_mean["WriteTime"],
        width,
        alpha=0.4,
        color="blue",
        bottom=df_memb_mean["ReadTime"],
        label="Anticipated Write Duration",
        yerr=df_memb_std["WriteTime"],
    )
    pad_read = ax.bar(
        ind + 3*width / 2,
        df_adb_mean["ReadTime"],
        width,
        alpha=0.4,
        color="red",
        hatch="//",
        yerr=df_adb_std["ReadTime"],
    )
    pad_write = ax.bar(
        ind + 3*width / 2,
        df_adb_mean["WriteTime"],
        width,
        alpha=0.4,
        color="blue",
        hatch="//",
        bottom=df_adb_mean["ReadTime"],
        yerr=df_adb_std["WriteTime"],
    )

    if bb20:
        plt.ylim(0, 12000)

    ax.set_ylabel("Mean makespan (s)")
    ax.set_xlabel("Storage type")
    ax.set_xticks(ind)
    ax.set_xticklabels(labels)
    plt.legend()
    plt.savefig("makespan-real-{}.pdf".format(sys.argv[2]))
    

def get_dbench():

    df_adb = pd.read_csv(ad_bench)
    convert_gb = lambda x: float(x.strip(' GB/s')) * 1024 if 'GB/s' in x else float(x.strip(' MB/s'))
    df_adb['Write'] = df_adb['Write'].apply(convert_gb)
    df_adb['Read'] = df_adb['Read'].apply(convert_gb)

    if bb20:
        df_adb = df_adb[df_adb['Device'].str.contains('isilon') | df_adb['Device'].str.contains('optane') | df_adb['Device'].str.contains('tmpfs')]

    df_adb['WriteTime'] = data_size / df_adb['Write']
    df_adb['ReadTime'] = data_size / df_adb['Read']
    df_adb_mean = df_adb.groupby(['Device']).mean().sort_index(ascending=False)
    df_adb_std = df_adb.groupby(['Device']).std().sort_index(ascending=False)

    df_memb = pd.read_csv(mem_bench)
    convert_gb = lambda x: float(x.strip(' GB/s')) * 1024 if 'GB/s' in x else float(x.strip(' MB/s'))
    df_memb['Write'] = df_memb['Write'].apply(convert_gb)
    df_memb['Read'] = df_memb['Read'].apply(convert_gb)

    if bb20:
        df_memb = df_memb[df_memb['Device'].str.contains('isilon') | df_memb['Device'].str.contains('tmpfs')]

    df_memb['WriteTime'] = data_size / df_memb['Write']
    df_memb['ReadTime'] = data_size / df_memb['Read']
    df_memb_mean = df_memb.groupby(['Device']).mean().sort_index(ascending=False)
    df_memb_std = df_memb.groupby(['Device']).std().sort_index(ascending=False)

    if not bb20:
        df_memb_mean.at['local', 'WriteTime'] = df_memb_mean.at['tmpfs', 'WriteTime']
        df_memb_std.at['local', 'WriteTime'] = df_memb_std.at['tmpfs', 'WriteTime']
        df_adb_mean.at['localAD', 'WriteTime'] = df_adb_mean.at['tmpfsAD', 'WriteTime']
        df_adb_std.at['localAD', 'WriteTime'] = df_adb_std.at['tmpfsAD', 'WriteTime']

    return df_memb_mean, df_adb_mean


def trendline_tasks(df):
   
    df_memb_mean, df_adb_mean = get_dbench()

    def fill_start(grp, val):
        grp['Start'] = val
        return grp

    if bb20:
        size = 617
    else:
        size = 614

    df_tasks = df[~df['Task'].str.contains('task')]# & df['Task'].str.contains('write')]

    df_durations = df[df['Task'].str.contains('task') | df['Task'].str.contains('driver')].groupby(['filename'])
    df_start = df_durations['Start'].min()
    df_end = df_durations['End'].max()

    #print(df_start)
    #print(df_end)

    df_durations = (df_end - df_start).sort_index()
    

    def count_concurrent(df):

        df_over = df.rename(columns={'Start':'Time'})
        df_over['Start'] = 1 
        df1 = pd.DataFrame(df_over[['filename', 'File', 'Task', 'Duration', 'Time','Start']])
        df_over['Start'] = -1
        df2 = pd.DataFrame(df_over[['filename','File', 'Task', 'Duration', 'End', 'Start']]).rename(columns={'End':'Time'})
        df_tasks = pd.concat([df1, df2], axis=0).groupby(['filename']).apply(lambda x: x.sort_values(by=['Time', 'Start']))
        df_tasks['R'] = df_tasks['Start'].transform(pd.Series.cumsum)
        df_tasks['T1'] = df_tasks['Time'].shift(periods=-1)
        df_tasks['Time0'] = df_tasks['Time'].shift(periods=1)
        df_tasks['Time0'].fillna(df_tasks['Time'], inplace=True)
        df_tasks['T1'].fillna(df_tasks['Time'], inplace=True)
        df_tasks['R0'] = df_tasks['R'].shift(periods=1, fill_value=0)
        df_tasks['C'] = df_tasks['R0'] * (df_tasks['Time'] - df_tasks['Time0'])
        df_tasks = df_tasks.reset_index(drop=True)

        return df_tasks


    df_tasks = count_concurrent(df_tasks)

    df_tasks['iR'] = df_tasks['R'] * (df_tasks['T1'] - df_tasks['Time'])
    df_tasks_g = df_tasks.groupby(['filename'])
    avg_p = (df_tasks_g['iR'].sum().sort_index() / df_durations).reset_index().rename(columns={0:'avgP'})
    avg_p['Storage'] = avg_p['filename'].transform(lambda x: ('spark-' if 'spark' in x else '') + op.basename(x).split("_")[0].split("-")[-1])
    print(avg_p.groupby(['Storage']).mean())
    print(avg_p.groupby(['Storage']).std())


print(sys.argv[1])
all_files = glob(op.abspath(sys.argv[1]))
all_files.sort(
    key=lambda x: disks.index(
        op.basename(x).strip("spark-").split("_")[0].split("-")[1]
    )
)

spark = False
d_white = True
if "spark" in sys.argv:
    spark = True

df = pd.concat(
    (
        pd.read_csv(
            f,
            delim_whitespace='spark' not in f,
            names=["Task", "Start", "End", "File", "ThreadId"],
        ).assign(
            filename="{0}-{1}".format(i, op.basename(f)),
            disk=op.basename(f).strip("spark-").split("_")[0].split("-")[-1],
        )
        for i, f in enumerate(all_files)
    )
)

df["Duration"] = df.End - df.Start
makespan(df, spark=spark)
#violinplots(df)
# makespan(df, "read_file")
trendline_tasks(df)
