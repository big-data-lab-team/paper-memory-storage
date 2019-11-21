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
        lambda x: x.split("_")[0].split("-")[-1] + ("-em" if "_em" in x else "-real")
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

    labels = ["DRAM", "Optane", "local SSD*", "Isilon"]
    if not spark:
        dfp = df[df["Task"] == "task_duration"].groupby(["filename"])
    else:
        dfp = df[(~df["filename"].str.contains('spark')) & (df["Task"] == "task_duration") & ~(df["filename"].str.contains('em'))].groupby(["filename"])
        dfpe = df[(~df["filename"].str.contains('spark')) & (df["Task"] == "task_duration") & (df["filename"].str.contains('em'))].groupby(["filename"])
        dfs = df[df["filename"].str.contains('spark') & ~(df["filename"].str.contains('em'))].groupby(["filename"])
        dfse = df[df["filename"].str.contains('spark') & (df["filename"].str.contains('em'))].groupby(["filename"])

    ind = np.arange(len(labels))

    df_pres = configure_df(dfp)

    if not spark:
        df_pres_mem = df_pres[
            df_pres.index.map(lambda x: x in ["tmpfs-real", "local-real", "isilon-real"])
        ]
        df_pres_mem = df_pres_mem.sort_index(ascending=False)
        df_pres_ad = df_pres[df_pres.index.map(lambda x: "AD" in x)]
        df_pres_ad = df_pres_ad.sort_index(ascending=False)
        df_pres_ad.rename(
            index={
                "tmpfsAD-real": "tmpfs",
                "optaneAD-real": "optane",
                "localAD-real": "local",
                "isilonAD-real": "isilon",
            },
            inplace=True,
        )
        print(df_pres)
    else:
        df_sres = configure_df(dfs)
        df_peres = configure_df(dfpe)
        df_seres = configure_df(dfse)

        print(df_pres)
        print(df_sres)
        print(df_peres)
        print(df_seres)


        df_pres_mem = df_pres[
            df_pres.index.map(lambda x: 'AD' not in x)
        ]
        df_pres_mem = df_pres_mem.sort_index(ascending=False)

        df_sres_mem = df_sres[
            df_sres.index.map(lambda x: 'AD' not in x)
        ]
        df_sres_mem = df_sres_mem.sort_index(ascending=False)
        
        df_pres_mem_em = df_peres[
            df_peres.index.map(lambda x: 'AD' not in x)
        ]
        df_pres_mem_em = df_pres_mem_em.sort_index(ascending=False)

        df_sres_mem_em = df_seres[
            df_seres.index.map(lambda x: 'AD' not in x)
        ]
        df_sres_mem_em = df_sres_mem_em.sort_index(ascending=False)

        df_pres_ad = df_pres[
            df_pres.index.map(lambda x: 'AD' not in x)
        ]
        df_pres_ad = df_pres_ad.sort_index(ascending=False)
        df_pres_ad.rename(index={'tmpfsAD':'tmpfs', 'optaneAD': 'optane', 'localAD': 'local', 'isilonAD':'isilon'}, inplace=True)

        df_sres_ad = df_sres[df_sres.index.map(lambda x: 'AD' in x)]
        df_sres_ad = df_sres_ad.sort_index(ascending=False)
        df_sres_ad.rename(index={'tmpfsAD':'tmpfs', 'optaneAD': 'optane', 'localAD': 'local', 'isilonAD':'isilon'}, inplace=True)

        
        df_sres_ad_em = df_seres[df_seres.index.map(lambda x: 'AD' in x and 'em' in x)]
        df_sres_ad_em = df_sres_ad_em.sort_index(ascending=False)
        df_sres_ad_em.rename(index={'tmpfsAD':'tmpfs', 'optaneAD': 'optane', 'localAD': 'local', 'isilonAD':'isilon'}, inplace=True)


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
        labels = ['Optane', 'Isilon']

    print(df_memb_mean)
    print(df_memb_std)
    print(df_adb_mean)
    print(df_adb_std)

    width = 0.15

    fig, ax = plt.subplots()
    if not spark:
        if not bb20:
            ind_mem = np.delete(ind, 0)
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
    else:
        width = 0.2
        labels_em =  ["DRAM", "Optane", "local disk", "Isilon"]
        ind = np.arange(len(labels_em))
        ind_mem_em = np.delete(ind, 0)

        #print(ind_mem_em)
        #print(df_pres_mem_em)
        '''pmem_pem = ax.bar(
            ind_mem_em - 3 * width / 2,
            df_pres_mem_em["mean"],
            width,
            alpha=0.4,
            color="gray",
            yerr=df_pres_mem_em["std"],
            label="Memory mode - GNU Parallel",
        )'''

        '''pad_read_real = ax.bar(
            ind_ad_real - width / 2,
            df_res_ad["mean"],
            width,
            alpha=0.4,
            color="gray",
            hatch="//",
            yerr=df_res_ad["std"],
            label="App Direct real",
        )'''

        '''pmem_sem = ax.bar(
            ind_mem_em + width / 2,
            df_sres_mem_em["mean"],
            width,
            alpha=0.4,
            color="gray",
            hatch='--',
            yerr=df_sres_mem_em["std"],
            label="Memory mode - Spark",
        )


        pad_sem = ax.bar(
            ind + 1.5 * width,
            df_sres_ad_em["mean"],
            width,
            alpha=0.4,
            color="gray",
            hatch='..',
            yerr=df_sres_ad_em["std"],
            label="App Direct - Spark",
        )

        
        ax.set_ylabel("Mean makespan (s)")
        ax.set_xlabel("Storage type")
        ax.set_xticks(ind)
        ax.set_xticklabels(labels_em)
        ax.set_ylim([0, 12000])
        plt.legend()
        plt.savefig("makespan-em-{}.pdf".format(sys.argv[2]))
        '''
        labels = ["Optane", "Isilon"]
        ind = np.arange(len(labels))
        ind_mem_real = ind
        ind_ad_real = ind_mem_real

        fig, ax = plt.subplots()
        pmem_real = ax.bar(
            ind_mem_real - 3 * width / 2,
            df_pres_mem["mean"],
            width,
            alpha=0.4,
            color="gray",
            yerr=df_pres_mem["std"],
            label="Memory mode - GNU Parallel",
        )

        pad_read_real = ax.bar(
            ind_ad_real - width / 2,
            df_pres_ad["mean"],
            width,
            alpha=0.4,
            color="gray",
            hatch='//',
            yerr=df_pres_ad["std"],
            label="App Direct - GNU Parallel",
        )

        pmem_sreal = ax.bar(
            ind_mem_real + width / 2,
            df_sres_mem["mean"],
            width,
            alpha=0.4,
            color="gray",
            hatch='--',
            yerr=df_sres_mem["std"],
            label="Memory mode - Spark",
        )
        pad_sreal = ax.bar(
            ind_ad_real + 1.5 * width,
            df_sres_ad["mean"],
            width,
            alpha=0.4,
            color="gray",
            hatch='..',
            yerr=df_sres_ad["std"],
            label="App Direct - Spark",
        )
        ax.set_ylim([0, 12000])

    # plt.ylim(0, 13000)
    print('TEST')
    ax.set_ylabel("Mean makespan (s)")
    ax.set_xlabel("Storage type")
    ax.set_xticks(ind)
    ax.set_xticklabels(labels)
    plt.legend()
    #plt.show()
    #plt.savefig("makespan-real-{}.pdf".format(sys.argv[2]))
    plt.savefig("makespan-real-25.pdf")#.format(sys.argv[2]))
    

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


def violinplots(df):

    df_memb_mean, df_adb_mean = get_dbench()

    if bb20:
        ind = np.asarray([0, 1])
        ind_mem = ind
        labels = ['Optane', 'Isilon']


    labels = ["DRAM", "Optane", "local disk", "Isilon"]

    if '40bb' in sys.argv[1]:
        df_read = df[df["Task"].str.contains("read")]
        df_increment = df[df["Task"].str.contains("increment")]
        df_write = df[df["Task"].str.contains("write")]
        size = 614
    else:
        df_read = df[df["Task"].str.contains("load")]
        df_increment = df[df["Task"].str.contains("increment_data")]
        df_write = df[df["Task"].str.contains("save")]
        size = 617

    df_increment_mem_optane = df_increment[~df_increment['filename'].str.contains('AD') & df_increment['disk'].str.contains('tmpfs')] 
    #df_increment_mem_local = df_increment[~df_increment['filename'].str.contains('AD') & df_increment['disk'].str.contains('local')] 
    df_increment_mem_isilon = df_increment[~df_increment['filename'].str.contains('AD') & df_increment['disk'].str.contains('isilon')] 

    df_increment_mem_optane['membw'] = size / df_increment_mem_optane['Duration']
    #df_increment_mem_local['membw'] = size / df_increment_mem_local['Duration']
    df_increment_mem_isilon['membw'] = size / df_increment_mem_isilon['Duration']

    df_increment_ad_dram = df_increment[df_increment['filename'].str.contains('AD') & df_increment['disk'].str.contains('tmpfs')] 
    df_increment_ad_optane = df_increment[df_increment['filename'].str.contains('AD') & df_increment['disk'].str.contains('tmpfs')] 
    df_increment_ad_isilon = df_increment[df_increment['filename'].str.contains('AD') & df_increment['disk'].str.contains('isilon')] 

    df_increment_ad_dram['membw'] = size / df_increment_mem_optane['Duration']
    df_increment_ad_optane['membw'] = size / df_increment_mem_optane['Duration']
    df_increment_ad_isilon['membw'] = size / df_increment_mem_isilon['Duration']

    df_write_mem_optane = df_write[~df_write['filename'].str.contains('AD') & df_write['disk'].str.contains('tmpfs')].head(357) 
    #df_write_mem_local = df_write[~df_write['filename'].str.contains('AD') & df_write['disk'].str.contains('local')].head(357) 
    df_write_mem_isilon = df_write[~df_write['filename'].str.contains('AD') & df_write['disk'].str.contains('isilon')].head(357) 

    df_write_mem_optane['membw'] = size / df_write_mem_optane['Duration']
    #df_write_mem_local['membw'] = size / df_write_mem_local['Duration']
    df_write_mem_isilon['membw'] = size / df_write_mem_isilon['Duration']

    df_write_ad_dram = df_write[df_write['filename'].str.contains('AD') & df_write['disk'].str.contains('tmpfs')]#.head(357)
    df_write_ad_optane = df_write[df_write['filename'].str.contains('AD') & df_write['disk'].str.contains('optane')]#.head(357)
    #df_write_ad_local = df_write[df_write['filename'].str.contains('AD') & df_write['disk'].str.contains('local')]#.head(357)
    df_write_ad_isilon = df_write[df_write['filename'].str.contains('AD') & df_write['disk'].str.contains('isilon')]#.head(357)

    df_write_ad_dram['membw'] = size / df_write_ad_dram['Duration']
    df_write_ad_optane['membw'] = size / df_write_ad_optane['Duration']
    #df_write_ad_local['membw'] = size / df_write_ad_local['Duration']
    df_write_ad_isilon['membw'] = size / df_write_ad_isilon['Duration']

    # Create the boxplot
    ind = np.arange(0, 6, 2)
    ind_mem = np.delete(ind, 0)
    width = 0.4
    labels = ['DRAM', 'Optane', 'Isilon']

    if '40bb' in sys.argv[1]:
        print('test')
        fig, ax = plt.subplots()
        vpw_mem = ax.violinplot([ df_increment_mem_optane['membw'], df_increment_mem_isilon['membw'] ], ind_mem - width/2, widths=width)
        vpw_ad = ax.violinplot([ df_increment_ad_dram['membw'], df_increment_ad_optane['membw'], df_increment_ad_isilon['membw'] ], ind + width/2, widths=width)
        ax.axhline(df_adb_mean.loc['tmpfsAD', 'Write'], linestyle='-', color='k')
        ax.axhline(df_adb_mean.loc['optaneAD', 'Write'], linestyle='--', color='k')
        ax.axhline(df_adb_mean.loc['isilonAD', 'Write'], linestyle=':', color='k')
        ax.set_xticks(ind)
        ax.set_xticklabels(labels)
        plt.savefig('violin-read-{}.pdf'.format(sys.argv[2]))

        fig, ax = plt.subplots()
        vpw_mem = ax.violinplot([ df_write_mem_optane['membw'], df_write_mem_isilon['membw'] ], ind_mem - width/2, widths=width)
        vpw_ad = ax.violinplot([ df_write_ad_dram['membw'], df_write_ad_optane['membw'], df_write_ad_isilon['membw'] ], ind + width/2, widths=width)
        ax.axhline(df_adb_mean.loc['tmpfsAD', 'Write'], linestyle='-', color='k')
        ax.axhline(df_adb_mean.loc['optaneAD', 'Write'], linestyle='--', color='k')
        ax.axhline(df_adb_mean.loc['isilonAD', 'Write'], linestyle=':', color='k')
        ax.set_xticks(ind)
        ax.set_xticklabels(labels)
        plt.savefig('violin-write-{}.pdf'.format(sys.argv[2]))

    else:
        print('test')
        ind = np.arange(0, 4, 2)
        ind_mem = ind
        width = 0.4
        labels = ['Optane', 'Isilon']
        fig, ax = plt.subplots()
        vpw_mem = ax.violinplot([ df_increment_mem_optane['BW'], df_increment_mem_isilon['BW'] ], ind_mem - width/2, widths=width)
        vpw_ad = ax.violinplot([ df_increment_ad_optane['BW'], df_increment_ad_isilon['BW'] ], ind + width/2, widths=width)
        ax.axhline(df_adb_mean.loc['tmpfsAD', 'Write'], linestyle='-', color='k')
        ax.axhline(df_adb_mean.loc['optaneAD', 'Write'], linestyle='--', color='k')
        ax.axhline(df_adb_mean.loc['isilonAD', 'Write'], linestyle=':', color='k')
        ax.set_xticks(ind)
        ax.set_xticklabels(labels)
        plt.savefig('violin-read-{}.pdf'.format(sys.argv[2]))

        fig, ax = plt.subplots()
        vpw_mem = ax.violinplot([ df_write_mem_optane['BW'], df_write_mem_isilon['BW'] ], ind_mem - width/2, widths=width)
        vpw_ad = ax.violinplot([ df_write_ad_optane['BW'], df_write_ad_isilon['BW'] ], ind + width/2, widths=width)
        ax.axhline(df_adb_mean.loc['tmpfsAD', 'Write'], linestyle='-', color='k')
        ax.axhline(df_adb_mean.loc['optaneAD', 'Write'], linestyle='--', color='k')
        ax.axhline(df_adb_mean.loc['isilonAD', 'Write'], linestyle=':', color='k')
        ax.set_xticks(ind)
        ax.set_xticklabels(labels)
        ax.legend()
        plt.savefig('violin-write-{}.pdf'.format(sys.argv[2]))

    for pc in vpw_ad['bodies']:
        pc.set_facecolor('r')
        #pc.set_edgecolor('r')




def trendline_tasks(df):
   
    df_memb_mean, df_adb_mean = get_dbench()

    def fill_start(grp, val):
        grp['Start'] = val
        return grp

    if bb20:
        size = 617
    else:
        size = 614

    df_increment = df[~df['Task'].str.contains('task')]# & df['Task'].str.contains('increment')]
    df_write = df[~df['Task'].str.contains('task')]# & df['Task'].str.contains('write')]

    df_durations = df[df['Task'].str.contains('task')].groupby(['filename'])
    df_start = df_durations['Start'].min()
    df_end = df_durations['End'].max()

    df_durations = (df_end - df_start)
    

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


    df_tasks = count_concurrent(df_increment)
    ''' old way of calculating
    df_tasks['BW'] = ((size * ((df_tasks['T1'] - df_tasks['Time']) / df_tasks['Duration'])) / (df_tasks['T1'] - df_tasks['Time'])) * df_tasks['R']
    df_tasks['BW'].fillna(0, inplace=True)
    df_tasks_grouped = df_tasks.groupby(['filename','File', 'Task'])
    bwidths = ((df_tasks_grouped['BW'].sum() / df_tasks_grouped['Duration'].unique().transform(lambda x: x[0])) / df_durations).reset_index().rename(columns={0:'BW'}).fillna(0)
    '''


    df_tasks['iD'] = df_tasks['T1'] - df_tasks['Time']
    df_tasks['aP'] = df_tasks['R'] * (df_tasks['T1'] - df_tasks['Time'])
    print(df_tasks['aP'] / df_tasks['Duration'])
    #df_tasks['BW'] = ((size * ((df_tasks['T1'] - df_tasks['Time']) / df_tasks['Duration']))) * df_tasks['R']
    df_tasks['aP'].fillna(0, inplace=True)
    df_tasks['total_time'] = df_tasks['iD'].transform(lambda x: df_tasks )
    print(df_tasks)
    df_tasks_grouped = df_tasks.groupby(['filename','File', 'Task', 'Duration'])
    print(df_tasks_grouped['iD'].sum())
    bwidths = (size*(df_tasks_grouped['aP'].sum()/ df_tasks_grouped['Duration'].unique().transform(lambda x: x[0]))/df_tasks_grouped['Duration'].unique().transform(lambda x: x[0])).reset_index().rename(columns={0:'BW'}).fillna(0)
    print(bwidths)
    #bwidths = ((df_tasks_grouped['BW'].sum() / df_tasks_grouped['Duration'].unique().transform(lambda x: x[0])) / df_durations).reset_index().rename(columns={0:'BW'}).fillna(0)

    if True:#'40bb' in sys.argv[1]:
        df_read_mem_optane = bwidths[~bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('tmpfs') & bwidths['Task'].str.contains('increment')]
        df_read_mem_local = bwidths[~bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('local') & bwidths['Task'].str.contains('increment')]
        df_read_mem_isilon = bwidths[~bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('isilon') & bwidths['Task'].str.contains('increment')]

        df_read_ad_dram = bwidths[bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('tmpfs') & bwidths['Task'].str.contains('increment')]
        df_read_ad_optane = bwidths[bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('optane') & bwidths['Task'].str.contains('increment')]
        df_read_ad_local = bwidths[bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('local') & bwidths['Task'].str.contains('increment')]
        df_read_ad_isilon = bwidths[bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('isilon') & bwidths['Task'].str.contains('increment')]

        df_write_mem_optane = bwidths[~bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('tmpfs') & bwidths['Task'].str.contains('write')].fillna(0)
        df_write_mem_local = bwidths[~bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('local') & bwidths['Task'].str.contains('write')].fillna(0)
        df_write_mem_isilon = bwidths[~bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('isilon') & bwidths['Task'].str.contains('write')].fillna(0)

        df_write_ad_dram = bwidths[bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('tmpfs') & bwidths['Task'].str.contains('write')].fillna(0)
        df_write_ad_optane = bwidths[bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('optane') & bwidths['Task'].str.contains('write')]
        df_write_ad_local = bwidths[bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('local') & bwidths['Task'].str.contains('write')]
        df_write_ad_isilon = bwidths[bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('isilon') & bwidths['Task'].str.contains('write')]

        local_mem_write_bw = df_write_mem_local['BW'].append(pd.Series([0]*(375 - df_write_mem_local['BW'].count())))
        local_ad_write_bw = df_write_ad_local['BW'].append(pd.Series([0]*(375 - df_write_ad_local['BW'].count())))

        pd.set_option('display.float_format', lambda x: '%.3f' % x)

        ind = np.arange(0, 8, 2)
        ind_mem = np.delete(ind, 0)
        width = 0.4
        labels = ['DRAM', 'Optane', 'Local SSD*', 'Isilon']
        fig, ax = plt.subplots()
        vpw_mem = ax.violinplot([ df_read_mem_optane['BW'], df_read_mem_local['BW'], df_read_mem_isilon['BW'] ], ind_mem - width/2, widths=width)
        vpw_ad = ax.violinplot([ df_read_ad_dram['BW'], df_read_ad_optane['BW'], df_read_ad_local['BW'], df_read_ad_isilon['BW'] ], ind + width/2, widths=width)

        col_memr = '#e60000'
        col_adr = '#ffb3b3'
        plt.setp(vpw_mem['bodies'], facecolor=col_memr, alpha=0.5)
        plt.setp(vpw_mem['cbars'], edgecolor=col_memr)
        plt.setp(vpw_mem['cmins'], edgecolor=col_memr)
        plt.setp(vpw_mem['cmaxes'], edgecolor=col_memr)
        plt.setp(vpw_ad['bodies'], facecolor=col_adr, alpha=0.5)
        plt.setp(vpw_ad['cbars'], edgecolor=col_adr)
        plt.setp(vpw_ad['cmins'], edgecolor=col_adr)
        plt.setp(vpw_ad['cmaxes'], edgecolor=col_adr)

        print('df_read_mem_optane bw\n', df_read_mem_optane['BW'].describe())
        print('df_read_mem_local bw\n', df_read_mem_local['BW'].describe())
        print('df_read_mem_isilon bw\n', df_read_mem_isilon['BW'].describe())
        print('df_read_ad_dram bw\n', df_read_ad_dram['BW'].describe())
        print('df_read_ad_optane bw\n', df_read_ad_optane['BW'].describe())
        print('df_read_ad_local bw\n', df_read_ad_local['BW'].describe())
        print('df_read_ad_isilon bw\n', df_read_ad_isilon['BW'].describe())


        dram = ax.axhline(df_adb_mean.loc['tmpfsAD', 'Read'], linestyle='-', color='k', label='DRAM', linewidth=0.5)
        optane = ax.axhline(df_adb_mean.loc['optaneAD', 'Read'], linestyle='--', color='k', label='Optane', linewidth=0.5)
        local = ax.axhline(df_adb_mean.loc['localAD', 'Read'], linestyle='-.', color='k', label='Local SSD', linewidth=0.5)
        isilon = ax.axhline(df_adb_mean.loc['isilonAD', 'Read'], linestyle=':', color='k', label='Isilon', linewidth=0.5)
        ax.set_xticks(ind)
        ax.set_xticklabels(labels)
        ax.set_ylim(0, 33000)

        b_patch = mpatches.Patch(color=col_memr, label='Memory Mode')
        o_patch = mpatches.Patch(color=col_adr, label='App Direct Mode')

        ax.legend(handles=[b_patch, o_patch, dram, optane, local, isilon])
        plt.savefig('violin-read-{}.pdf'.format(sys.argv[2]))

        fig, ax = plt.subplots()
        vpw_mem = ax.violinplot([ df_write_mem_optane['BW'], local_mem_write_bw, df_write_mem_isilon['BW'] ], ind_mem - width/2, widths=width)
        vpw_ad = ax.violinplot([ df_write_ad_dram['BW'], df_write_ad_optane['BW'], local_ad_write_bw, df_write_ad_isilon['BW'] ], ind + width/2, widths=width)

        print('df_write_mem_optane bw\n', df_write_mem_optane['BW'].describe())
        print('df_write_mem_local bw\n', df_write_mem_local['BW'].describe())
        print('df_write_mem_isilon bw\n', df_write_mem_isilon['BW'].describe())
        print('df_write_ad_dram bw\n', df_write_ad_dram['BW'].describe())
        print('df_write_ad_optane bw\n', df_write_ad_optane['BW'].describe())
        print('df_write_ad_local bw\n', df_write_ad_local['BW'].describe())
        print('df_write_ad_isilon bw\n', df_write_ad_isilon['BW'].describe())

        col_memw = '#0000ff'
        col_adw = '#9999ff'

        plt.setp(vpw_mem['bodies'], facecolor=col_memw, alpha=0.5)
        plt.setp(vpw_mem['cbars'], edgecolor=col_memw)
        plt.setp(vpw_mem['cmins'], edgecolor=col_memw)
        plt.setp(vpw_mem['cmaxes'], edgecolor=col_memw)
        plt.setp(vpw_ad['bodies'], facecolor=col_adw, alpha=0.5)
        plt.setp(vpw_ad['cbars'], edgecolor=col_adw)
        plt.setp(vpw_ad['cmins'], edgecolor=col_adw)
        plt.setp(vpw_ad['cmaxes'], edgecolor=col_adw)

        dram = ax.axhline(df_adb_mean.loc['tmpfsAD', 'Write'], linestyle='-', color='k', label='DRAM', linewidth=0.5)
        optane = ax.axhline(df_adb_mean.loc['optaneAD', 'Write'], linestyle='--', color='k', label='Optane', linewidth=0.5)
        local = ax.axhline(df_adb_mean.loc['localAD', 'Write'], linestyle='-.', color='k', label='Local SSD', linewidth=0.5)
        isilon = ax.axhline(df_adb_mean.loc['isilonAD', 'Write'], linestyle=':', color='k', label='Isilon', linewidth=0.5)
        ax.set_xticks(ind)
        ax.set_xticklabels(labels)
        ax.set_ylim(0, 30000)
        b_patch = mpatches.Patch(color=col_memw, label='Memory Mode')
        o_patch = mpatches.Patch(color=col_adw, label='App Direct Mode')
        ax.legend(handles=[b_patch, o_patch, dram, optane, local, isilon])
        plt.savefig('violin-write-{}.pdf'.format(sys.argv[2]))

    else:
        df_read_mem_optane = bwidths[~bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('tmpfs') & bwidths['Task'].str.contains('increment')]
        df_read_mem_isilon = bwidths[~bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('isilon') & bwidths['Task'].str.contains('increment')]

        df_read_ad_optane = bwidths[bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('optane') & bwidths['Task'].str.contains('increment')]
        df_read_ad_isilon = bwidths[bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('isilon') & bwidths['Task'].str.contains('increment')]

        df_write_mem_optane = bwidths[~bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('tmpfs') & bwidths['Task'].str.contains('write')]
        df_write_mem_isilon = bwidths[~bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('isilon') & bwidths['Task'].str.contains('write')]

        df_write_ad_optane = bwidths[bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('optane') & bwidths['Task'].str.contains('write')]
        df_write_ad_isilon = bwidths[bwidths['filename'].str.contains('AD') & bwidths['filename'].str.contains('isilon') & bwidths['Task'].str.contains('write')]

        ind = np.arange(0, 4, 2)
        ind_mem = ind
        width = 0.4
        labels = ['Optane', 'Isilon']
        fig, ax = plt.subplots()
        vpw_mem = ax.violinplot([ df_read_mem_optane['BW'], df_read_mem_isilon['BW'] ], ind_mem - width/2, widths=width)
        vpw_ad = ax.violinplot([ df_read_ad_optane['BW'], df_read_ad_isilon['BW'] ], ind + width/2, widths=width)
        dram = ax.axhline(df_adb_mean.loc['tmpfsAD', 'Write'], linestyle='-', color='k', label='DRAM')
        optane = ax.axhline(df_adb_mean.loc['optaneAD', 'Write'], linestyle='--', color='k', label='Optane')
        isilon = ax.axhline(df_adb_mean.loc['isilonAD', 'Write'], linestyle=':', color='k', label='Isilon')

        ax.set_xticks(ind)
        ax.set_xticklabels(labels)
        ax.legend(handles=[b_patch, o_patch, dram, optane, isilon])
        plt.savefig('violin-read-{}.pdf'.format(sys.argv[2]))

        fig, ax = plt.subplots()
        vpw_mem = ax.violinplot([ df_write_mem_optane['BW'], df_write_mem_isilon['BW'] ], ind_mem - width/2, widths=width)
        vpw_ad = ax.violinplot([ df_write_ad_optane['BW'], df_write_ad_isilon['BW'] ], ind + width/2, widths=width)
        dram = ax.axhline(df_adb_mean.loc['tmpfsAD', 'Write'], linestyle='-', color='k', label='DRAM')
        optane = ax.axhline(df_adb_mean.loc['optaneAD', 'Write'], linestyle='--', color='k', label='Optane')
        isilon = ax.axhline(df_adb_mean.loc['isilonAD', 'Write'], linestyle=':', color='k', label='Isilon')
        ax.set_xticks(ind)
        ax.set_xticklabels(labels)
        leg1 = ax.legend()
        axs = plt.gca().add_artist(leg1)
        b_patch = mpatches.Patch(color='b', label='Memory Mode')
        o_patch = mpatches.Patch(color='orange', label='App Direct Mode')
        ax.legend(handles=[b_patch, o_patch, dram, optane, isilon])
        plt.savefig('violin-write-{}.pdf'.format(sys.argv[2]))


print(sys.argv[1])
all_files = glob(op.abspath(sys.argv[1]))
disks = ["tmpfs", "tmpfsAD", "optaneAD", "local", "localAD", "isilon", "isilonAD"]
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
#trendline_tasks(df)
