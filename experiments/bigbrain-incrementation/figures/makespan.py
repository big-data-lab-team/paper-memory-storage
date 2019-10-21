#!/usr/bin/env python3

import sys
import pandas as pd
import matplotlib.pyplot as plt
from glob import glob
from os import path as op
import numpy as np


def configure_df(df):
    df_end = df.End.max()
    df_start = df.Start.min()

    df_data = pd.concat([df_start, df_end], axis=1)
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

    labels = ["tmpfs", "Optane", "local disk", "Isilon"]
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
        print(df_pres_mem)
    else:
        df_sres = configure_df(dfs)
        df_peres = configure_df(dfpe)
        df_seres = configure_df(dfse)


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

        df_sres_ad = df_sres[df_sres.index.map(lambda x: 'AD' in x)]
        df_sres_ad = df_sres_ad.sort_index(ascending=False)
        df_sres_ad.rename(index={'tmpfsAD':'tmpfs', 'optaneAD': 'optane', 'localAD': 'local', 'isilonAD':'isilon'}, inplace=True)

        
        df_sres_ad_em = df_seres[df_seres.index.map(lambda x: 'AD' in x and 'em' in x)]
        df_sres_ad_em = df_sres_ad_em.sort_index(ascending=False)
        df_sres_ad_em.rename(index={'tmpfsAD':'tmpfs', 'optaneAD': 'optane', 'localAD': 'local', 'isilonAD':'isilon'}, inplace=True)
        

    width = 0.35

    fig, ax = plt.subplots()
    if not spark:
        ind_mem = np.delete(ind, 1)
        pmem_read = ax.bar(
            ind_mem - width / 2,
            df_pres_mem["mean"],
            width,
            alpha=0.4,
            color="b",
            yerr=df_pres_mem["std"],
            label="Memory mode",
        )
        pad_read = ax.bar(
            ind + width / 2,
            df_pres_ad["mean"],
            width,
            alpha=0.4,
            color="orange",
            yerr=df_pres_ad["std"],
            label="App Direct mode",
        )
    else:
        width = 0.2
        ind_mem_real = np.delete(ind, [1, 2])
        ind_mem_em = np.delete(ind, 1)
        ind_ad_real = np.delete(ind, [0, 2])
        labels = ["tmpfs", "optane", "local", "isilon"]

        print(ind_mem_em)
        print(df_pres_mem_em)
        pmem_pem = ax.bar(
            ind_mem_em - 3 * width / 2,
            df_pres_mem_em["mean"],
            width,
            alpha=0.4,
            color="r",
            yerr=df_pres_mem_em["std"],
            label="Memory mode - GNU Parallel",
        )

        pmem_sem = ax.bar(
            ind_mem_em - width / 2,
            df_sres_mem_em["mean"],
            width,
            alpha=0.4,
            color="b",
            yerr=df_sres_mem_em["std"],
            label="Memory mode - Spark",
        )

        '''pad_read_real = ax.bar(
            ind_ad_real + width / 2,
            df_res_ad["mean"],
            width,
            alpha=0.4,
            color="orange",
            yerr=df_res_ad["std"],
            label="App Direct real",
        )'''
        pad_sem = ax.bar(
            ind + 1.5 * width,
            df_sres_ad_em["mean"],
            width,
            alpha=0.4,
            color="green",
            yerr=df_sres_ad_em["std"],
            label="App Direct - Spark",
        )

        
        ax.set_ylabel("Mean makespan (s)")
        ax.set_xlabel("Storage type")
        ax.set_xticks(ind)
        ax.set_xticklabels(labels)
        plt.legend()
        plt.savefig("makespan-em-{}.pdf".format(sys.argv[2]))


        fig, ax = plt.subplots()
        print(df_pres_mem)
        pmem_real = ax.bar(
            ind_mem_real - 3 * width / 2,
            df_pres_mem["mean"],
            width,
            alpha=0.4,
            color="r",
            yerr=df_pres_mem["std"],
            label="Memory mode - GNU Parallel",
        )
        pmem_sreal = ax.bar(
            ind_mem_real - width / 2,
            df_sres_mem["mean"],
            width,
            alpha=0.4,
            color="b",
            yerr=df_sres_mem["std"],
            label="Memory mode - Spark",
        )
        '''pad_read_real = ax.bar(
            ind_ad_real + width / 2,
            df_res_ad["mean"],
            width,
            alpha=0.4,
            color="orange",
            yerr=df_res_ad["std"],
            label="App Direct real",
        )'''
        pad_sreal = ax.bar(
            ind_ad_real + 1.5 * width,
            df_sres_ad["mean"],
            width,
            alpha=0.4,
            color="green",
            yerr=df_sres_ad["std"],
            label="App Direct - Spark",
        )

    # plt.ylim(0, 13000)
    ax.set_ylabel("Mean makespan (s)")
    ax.set_xlabel("Storage type")
    ax.set_xticks(ind)
    ax.set_xticklabels(labels)
    plt.legend()
    plt.savefig("makespan-real-{}.pdf".format(sys.argv[2]))


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
# makespan(df, "read_file")
