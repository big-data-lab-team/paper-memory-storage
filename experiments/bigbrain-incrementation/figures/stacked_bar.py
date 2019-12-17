#!/usr/bin/env python3

import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatch
from glob import glob
from os import path as op
import numpy as np


ad_bench = '../../system_conf/ad_diskbench.out'
mem_bench = '../../system_conf/mem_diskbench.out'
data_size = 614 * 125
total_1000 = 617 * 1000
bb20 = '20bb' in sys.argv[1]


def add_disk_col(df):
    df["disk"] = df.index.map(
        lambda x: x.split("_")[0].split("-")[-1] + ("-em" if "_em" in x else "-real")
    )


def get_mean_std(df, r_task, i_task, w_task):
    dfr = (
        df[df["Task"] == r_task]
        .rename(columns={"Duration": "read_time"})
        .groupby(["filename"])["read_time"]
        .sum()
    )
    dfi = (
        df[df["Task"] == i_task]
        .rename(columns={"Duration": "increment_time"})
        .groupby(["filename"])["increment_time"]
        .sum()
    )
    dfw = (
        df[df["Task"] == w_task]
        .rename(columns={"Duration": "write_time"})
        .groupby(["filename"])["write_time"]
        .sum()
    )

    df_sum = pd.concat([dfr, dfi, dfw], axis=1)

    add_disk_col(df_sum)

    df_mean = df_sum.set_index("disk").groupby("disk").mean()
    df_std = df_sum.set_index("disk").groupby("disk").std()
    print("\n\nMean:")
    print(df_mean)
    print("\n\nSTD:")
    print(df_std)

    return df_mean, df_std


def stacked_bar(df, spark=False):

    global data_size, total_1000

    labels = ["DRAM", "Optane DCPMM", "local SSD", "Isilon"]
    ind = np.arange(len(labels))
    ind_mem = np.delete(ind, 0)

    r_task = "read_file"
    i_task = "increment_file"
    w_task = "write_file"

    dfp = df[~df["filename"].str.contains('spark')]

    print('**GNU Parallel**\n\n')
    df_mean, df_std = get_mean_std(dfp, r_task, i_task, w_task)

    def prep_mem_df(df, exec_type=None):
        mem_labels = ["tmpfs", "local", "isilon"]

        if exec_type is not None:
            if exec_type == "real":
                mem_labels = ["tmpfs-real", "local-real", "isilon-real"]
            else:
                mem_labels = ["tmpfs-em", "local-em", "isilon-em"]

        df = df[df.index.map(lambda x: 'AD' not in x and (exec_type is None or x in mem_labels))]
        return df.sort_index(ascending=False)

    if not spark:
        df_mean_mem = prep_mem_df(df_mean)
        df_std_mem = prep_mem_df(df_std)
    else:
        dfs = df[df["filename"].str.contains('spark')]
        print('**Spark**\n\n')
        df_smean, df_sstd = get_mean_std(dfs, "load_img", "increment_data", "save_incremented") 

        df_mean_mem_r = prep_mem_df(df_mean, "real")
        df_std_mem_r = prep_mem_df(df_std, "real")

        df_smean_mem_r = prep_mem_df(df_smean, "real")
        df_sstd_mem_r = prep_mem_df(df_sstd, "real")

    def prep_ad_df(df, exec_type=None):
        df = df[df.index.map(lambda x: "AD" in x and (exec_type is None or exec_type in x))]
        df = df.sort_index(ascending=False)

        if exec_type == "real":
            df.rename(
                index={
                    "tmpfsAD-real": "tmpfs",
                    "optaneAD-real": "optane",
                    "localAD-real": "local",
                    "isilonAD-real": "isilon",
                },
                inplace=True,
            )
        elif exec_type == "em":
            df.rename(
                index={
                    "tmpfsAD-em": "tmpfs",
                    "optaneAD-em": "optane",
                    "localAD-em": "local",
                    "isilonAD-em": "isilon",
                },
                inplace=True,
            )
        else:
            df.rename(
                index={
                    "tmpfsAD": "tmpfs",
                    "optaneAD": "optane",
                    "localAD": "local",
                    "isilonAD": "isilon",
                },
                inplace=True,
            )
        return df

    if not spark:
        df_mean_ad = prep_ad_df(df_mean)
        df_std_ad = prep_ad_df(df_std)
    else:
        df_mean_ad_r = prep_ad_df(df_mean, "real")
        df_std_ad_r = prep_ad_df(df_std, "real")

        df_smean_ad_r = prep_ad_df(df_smean, "real")
        df_sstd_ad_r = prep_ad_df(df_sstd, "real")

    width = 0.2

    alpha = 0.4

    figure, ax = plt.subplots()

    df_adb = pd.read_csv(ad_bench)
    convert_gb = lambda x: float(x.strip(' GB/s')) * 1024 if 'GB/s' in x else float(x.strip(' MB/s'))
    df_adb['Write'] = df_adb['Write'].apply(convert_gb)
    df_adb['Read'] = df_adb['Read'].apply(convert_gb)

    if bb20:
        data_size = total_1000
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

    print(data_size)
    print(df_memb_mean)
    print(df_memb_std)
    print(df_adb_mean)
    print(df_adb_std)
    #print(df_mean_mem)

    if not spark:
        p_read_mem = ax.bar(
            ind_mem - width / 2,
            df_mean_mem["read_time"],
            width,
            color="r",
            label="load header",
            alpha=alpha,
            yerr=df_std_mem["read_time"],
            edgecolor="black",
        )
        # p_expread_mem = ax.bar(
        #     ind_mem - width / 2,
        #     df_memb_mean["ReadTime"],
        #     width,
        #     color="pink",
        #     label="Expected Read Time",
        #     alpha=alpha,
        #     yerr=df_memb_std["ReadTime"],
        #     edgecolor="black",
        # )
        p_read_ad = ax.bar(
            ind + width / 2,
            df_mean_ad["read_time"],
            width,
            color="r",
            hatch="//",
            alpha=alpha,
            yerr=df_std_ad["read_time"],
            edgecolor="black",
        )
        # p_expread_ad = ax.bar(
        #     ind + 3*width / 2,
        #     df_adb_mean["ReadTime"],
        #     width,
        #     color="pink",
        #     hatch="//",
        #     alpha=alpha,
        #     yerr=df_adb_std["ReadTime"],
        #     edgecolor="black",
        # )
        p_inc_mem = ax.bar(
            ind_mem - width / 2,
            df_mean_mem["increment_time"],
            width,
            bottom=np.array(df_mean_mem["read_time"]),
            color="r",
            label="read/increment",
            alpha=alpha + 0.2,
            yerr=df_std_mem["increment_time"],
            edgecolor="black",
        )
        p_inc_ad = ax.bar(
            ind + width / 2,
            df_mean_ad["increment_time"],
            width,
            bottom=np.array(df_mean_ad["read_time"]),
            color="r",
            hatch="//",
            alpha=alpha + 0.2,
            yerr=df_std_ad["increment_time"],
            edgecolor="black",
        )
        p_write_mem = ax.bar(
            ind_mem - width / 2,
            df_mean_mem["write_time"],
            width,
            bottom=np.array(df_mean_mem["read_time"])
            + np.array(df_mean_mem["increment_time"]),
            color="g",
            label="write",
            alpha=alpha,
            yerr=df_std_mem["write_time"],
            edgecolor="black",
        )
        # p_expwrite_mem = ax.bar(
        #     ind_mem - width / 2,
        #     df_memb_mean["WriteTime"],
        #     width,
        #     bottom=df_memb_mean["ReadTime"],
        #     color="orange",
        #     label="Expected Write Time",
        #     alpha=alpha,
        #     yerr=df_memb_std["WriteTime"],
        #     edgecolor="black",
        # )
        p_write_ad = ax.bar(
            ind + width / 2,
            df_mean_ad["write_time"],
            width,
            bottom=np.array(df_mean_ad["read_time"])
            + np.array(df_mean_ad["increment_time"]),
            color="g",
            hatch="//",
            alpha=alpha,
            yerr=df_std_ad["write_time"],
            edgecolor="black",
        )
        # p_expwrite_ad = ax.bar(
        #     ind + 3*width / 2,
        #     df_adb_mean["WriteTime"],
        #     width,
        #     bottom=df_adb_mean["ReadTime"],
        #     color="orange",
        #     hatch="//",
        #     alpha=alpha,
        #     yerr=df_adb_std["WriteTime"],
        #     edgecolor="black",
        # )

        ax.set_ylabel("Average total execution time (s)")
        ax.set_xlabel("Storage type")
        ax.set_xticks(ind)
        ax.set_xticklabels(labels)
        #ax.set_ylim([0,8000])
        #plt.show()
        plt.savefig("stacked-real-{}.pdf".format(sys.argv[2]))

    else:

        labels = ["Optane DCPMM", "Isilon"]
        ind = np.arange(len(labels))
        ind_mem_r = ind
        ind_ad_r = ind

        figure, ax = plt.subplots()
        #### REAL
        p_read_mem_r = ax.bar(
            ind_mem_r - width / 2,
            df_smean_mem_r["read_time"],
            width,
            color="r",
            label="convert data",
            alpha=alpha,
            yerr=df_sstd_mem_r["read_time"],
            edgecolor="black",
        )
        p_read_ad_r = ax.bar(
            ind_ad_r + width / 2,
            df_smean_ad_r["read_time"],
            width,
            color="r",
            hatch="//",
            label="convert data",
            alpha=alpha,
            yerr=df_sstd_ad_r["read_time"],
            edgecolor="black",
        )
        # p_sread_mem_r = ax.bar(
        #     ind_mem_r + width / 2,
        #     df_smean_mem_r["read_time"],
        #     width,
        #     color="r",
        #     hatch="--",
        #     label="read",
        #     alpha=alpha,
        #     yerr=df_sstd_mem_r["read_time"],
        #     edgecolor="black",
        # )
        # p_sread_ad_r = ax.bar(
        #     ind_ad_r + 3*width / 2,
        #     df_smean_ad_r["read_time"],
        #     width,
        #     color="r",
        #     hatch="..",
        #     label="read",
        #     alpha=alpha,
        #     yerr=df_sstd_ad_r["read_time"],
        #     edgecolor="black",
        # )
        p_inc_mem_r = ax.bar(
            ind_mem_r - width / 2,
            df_smean_mem_r["increment_time"],
            width,
            bottom=np.array(df_smean_mem_r["read_time"]),
            color="r",
            label="increment",
            alpha=1,
            yerr=df_sstd_mem_r["increment_time"],
            edgecolor="black",
        )
        p_inc_ad_r = ax.bar(
            ind_ad_r + width / 2,
            df_smean_ad_r["increment_time"],
            width,
            bottom=np.array(df_smean_ad_r["read_time"]),
            color="r",
            hatch="//",
            label="increment",
            alpha=1,
            yerr=df_sstd_ad_r["increment_time"],
            edgecolor="black",
        )
        # p_sinc_mem_r = ax.bar(
        #     ind_mem_r + width / 2,
        #     df_smean_mem_r["increment_time"],
        #     width,
        #     bottom=np.array(df_smean_mem_r["read_time"]),
        #     color="b",
        #     hatch=r'--',
        #     label="increment",
        #     alpha=alpha,
        #     yerr=df_sstd_mem_r["increment_time"],
        #     edgecolor="black",
        # )
        # p_sinc_ad_r = ax.bar(
        #     ind_ad_r + 3*width / 2,
        #     df_smean_ad_r["increment_time"],
        #     width,
        #     bottom=np.array(df_smean_ad_r["read_time"]),
        #     color="b",
        #     hatch="..",
        #     label="increment",
        #     alpha=alpha,
        #     yerr=df_sstd_ad_r["increment_time"],
        #     edgecolor="black",
        # )
        p_write_mem_r = ax.bar(
            ind_mem_r -  width / 2,
            df_smean_mem_r["write_time"],
            width,
            bottom=np.array(df_smean_mem_r["read_time"])
            + np.array(df_smean_mem_r["increment_time"]),
            color="g",
            label="write",
            alpha=alpha,
            yerr=df_sstd_mem_r["write_time"],
            edgecolor="black",
        )
        p_write_ad_r = ax.bar(
            ind_ad_r + width / 2,
            df_smean_ad_r["write_time"],
            width,
            bottom=np.array(df_smean_ad_r["read_time"])
            + np.array(df_smean_ad_r["increment_time"]),
            color="g",
            hatch="//",
            label="write",
            alpha=alpha,
            yerr=df_sstd_ad_r["write_time"],
            edgecolor="black",
        )
        # p_swrite_mem_r = ax.bar(
        #     ind_mem_r + width / 2,
        #     df_smean_mem_r["write_time"],
        #     width,
        #     bottom=np.array(df_smean_mem_r["read_time"])
        #     + np.array(df_smean_mem_r["increment_time"]),
        #     color="g",
        #     hatch=r"--",
        #     label="write",
        #     alpha=alpha,
        #     yerr=df_sstd_mem_r["write_time"],
        #     edgecolor="black",
        # )

        # p_swrite_ad_r = ax.bar(
        #     ind_ad_r + 3*width / 2,
        #     df_smean_ad_r["write_time"],
        #     width,
        #     bottom=np.array(df_smean_ad_r["read_time"])
        #     + np.array(df_smean_ad_r["increment_time"]),
        #     color="g",
        #     hatch="..",
        #     label="write",
        #     alpha=alpha,
        #     yerr=df_sstd_ad_r["write_time"],
        #     edgecolor="black",
        # )

    r_read = mpatch.Patch(facecolor="r", alpha=alpha, label="load header")
    r_inc = mpatch.Patch(facecolor="r", alpha=alpha+0.2, label="read/increment")

    if spark:
        if not bb20 or '25cpu' in sys.argv[1] :
            ax.set_ylim(0, 140000)
        else:
            ax.set_ylim(0, 500000)
        r_read = mpatch.Patch(facecolor="r", alpha=alpha, label="convert data")
        r_inc = mpatch.Patch(facecolor="r", alpha=alpha+0.2, label="increment")

    r_write = mpatch.Patch(facecolor="g", alpha=alpha, label="write")
    r_mem = mpatch.Patch(facecolor="gray", alpha=alpha, label="Memory mode")
    r_ad = mpatch.Patch(
        facecolor="gray", alpha=alpha, hatch=r"///", label="App Direct mode"
    )
    r_smem = mpatch.Patch(facecolor="gray", alpha=alpha, hatch=r"---", label="Memory mode - Spark")
    r_sad = mpatch.Patch(
        facecolor="gray", alpha=alpha, hatch=r"...", label="App Direct mode - Spark"
    )

    ax.set_ylabel("Average total execution time (s)")
    ax.set_xlabel("Storage type")
    ax.set_xticks(ind)
    ax.set_xticklabels(labels)

    ax.legend(handles=[r_mem, r_ad, r_read, r_inc, r_write])

    plt.savefig("stacked-real-{}.pdf".format(sys.argv[2]))


spark = False
delim_whitespace = True
if "spark" in sys.argv[-1]:
    spark = True

all_files = glob(op.abspath(sys.argv[1]))
disks = [
    "tmpfs",
    "tmpfsAD",
    "optane",
    "optaneAD",
    "local",
    "localAD",
    "isilon",
    "isilonAD",
]
all_files.sort(
    key=lambda x: disks.index(
        op.basename(x).strip("spark-").split("_")[0].split("-")[1]
    )
)

df = pd.concat(
    (
        pd.read_csv(
            f,
            delim_whitespace='spark' not in f,
            names=["Task", "Start", "End", "File", "ThreadId"],
        ).assign(
            filename="{0}-{1}".format(i, op.basename(f)),
            disk=op.basename(f).split("_")[0].split("-")[-1],
        )
    )
    for i, f in enumerate(all_files)
)


df["Duration"] = df.End - df.Start
stacked_bar(df, spark)
