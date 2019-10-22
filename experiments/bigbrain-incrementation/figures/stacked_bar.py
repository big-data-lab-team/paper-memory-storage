#!/usr/bin/env python3

import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatch
from glob import glob
from os import path as op
import numpy as np


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

    labels = ["tmpfs", "Optane", "local disk", "Isilon"]
    ind = np.arange(len(labels))
    ind_mem = np.delete(ind, 1)

    r_task = "read_file"
    i_task = "increment_file"
    w_task = "write_file"

    dfp = df[~df["filename"].str.contains('spark')]
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
        df_smean, df_sstd = get_mean_std(dfs, "load_img", "increment_data", "save_incremented") 

        df_mean_mem_r = prep_mem_df(df_mean, "real")
        df_mean_mem_em = prep_mem_df(df_mean, "em")
        df_std_mem_r = prep_mem_df(df_std, "real")
        df_std_mem_em = prep_mem_df(df_std, "em")

        df_smean_mem_r = prep_mem_df(df_smean, "real")
        df_smean_mem_em = prep_mem_df(df_smean, "em")
        df_sstd_mem_r = prep_mem_df(df_sstd, "real")
        df_sstd_mem_em = prep_mem_df(df_sstd, "em")

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
        ind_mem_r = np.delete(ind_mem, 1)
        ind_ad_r = np.delete(ind, [0, 2])
        df_mean_ad_r = prep_ad_df(df_mean, "real")
        df_mean_ad_em = prep_ad_df(df_mean, "em")
        df_std_ad_r = prep_ad_df(df_std, "real")
        df_std_ad_em = prep_ad_df(df_std, "em")

        df_smean_ad_r = prep_ad_df(df_smean, "real")
        df_smean_ad_em = prep_ad_df(df_smean, "em")
        df_sstd_ad_r = prep_ad_df(df_sstd, "real")
        df_sstd_ad_em = prep_ad_df(df_sstd, "em")

    width = 0.2

    alpha = 0.4

    figure, ax = plt.subplots()

    if not spark:
        p_read_mem = ax.bar(
            ind_mem - width / 2,
            df_mean_mem["read_time"],
            width,
            color="r",
            label="read",
            alpha=alpha,
            yerr=df_std_mem["read_time"],
            edgecolor="black",
        )
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
        p_inc_mem = ax.bar(
            ind_mem - width / 2,
            df_mean_mem["increment_time"],
            width,
            bottom=np.array(df_mean_mem["read_time"]),
            color="b",
            label="increment",
            alpha=alpha,
            yerr=df_std_mem["increment_time"],
            edgecolor="black",
        )
        p_inc_ad = ax.bar(
            ind + width / 2,
            df_mean_ad["increment_time"],
            width,
            bottom=np.array(df_mean_ad["read_time"]),
            color="b",
            hatch="//",
            alpha=alpha,
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

    else:
        ### EMULATED
        p_read_mem_em = ax.bar(
            ind_mem - 3*width / 2,
            df_mean_mem_em["read_time"],
            width,
            color="r",
            label="read",
            alpha=alpha,
            yerr=df_std_mem_em["read_time"],
            edgecolor="black",
        )
        '''p_read_ad_em = ax.bar(
            ind - width / 2,
            df_mean_ad_em["read_time"],
            width,
            color="r",
            hatch="//",
            alpha=alpha,
            yerr=df_std_ad_em["read_time"],
            edgecolor="black",
        )'''
        p_sread_mem_em = ax.bar(
            ind_mem + width / 2,
            df_smean_mem_em["read_time"],
            width,
            color="r",
            hatch="--",
            label="read",
            alpha=alpha,
            yerr=df_sstd_mem_em["read_time"],
            edgecolor="black",
        )
        p_sread_ad_em = ax.bar(
            ind + 3*width / 2,
            df_smean_ad_em["read_time"],
            width,
            color="r",
            hatch="..",
            alpha=alpha,
            yerr=df_sstd_ad_em["read_time"],
            edgecolor="black",
        )
        p_inc_mem_em = ax.bar(
            ind_mem - 3*width / 2,
            df_mean_mem_em["increment_time"],
            width,
            bottom=np.array(df_mean_mem_em["read_time"]),
            color="b",
            label="increment",
            alpha=alpha,
            yerr=df_std_mem_em["increment_time"],
            edgecolor="black",
        )
        '''p_inc_ad_em = ax.bar(
            ind - width / 2,
            df_mean_ad_em["increment_time"],
            width,
            bottom=np.array(df_mean_ad_em["read_time"]),
            color="b",
            hatch="//",
            alpha=alpha,
            yerr=df_std_ad_em["increment_time"],
            edgecolor="black",
        )'''
        p_sinc_mem_em = ax.bar(
            ind_mem + width / 2,
            df_smean_mem_em["increment_time"],
            width,
            bottom=np.array(df_smean_mem_em["read_time"]),
            color="b",
            hatch="--",
            label="increment",
            alpha=alpha,
            yerr=df_sstd_mem_em["increment_time"],
            edgecolor="black",
        )
        p_sinc_ad_em = ax.bar(
            ind + 3*width / 2,
            df_smean_ad_em["increment_time"],
            width,
            bottom=np.array(df_smean_ad_em["read_time"]),
            color="b",
            hatch="..",
            alpha=alpha,
            yerr=df_sstd_ad_em["increment_time"],
            edgecolor="black",
        )
        p_write_mem_em = ax.bar(
            ind_mem - 3*width / 2,
            df_mean_mem_em["write_time"],
            width,
            bottom=np.array(df_mean_mem_em["read_time"])
            + np.array(df_mean_mem_em["increment_time"]),
            color="g",
            label="write",
            alpha=alpha,
            yerr=df_std_mem_em["write_time"],
            edgecolor="black",
        )
        '''p_write_ad_em = ax.bar(
            ind - width / 2,
            df_mean_ad_em["write_time"],
            width,
            bottom=np.array(df_mean_ad_em["read_time"])
            + np.array(df_mean_ad_em["increment_time"]),
            color="g",
            hatch="//",
            alpha=alpha,
            yerr=df_std_ad_em["write_time"],
            edgecolor="black",
        )'''
        p_swrite_mem_em = ax.bar(
            ind_mem + width / 2,
            df_smean_mem_em["write_time"],
            width,
            bottom=np.array(df_smean_mem_em["read_time"])
            + np.array(df_smean_mem_em["increment_time"]),
            color="g",
            hatch="--",
            label="write",
            alpha=alpha,
            yerr=df_sstd_mem_em["write_time"],
            edgecolor="black",
        )
        p_swrite_ad_em = ax.bar(
            ind + 3 * width / 2,
            df_smean_ad_em["write_time"],
            width,
            bottom=np.array(df_smean_ad_em["read_time"])
            + np.array(df_smean_ad_em["increment_time"]),
            color="g",
            hatch="..",
            alpha=alpha,
            yerr=df_sstd_ad_em["write_time"],
            edgecolor="black",
        )

        r_read = mpatch.Patch(facecolor="r", alpha=alpha, label="read")
        r_inc = mpatch.Patch(facecolor="b", alpha=alpha, label="increment")
        r_write = mpatch.Patch(facecolor="g", alpha=alpha, label="write")
        r_mem_em = mpatch.Patch(
            facecolor="gray", alpha=alpha, label="Memory mode - GNU Parallel"
        )
        r_smem_em = mpatch.Patch(
            facecolor="gray", alpha=alpha, hatch=r"///", label="Memory mode - Spark"
        )
        r_ad_em = mpatch.Patch(
            facecolor="gray", alpha=alpha, hatch=r"---", label="App Direct mode - GNU Parallel"
        )
        r_sad_em = mpatch.Patch(
            facecolor="gray", alpha=alpha, hatch=r"...", label="App Direct mode - Spark"
        )

        ax.set_ylabel("Average total execution time (s)")
        ax.set_xlabel("Storage type")
        ax.set_xticks(ind)
        ax.set_xticklabels(labels)
        ax.set_ylim([0, 300000])

        ax.legend(handles=[r_mem_em, r_ad_em, r_smem_em, r_sad_em, r_read, r_inc, r_write])
        plt.savefig("stacked-emulated-{}.pdf".format(sys.argv[2]))

        figure, ax = plt.subplots()
        #### REAL
        p_read_mem_r = ax.bar(
            ind_mem_r - 3*width / 2,
            df_mean_mem_r["read_time"],
            width,
            color="r",
            label="read",
            alpha=alpha,
            yerr=df_std_mem_r["read_time"],
            edgecolor="black",
        )
        '''p_read_ad_r = ax.bar(
            ind_ad_r - width / 2,
            df_mean_ad_r["read_time"],
            width,
            color="r",
            hatch="//",
            label="read",
            alpha=alpha,
            yerr=df_std_ad_r["read_time"],
            edgecolor="black",
        )'''
        p_sread_mem_r = ax.bar(
            ind_mem_r + width / 2,
            df_smean_mem_r["read_time"],
            width,
            color="r",
            hatch="--",
            label="read",
            alpha=alpha,
            yerr=df_sstd_mem_r["read_time"],
            edgecolor="black",
        )
        p_read_ad_r = ax.bar(
            ind_ad_r + 3*width / 2,
            df_smean_ad_r["read_time"],
            width,
            color="r",
            hatch="..",
            label="read",
            alpha=alpha,
            yerr=df_sstd_ad_r["read_time"],
            edgecolor="black",
        )
        p_inc_mem_r = ax.bar(
            ind_mem_r - 3*width / 2,
            df_mean_mem_r["increment_time"],
            width,
            bottom=np.array(df_mean_mem_r["read_time"]),
            color="b",
            label="increment",
            alpha=alpha,
            yerr=df_std_mem_r["increment_time"],
            edgecolor="black",
        )
        '''p_inc_ad_r = ax.bar(
            ind_ad_r - width / 2,
            df_mean_ad_r["increment_time"],
            width,
            bottom=np.array(df_mean_ad_r["read_time"]),
            color="b",
            hatch="//",
            label="increment",
            alpha=alpha,
            yerr=df_std_ad_r["increment_time"],
            edgecolor="black",
        )'''
        p_sinc_mem_r = ax.bar(
            ind_mem_r + width / 2,
            df_smean_mem_r["increment_time"],
            width,
            bottom=np.array(df_smean_mem_r["read_time"]),
            color="b",
            hatch=r'--',
            label="increment",
            alpha=alpha,
            yerr=df_sstd_mem_r["increment_time"],
            edgecolor="black",
        )
        p_sinc_ad_r = ax.bar(
            ind_ad_r + 3*width / 2,
            df_smean_ad_r["increment_time"],
            width,
            bottom=np.array(df_smean_ad_r["read_time"]),
            color="b",
            hatch="..",
            label="increment",
            alpha=alpha,
            yerr=df_sstd_ad_r["increment_time"],
            edgecolor="black",
        )
        p_write_mem_r = ax.bar(
            ind_mem_r - 3 * width / 2,
            df_mean_mem_r["write_time"],
            width,
            bottom=np.array(df_mean_mem_r["read_time"])
            + np.array(df_mean_mem_r["increment_time"]),
            color="g",
            label="write",
            alpha=alpha,
            yerr=df_std_mem_r["write_time"],
            edgecolor="black",
        )
        '''p_write_ad_r = ax.bar(
            ind_ad_r - width / 2,
            df_mean_ad_r["write_time"],
            width,
            bottom=np.array(df_mean_ad_r["read_time"])
            + np.array(df_mean_ad_r["increment_time"]),
            color="g",
            hatch="//",
            label="write",
            alpha=alpha,
            yerr=df_std_ad_r["write_time"],
            edgecolor="black",
        )'''
        p_swrite_mem_r = ax.bar(
            ind_mem_r + width / 2,
            df_smean_mem_r["write_time"],
            width,
            bottom=np.array(df_smean_mem_r["read_time"])
            + np.array(df_smean_mem_r["increment_time"]),
            color="g",
            hatch=r"--",
            label="write",
            alpha=alpha,
            yerr=df_sstd_mem_r["write_time"],
            edgecolor="black",
        )

        p_swrite_ad_r = ax.bar(
            ind_ad_r + 3*width / 2,
            df_smean_ad_r["write_time"],
            width,
            bottom=np.array(df_smean_ad_r["read_time"])
            + np.array(df_smean_ad_r["increment_time"]),
            color="g",
            hatch="..",
            label="write",
            alpha=alpha,
            yerr=df_sstd_ad_r["write_time"],
            edgecolor="black",
        )

    r_read = mpatch.Patch(facecolor="r", alpha=alpha, label="read")
    r_inc = mpatch.Patch(facecolor="b", alpha=alpha, label="increment")
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

    if not spark:
        ax.legend(handles=[r_mem, r_ad, r_read, r_inc, r_write])
    else:
        ax.set_ylim([0, 300000])
        r_mem = mpatch.Patch(facecolor="gray", alpha=alpha, label="Memory mode - GNU Parallel")
        r_ad = mpatch.Patch(
            facecolor="gray", alpha=alpha, hatch=r"///", label="App Direct mode - GNU Parallel"
        )
        ax.legend(handles=[r_mem, r_ad, r_smem, r_sad, r_read, r_inc, r_write])

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
