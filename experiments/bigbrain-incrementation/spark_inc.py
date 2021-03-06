#!/usr/bin/env python3

from pyspark import SparkContext, SparkConf
from io import BytesIO
from time import sleep, time
from glob import glob
import os
import uuid
import shutil
import numpy as np
import nibabel as nib
import argparse
import subprocess


def write_bench(name, start_time, end_time, filename, executor,
                benchmark_dir=None, benchmark_file=None):
    """Write benchmarks to file.

    Keyword arguments:
    name -- name of task benchmarked
    start_time -- task execution start time (in seconds)
    end_time -- task execution end time (in seconds)
    node -- hostname ID of node which executed task
    filename -- filename of image task processed.
    executor -- process ID
    benchmark_dir -- directory to save benchmarks to. Not required if
                     file provided
    benchmark_file -- file to append benchmarks to.

    Returns: filename of benchmark file
    """

    if not benchmark_file:
        assert benchmark_dir, 'benchmark_dir parameter has not been defined.'

        try:
            os.makedirs(benchmark_dir)
        except Exception as e:
            pass

        benchmark_file = os.path.join(
                benchmark_dir,
                "bench-{}.txt".format(str(uuid.uuid1()))
                )

    with open(benchmark_file, 'a+') as f:
        f.write('{0},{1},{2},{3},{4}\n'.format(name, start_time, end_time,
                                                   filename, executor))

    return benchmark_file


def read_img(filename, data, benchmark, start, bench_dir=None):
    """Convert image in-memory binary data into Numpy array.
    For in-memory Spark only.

    Keyword arguments:
    filename -- filename for which data belongs to
    data -- binary image data
    benchmark -- boolean value that specifies whether to benchmark the task
    start -- start time of the driver (in seconds)
    benchmark_dir -- benchmark directory required if benchmarking is requested.
                     (default None)

    Returns: Tuple containing image data and metadata
    """
    start_time = time() - start

    print('Reading image')
    # load binary data into Nibabel
    if data is not None:
        fh = nib.FileHolder(fileobj=BytesIO(data))
        im = nib.Nifti1Image.from_file_map({'header': fh, 'image': fh})
    else:
        im = nib.load(filename)

    data = np.asanyarray(im.dataobj)

    end_time = time() - start

    bn = os.path.basename(filename)

    bench_file = None
    if benchmark:
        bench_file = write_bench('load_img', start_time, end_time, bn,
                                 os.getpid(), benchmark_dir=bench_dir)

    return (os.getpid(), filename, im, bench_file)


def increment_data(idx, filename, im, delay, benchmark, start,
                   iteration=0, bench_file=None):
    """Increment image data by 1

    Keyword arguments:
    idx -- image index within the RDD (Or pid if in-memory)
    filename -- image filename
    im -- Nibabel image (can be None w/ CLI-based processing)
    delay -- task duration (or sleep time) in seconds
    benchmark -- boolean representing whether task should be benchmarked
    start -- start time of driver
    iteration -- the current iteration value (default 0)
    bench_file -- the filepath to save benchmarks to

    Returns: Tuple containing incremented data and associated metadata
    """
    start_time = time() - start

    if benchmark and os.path.isdir(bench_file):
        bench_dir = bench_file
        try:
            os.makedirs(bench_dir)
        except Exception as e:
            pass

        bench_file = os.path.join(
                        bench_dir,
                        "bench-{}.txt".format(str(uuid.uuid1()))
                     )

    print("Incrementing data in memory")
    data = np.asanyarray(im.dataobj) + 1
    im = nib.Nifti1Image(data, im.affine, im.header)
    sleep(delay)

    end_time = time() - start

    bench_dir = None
    if benchmark:
        bn = os.path.basename(filename)
        write_bench('increment_data', start_time, end_time,
                    bn,
                    os.getpid(), bench_dir, bench_file)

    return (idx, filename, im, bench_file, iteration + 1)


def save_incremented(idx, filename, im, benchmark, start,
                     output_dir, iterations, bench_file=None):
    """Save final data to output directory

    Keyword arguments:
    idx -- index of data in RDD (cli) or initial hostname (in-mem)
    filename -- image filename
    im -- Nibabel image (can be None if processed using CLI)
    benchmark -- boolean denoting whether to benchmark task
    start -- driver start time (in seconds)
    output_dir -- directory to write final data to
    iterations -- number of iterations that resulted in final data
    bench_file -- the file to append benchmarks to (default None)

    Returns: Tuple containing final output filename
    """

    start_time = time() - start

    bn = os.path.basename(filename) #.replace('inc-', '')
    out_fn = os.path.join(output_dir, bn) #'inc{0}-{1}'.format(iterations, bn))

    nib.save(im, out_fn)

    end_time = time() - start

    if benchmark:
        bench_dir = None
        if os.path.isdir(bench_file):
            bench_dir = bench_file
            bench_file = None
        write_bench('save_incremented', start_time, end_time, bn, os.getpid(),
                    benchmark_dir=bench_dir, benchmark_file=bench_file)

    return (out_fn, 'SUCCESS')


def main():

    start = time()

    parser = argparse.ArgumentParser(description="BigBrain incrementation")
    parser.add_argument('bb_dir', type=str,
                        help=('The folder containing BigBrain NIfTI images'
                              '(local fs only)'))
    parser.add_argument('output_dir', type=str,
                        help=('the folder to save incremented images to'
                              '(local fs only)'))
    parser.add_argument('iterations', type=int, help='number of iterations')
    parser.add_argument('--delay', type=float, default=0,
                        help='task duration time (in s)')
    parser.add_argument('--benchmark', action='store_true',
                        help='benchmark results')
    parser.add_argument('--emulate', action='store_true', help='emulate missing' +
                                                                'images')

    args = parser.parse_args()

    conf = SparkConf().setAppName("Spark BigBrain incrementation")
    sc = SparkContext.getOrCreate(conf=conf)

    delay = args.delay

    output_dir = os.path.abspath(args.output_dir)
    try:
        os.makedirs(output_dir)
    except Exception as e:
        pass

    app_uuid = str(uuid.uuid1())
    print('Application id: ', app_uuid)
    benchmark_dir = os.path.join(output_dir,
                                 'benchmarks-{}'.format(app_uuid))
    try:
        print('Creating benchmark directory at ', benchmark_dir)
        os.makedirs(benchmark_dir)
    except Exception as e:
        pass

    if not args.emulate:
        # read binary data stored in folder and create an RDD from it
        imRDD = sc.binaryFiles('file://' + os.path.abspath(args.bb_dir) + '/bigbrain*')
        imRDD = imRDD.map(lambda x: read_img(x[0], x[1],
                                             args.benchmark,
                                             start,
                                             bench_dir=benchmark_dir))
    else:
        available_files = glob(os.path.abspath(args.bb_dir + '/bigbrain*'))
        file_list = (available_files * 1000)[:1000]
        imRDD = sc.parallelize(file_list)
        imRDD = imRDD.map(lambda x: read_img(x, None,
                                             args.benchmark,
                                             start,
                                             bench_dir=benchmark_dir))

    for i in range(args.iterations):
        imRDD = imRDD.map(lambda x: increment_data(x[0], x[1], x[2],
                                                   delay, args.benchmark,
                                                   start,
                                                   bench_file=x[3]))

    imRDD.map(lambda x: save_incremented(x[0], x[1], x[2],
                                         args.benchmark, start,
                                         output_dir,
                                         args.iterations, x[3]),
              preservesPartitioning=True).collect()

    end = time() - start

    if args.benchmark:
        fname = 'benchmark-{}.txt'.format(app_uuid)
        benchmark_file = os.path.join(output_dir, fname)
        write_bench('driver_program', 0, end,
                    output_dir, 'allfiles', os.getpid(),
                    benchmark_file=benchmark_file)

        with open(benchmark_file, 'a+') as bench:
            for b in os.listdir(benchmark_dir):
                with open(os.path.join(benchmark_dir, b), 'r') as f:
                    bench.write(f.read())

        shutil.rmtree(benchmark_dir)


if __name__ == '__main__':
    main()
