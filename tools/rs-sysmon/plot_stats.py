#!/usr/bin/env python
#-*- encoding: utf-8 -*-

"""usage: Plot the load statistics [-h] ds_path raw_path [columns [columns ...]]

positional arguments:
  ds_path     Path to the ds-stat loadstatistics
  raw_path    Path to OLTPBenchmark rawoutput
  columns     Ds-stat columns that should be ploted. It the label
              containsspaces, use " (default: ['load avg', 'memory usage'])

optional arguments:
  -h, --help  show this help message and exit
"""

from __future__ import print_function, division

import numpy as np
import pandas as pd
import pylab as p
import sys
import argparse

def prepare_data(ds_path, raw_path):
    """Loads ds stats and oltpbench output, prepares data. 
    Returns a tuple with filtered ds data and raw data pandas DFs"""
    if pd.version.short_version.split(".") > (0, 11, 0):
        kwargs = {"na_filter": False}
    else:
        kwargs = {}
    ds_stats = pd.read_csv(ds_path, skiprows=[1],
                            header=0, **kwargs)
    raw_data = pd.read_csv(raw_path, **kwargs)

    raw_data.columns = ['transactiontype', 'start', 'latency',
                        'worker', 'phase']
    filtered_data = ds_stats[(ds_stats['epoch'] > raw_data['start'].min()) & 
        (ds_stats['epoch'] < raw_data['start'].max())]

    raw_data['start'] -= raw_data['start'].min()
    filtered_data['epoch'] -= filtered_data['epoch'].min()
    raw_data.set_index('start', inplace=True, drop=False)
    filtered_data.set_index('epoch', inplace=True, drop=False)

    return filtered_data, raw_data

def prepare_throughput_axis(ax, raw_data):
    """Plots throughput on the given matplotlib axis"""
    bin_number = raw_data['start'].max() // 5
    bins, edges = np.histogram(raw_data['start'],
                                bins=bin_number)
    throughput = bins / np.diff(edges)

    ax.plot(edges[:-1], throughput, label='Throughput')
    ax.set_ylabel("Transactions / second")
    ax.set_xlabel("Seconds")

def plot_stat(filtered_data, raw_data, stat_name, label=None):
    """Plot throughput alongside the defined stat. If label is specified,
    use it as the label for the stat"""

    fig = p.figure()
    ax1 = fig.add_subplot(111)
    prepare_throughput_axis(ax1, raw_data)
    
    ax2 = ax1.twinx()
    if label is None:
        label = stat_name.title()
    filtered_data[stat_name].plot(ax=ax2, color='red', label='Avg Load')
    ax2.set_ylabel(label)
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(handles1 + handles2, labels1 + labels2, 4)
    p.show()


def main():
    parser = argparse.ArgumentParser("Plot the load statistics",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("ds_path", help='Path to the ds-stat load'
        'statistics')
    parser.add_argument("raw_path", help='Path to OLTPBenchmark raw'
        'output')
    parser.add_argument("columns", nargs='*', default=['load avg',
                                                         'memory usage'],
        help='Ds-stat columns that should be ploted. It the label contains'
                'spaces, use \"')
    args = parser.parse_args()

    filtered_data, raw_data = prepare_data(args.ds_path, args.raw_path)
    for stat_name in args.columns:
        plot_stat(filtered_data, raw_data, stat_name)


if __name__ == "__main__":
    main()