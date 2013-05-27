#!/usr/bin/env python
#-*- encoding: utf-8 -*-
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

def plot_avg_load(filtered_data, raw_data):
    """Plot throughput and avg cpu load"""

    fig = p.figure()
    ax1 = fig.add_subplot(111)

    bin_number = raw_data['start'].max() // 5
    bins, edges = np.histogram(raw_data['start'],
                                bins=bin_number)
    throughput = bins / np.diff(edges)

    ax1.plot(edges[:-1], throughput, label='Throughput')
    ax1.set_ylabel("Transactions / second")
    ax1.set_xlabel("Seconds")

    ax2 = ax1.twinx()
    filtered_data['load avg'].plot(ax=ax2, color='red', label='Avg Load')
    ax2.set_ylabel("Avg Load (s)")
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(handles1 + handles2, labels1 + labels2, 4)
    p.show()


def main():
    parser = argparse.ArgumentParser("Plot the load statistics")
    parser.add_argument("ds_path", help='Path to the ds-stat load'
        'statistics')
    parser.add_argument("raw_path", help='Path to OLTPBenchmark raw'
        'output')
    args = parser.parse_args()

    filtered_data, raw_data = prepare_data(args.ds_path, args.raw_path)
    plot_avg_load(filtered_data, raw_data)


if __name__ == "__main__":
    main()