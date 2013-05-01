#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Analyse output of OLTPBenchmark and compute CH-BenCHmark metrics

Usage:
    ./evaluate.py output.raw [--graphs]

Options:
    -h --help   Show this help.
    --graphs    Plot latency and throughput graphs.
"""

from __future__ import print_function, division

import sys
import math
from collections import namedtuple
import numpy as np
import pandas as pd

try:
    import pylab as p
    HAS_PYLAB = True
except ImportError:
    HAS_PYLAB = False

NEW_ORDER_ID = 2
OLAP_QUERY_LOWER_ID = 7
OLAP_QUERY_HIGHER_ID = 28
NUMBER_QUERIES = OLAP_QUERY_HIGHER_ID - OLAP_QUERY_LOWER_ID + 1
THROUGHPUT_BIN_SIZE = 5  # in seconds
REPORTING_FORMAT = """
NewOrder Transactions per minute: \t {tpmCH}
Geometric Mean of Latencies: \t\t {geometric_mean}
Query Set Time: \t\t\t {query_set_time}
Queries per Hour: \t\t\t {queries_per_hour}
"""


def get_norm_factors():
    """Tryies to import norm_factors, returns a dictionary. If norm_factors
    is not found, dictionary contains zeros"""

    try:
        from norm_factors import normalization_factors
        norm_factors = normalization_factors
    except ImportError:
        print("Could not find norm_factors.py. "
                "Not performing normalization on OLAP queries.")
        norm_factors = {x: 0 for x in range(1, 23)}
    return norm_factors


def get_olap_groups(norm_data):
    """Returns an array of data grouped by transaction types. Only returns
    OLAP queries.
    """
    olap_data = norm_data[norm_data['transactiontype'] >= OLAP_QUERY_LOWER_ID]
    return [groupdata
                for _, groupdata in olap_data.groupby('transactiontype')]


def get_tpmch(data):
    """Computes number of NewOrder transactions per minute metric.
     First argument is raw data, second is transaction id which corresponds
    to the NewOrder"""
    transaction_count = len(data[data['transactiontype'] == NEW_ORDER_ID])
    run_time = data['starttime'].max() - data['starttime'].min()
    return transaction_count / run_time * 60


def get_geometric_mean(norm_data):
    """Computes geometric mean of the latencies of the given data set.
    Usually works on normalized latencies of OLAP queries"""
    geometric_mean = 1
    olap_groups = get_olap_groups(norm_data)
    for transaction_type in olap_groups:
        geometric_mean *= transaction_type['latency'].mean()
    return math.pow(geometric_mean, 1 / len(olap_groups))


def get_query_set_time(norm_data):
    """Computes the sum of mean normalized latencies of OLAP queries"""
    return sum(query['latency'].mean()
            for query in get_olap_groups(norm_data))


def get_queries_per_hour(query_set_time):
    """Using query set time, computes number of queries that can be
    theoretically executed in an hour"""
    return 60 * 60 / query_set_time * NUMBER_QUERIES


def load_data(data_path):
    """Loads data from the path, converts it to a more usable format
    and normalizes it according using normalization factors"""
    try:
        data = pd.read_csv(data_path)
    except IOError:
        print ("Could not read {}.".format(data_path))
        print(__doc__)
        sys.exit(-1)

    # prepare the data set
    data.columns = ['transactiontype', 'starttime', 'latency', 'workerid',
                                                                     'phase']
    data['starttime'] -= data['starttime'].min()  # start the time from zero
    data['latency'] /= 10 ** 6  # convert latency to seconds
    data['diff'] = data['phase'].diff()  # phase switches are marker with 1

    norm_data = normalize_data(data, get_norm_factors())
    return norm_data


def normalize_data(data, norm_factors):
    """Normalize the OLAP query latencies in data using given normalization
    factors. Returns an array with OLAP queries"""
    norm_data = []
    # set to 1 for NewOrder transactions
    data['neworder_count'] = np.where(data['transactiontype'] == NEW_ORDER_ID,
                                                                         1, 0)
    # cumulative sum of NewOrder transactions
    data['neworder_cum_sum'] = data['neworder_count'].cumsum()

    # from IPython import embed; embed()
    norm_vector = pd.Series(norm_factors.values(),
                            index=range(OLAP_QUERY_LOWER_ID,
                                         OLAP_QUERY_HIGHER_ID + 1),
                            name="normfactors")

    norm_data = data.join(norm_vector, "transactiontype")
    norm_data['normfactors'].fillna(0, inplace=True)
    norm_data['latency'] -= norm_data['normfactors'] \
                            * norm_data['neworder_cum_sum']
    return norm_data


def get_metrics(data):
    """Takes normalized data and computes CH-BenCHmark metrics.
    Returns a named tuple chmetrics
    """

    ch_metric_tuple = namedtuple(
             'CHBenCHmark_Metric',
             ['tpmCH', 'geometric_mean', 'query_set_time', 'queries_per_hour'])

    query_set_time = get_query_set_time(data)

    return ch_metric_tuple(get_tpmch(data), get_geometric_mean(data),
                        query_set_time, get_queries_per_hour(query_set_time))


def plot_latencies(norm_data):
    """Plots the normalized data in a bar diagramm"""
    latencies = pd.Series(
        [query['latency'].mean() for query in get_olap_groups(norm_data)],
        index=range(1, NUMBER_QUERIES + 1))

    fig = p.figure()
    subplot = fig.add_subplot(111)
    latencies.plot(kind='bar')
    subplot.set_title("Normalized Latencies")
    subplot.set_ylabel("Seconds")
    subplot.set_xlabel("Query Number")
    p.show()


def plot_throughput(data):
    """Plot transactional throughput"""

    transactional_data = data[data['transactiontype'] < OLAP_QUERY_LOWER_ID]
    bins = transactional_data['starttime'].max() // THROUGHPUT_BIN_SIZE + 1
    bins, edges = np.histogram(transactional_data['starttime'], bins=bins)
    throughput = bins / np.diff(edges)

    fig = p.figure()
    subplot = fig.add_subplot(111)
    subplot.plot(edges[:-1], throughput)
    subplot.set_ylabel("Transactions/second")
    subplot.set_xlabel("Seconds")
    p.show()


def main():
    """Main module. Loads the raw output, delegates metrics computing"""
    # handle arguments by hand since docopt is not a part of stdlib
    # and argparse is...suboptimal
    if not 2 <= len(sys.argv) <= 3:
        print("Wrong arguments.")
        print(__doc__)
        sys.exit(-1)
    if len(sys.argv) == 3 and sys.argv[2] == "--graphs":
        plot_graphs = True
    else:
        plot_graphs = False
    first_argument = sys.argv[1]
    if first_argument == "-h" or first_argument == "--help":
        print (__doc__)
        sys.exit(0)

    data = load_data(first_argument)

    ch_metrics = get_metrics(data)
    print(REPORTING_FORMAT.format(
            tpmCH=ch_metrics.tpmCH,
            geometric_mean=ch_metrics.geometric_mean,
            query_set_time=ch_metrics.query_set_time,
            queries_per_hour=ch_metrics.queries_per_hour,
            ))

    if plot_graphs:
        if not HAS_PYLAB:
            print("Matplotlib is not installed. Skipping graph plotting.")
            sys.exit(-1)
        print("Plotting OLAP latency graph")
        plot_latencies(data)
        print("Plotting throughput graph")
        plot_throughput(data)


if __name__ == "__main__":
    main()
