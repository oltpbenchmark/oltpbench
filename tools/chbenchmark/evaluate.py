#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Analyse output of OLTPBenchmark and compute CH-BenCHmark metrics

Usage:
    ./evaluate.py output.raw [--plot]

Options:
    -h --help   Show this help.
    --plot      Plot latency and throughput graphs.
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


class MetricsCalculator(object):
    """Loads a raw output file, parses it and computes CH-BenCHmark metrics.


    Parameters
    -----------

    data_path: str or None
        path to the raw data from a benchmark run.

    normalization_factors: list or None
        list of normalization factors corresponding to queries 1 to 22.
        If None, tryies to import norm_factor, if it is not found, doesn't
        normalize the OLAP dependencies.


    Attributes
    -----------
    data: pandas.DataFrame
        Normalized benchmark output data

    olap_groups: list of pandas.DataFrame
        Normalized data grouped by transaction type. Contains only OLAP
        queries.

    metrics: collections.namedtuple
        Contains CH-BenCHmark metrics. Consists of:

        tpmch: numpy.float64
            NewOrder transactions per minute.

        geometric_mean: numpy.float64
            Geometric mean of the mean normalized latencies of OLAP queries

        query_set_time: numpy.float64
            Sum of mean OLAP query latencies, i.e. how long will it take to
            execute every one of them.

        queries_per_hour: numpy.float64
            How many OLAP queries will be executed in an hour on average


    Example usage
    --------------

    >>> metrics_calc = MetricsCalculator("output.raw", norm_factors)
    >>> metrics_calc.metrics.tpmCH
    5065.7051694977554

    >>> print(metrics_calc)
    NewOrder Transactions per minute:        5065.7051695
    Geometric Mean of Latencies:             0.507683095117
    Query Set Time:                          16.4944362742
    Queries per Hour:                        4801.61908435

    >>> metrics_calc.plot()
        # plots latency and throughput diagrams
    """

    NEW_ORDER_ID = 2
    OLAP_QUERY_LOWER_ID = 7
    OLAP_QUERY_HIGHER_ID = 28
    NUMBER_QUERIES = OLAP_QUERY_HIGHER_ID - OLAP_QUERY_LOWER_ID + 1
    THROUGHPUT_BIN_SIZE = 5  # in seconds
    REPORTING_FORMAT = """
    NewOrder Transactions per minute: \t {tpmCH}
    Geometric Mean of Latencies: \t {geometric_mean}
    Query Set Time: \t\t\t {query_set_time}
    Queries per Hour: \t\t\t {queries_per_hour}
    """

    def __init__(self, data_path, norm_factors=None):
        self.data_path = data_path

        if norm_factors is None:
            self.norm_factors = self.get_norm_factors()
        else:
            self.norm_factors = norm_factors

        self.data = self.load_data()
        self.olap_groups = self.get_olap_groups(self.data)

        self.metrics = self.get_metrics()

    def __str__(self):
        """Uses REPORTING_FORMAT to create a string representation"""
        return self.REPORTING_FORMAT.format(
                            tpmCH=self.metrics.tpmCH,
                            geometric_mean=self.metrics.geometric_mean,
                            query_set_time=self.metrics.query_set_time,
                            queries_per_hour=self.metrics.queries_per_hour,
                            )

    def get_norm_factors(self):
        """Tryies to import norm_factors, returns a list. If norm_factors
        is not found, the list contains zeros."""

        try:
            from norm_factors import normalization_factors
            norm_factors = normalization_factors.values()
        except ImportError:
            print("Could not find norm_factors.py. "
                    "Not performing normalization on OLAP queries.")
            norm_factors = [0] * self.NUMBER_QUERIES
        return norm_factors

    def get_olap_groups(self, norm_data):
        """Returns an array of data grouped by transaction types. Only returns
        OLAP queries.
        """
        olap_data = norm_data[norm_data['transactiontype']
                                            >= self.OLAP_QUERY_LOWER_ID]
        return [groupdata
                    for _, groupdata in olap_data.groupby('transactiontype')]

    def get_tpmch(self):
        """Computes number of NewOrder transactions per minute metric.
         First argument is raw data, second is transaction id which corresponds
        to the NewOrder"""
        transaction_count = len(self.data[
                                self.data['transactiontype']
                                    == self.NEW_ORDER_ID])
        run_time = self.data['starttime'].max() - self.data['starttime'].min()
        return transaction_count / run_time * 60

    def get_geometric_mean(self):
        """Computes geometric mean of the latencies of the given data set.
        Usually works on normalized latencies of OLAP queries"""
        geometric_mean = 1
        for transaction_type in self.olap_groups:
            geometric_mean *= transaction_type['latency'].mean()
        return math.pow(geometric_mean, 1 / len(self.olap_groups))

    def get_query_set_time(self):
        """Computes the sum of mean normalized latencies of OLAP queries"""
        return sum(query['latency'].mean()
                for query in self.olap_groups)

    def get_queries_per_hour(self, query_set_time=None):
        """Using query set time, computes number of queries that can be
        theoretically executed in an hour. If none query set time is given,
        generates it first."""
        if query_set_time is None:
            query_set_time = self.get_query_set_time()
        return 60 * 60 / query_set_time * self.NUMBER_QUERIES

    def load_data(self):
        """Loads data from the path and  converts it to a more usable format
        """
        try:
            data = pd.read_csv(self.data_path)
        except IOError:
            print ("Could not read {}.".format(self.data_path))
            print(__doc__)
            sys.exit(-1)

        # prepare the data set
        data.columns = ['transactiontype', 'starttime',
                         'latency', 'workerid', 'phase']
        data['starttime'] -= data['starttime'].min()  # measure time from zero
        data['latency'] /= 10 ** 6  # convert latency to seconds
        data['diff'] = data['phase'].diff()  # phase switches are marker with 1

        norm_data = self.normalize_data(data, self.norm_factors)
        return norm_data

    def normalize_data(self, data, norm_factors):
        """Normalize the OLAP query latencies in data using given normalization
        factors. Returns an array with OLAP queries"""
        norm_data = []
        # set to 1 for NewOrder transactions
        data['neworder_count'] = np.where(data['transactiontype']
                                                == self.NEW_ORDER_ID, 1, 0)
        # cumulative sum of NewOrder transactions
        data['neworder_cum_sum'] = data['neworder_count'].cumsum()

        # from IPython import embed; embed()
        norm_vector = pd.Series(norm_factors,
                                index=range(self.OLAP_QUERY_LOWER_ID,
                                             self.OLAP_QUERY_HIGHER_ID + 1),
                                name="normfactors")

        norm_data = data.join(norm_vector, "transactiontype")
        norm_data['normfactors'].fillna(0, inplace=True)
        norm_data['latency'] -= norm_data['normfactors'] \
                                * norm_data['neworder_cum_sum']
        return norm_data

    def get_metrics(self):
        """Calculates CH-BenCHmark metrics and returns a named tuple"""

        ch_metrics = namedtuple("CHMetrics",
            ['tpmCH', 'geometric_mean', 'query_set_time', 'queries_per_hour'])

        query_set_time = self.get_query_set_time()
        return ch_metrics(self.get_tpmch(), self.get_geometric_mean(),
                query_set_time, self.get_queries_per_hour(query_set_time))

    def plot_latencies(self):
        """Plots the normalized data in a bar diagramm"""
        latencies = pd.Series(
            [query['latency'].mean() for query in self.olap_groups],
            index=range(1, self.NUMBER_QUERIES + 1))

        fig = p.figure()
        subplot = fig.add_subplot(111)
        latencies.plot(kind='bar')
        subplot.set_title("Normalized Latencies")
        subplot.set_ylabel("Seconds")
        subplot.set_xlabel("Query Number")
        p.show()

    def plot_throughput(self):
        """Plot transactional throughput"""

        transactional_data = self.data[self.data['transactiontype'] <
                                            self.OLAP_QUERY_LOWER_ID]
        bins = transactional_data['starttime'].max() //\
                        self.THROUGHPUT_BIN_SIZE + 1
        bins, edges = np.histogram(transactional_data['starttime'], bins=bins)
        throughput = bins / np.diff(edges)

        fig = p.figure()
        subplot = fig.add_subplot(111)
        subplot.plot(edges[:-1], throughput)
        subplot.set_ylabel("Transactions/second")
        subplot.set_xlabel("Seconds")
        p.show()

    def plot(self, latencies=True, throughput=True):
        """Wrapper method for latencies and throughput plotting"""
        if latencies:
            self.plot_latencies()
        if throughput:
            self.plot_throughput()


def main():
    """Main module. Loads the raw output, delegates metrics computing"""
    # handle arguments by hand since docopt is not a part of stdlib
    # and argparse is...suboptimal
    if not 2 <= len(sys.argv) <= 3:
        print("Wrong arguments.")
        print(__doc__)
        sys.exit(-1)
    if len(sys.argv) == 3 and sys.argv[2] == "--plot":
        plot_graphs = True
    else:
        plot_graphs = False
    first_argument = sys.argv[1]
    if first_argument == "-h" or first_argument == "--help":
        print (__doc__)
        sys.exit(0)

    metrics_calc = MetricsCalculator(first_argument)

    print(metrics_calc)

    if plot_graphs:
        if not HAS_PYLAB:
            print("Matplotlib is not installed. Skipping graph plotting.")
            sys.exit(-1)
        metrics_calc.plot()


if __name__ == "__main__":
    main()
