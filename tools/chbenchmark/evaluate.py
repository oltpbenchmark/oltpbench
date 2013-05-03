#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Analyse output of OLTPBenchmark and compute CH-BenCHmark metrics

Usage:
    ./evaluate.py output.raw scale_factor [--plot]

Options:
    -h --help   Show this help.
    --plot      Plot latency and throughput graphs.
"""

from __future__ import print_function, division

import re
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

# container for CH metrics. Has to be located at the module level for it
# to be pickled
CH_METRICS = namedtuple("CH_METRICS",
    ['tpmCH', 'geometric_mean', 'query_set_time', 'queries_per_hour',
        'effective_queries_per_hour'])


class MetricsCalculator(object):
    """Loads a raw output file, parses it and computes CH-BenCHmark metrics.


    Parameters
    -----------

    data_path: str
        path to the raw data from a benchmark run.

    scale_factor: float
        Size of the initial data size measured in TPC-C warehouses.
        Corresponds to the scale_factor parameter of TPC-C.

    normalization_factors: list, dict or None
        list of normalization factors corresponding to queries 1 to 22.
        If None, uses NORMALIZATION_FACTORS class constant.
        Default: None

    keep_data: boolean
        If true, keeps the data DataFrame as attribute. Turn off to save
        the memory.
        Default: True


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
            How many OLAP queries will be executed in an hour on average.
            Computed as 3600 / query_set_time * number_of_queries

        effective_queries_per_hour: numpy.float64
            How many queries can be executed in total, i.e. by all analytical
            workers combined.
            Computed as queries_per_hour * number_of_workers


    Example usage
    --------------

    >>> metrics_calc = MetricsCalculator("output.raw", norm_factors)
    >>> metrics_calc.metrics.tpmCH
    5065.7051694977554

    >>> print(metrics_calc)

    NewOrder Transactions per minute:    4431.99420593
    Geometric Mean of Latencies:         1.04456824704
    Query Set Time:                      30.9542381403
    Queries per Hour:                    2558.6157101
    Effective Queries per Hour:          5117.2314202

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
    Effective Queries per Hour: \t {effective_queries_per_hour}
    """
    NORMALIZATION_FACTORS = {1: 4.3080787947283295e-05,
                             2: 1.3511842916930463e-06,
                             3: 1.4061316186722519e-05,
                             4: 5.0505764012875709e-05,
                             5: 8.2386818827363912e-05,
                             6: 4.76976420878031e-05,
                             7: 6.1194069039292685e-05,
                             8: 1.8605473268914576e-05,
                             9: 2.9757274683884159e-05,
                             10: 4.9739834035716385e-05,
                             11: 2.6012010035246955e-06,
                             12: 4.8384581137254573e-05,
                             13: 9.1957660424126757e-06,
                             14: 3.9511578568506352e-05,
                             15: 7.8781204811696957e-05,
                             16: 6.929053496878889e-07,
                             17: 3.229470913104759e-05,
                             18: 9.6369692588189949e-05,
                             19: 1.3354042769484055e-05,
                             20: 2.3403405125265545e-05,
                             21: 4.632092053642501e-05,
                             22: 1.2041461648844624e-05
                             }

    def __init__(self, data_path, scale_factor, norm_factors=None,
                                                    keep_data=True):
        self.data_path = data_path

        self.scale_factor = scale_factor
        self.norm_factors = self.get_norm_factors(norm_factors)

        self.data = self.load_data()
        self.olap_groups = self.get_olap_groups(self.data)

        self.metrics = self.get_metrics()

        if not keep_data:
            del self.data

    def __str__(self):
        """Uses REPORTING_FORMAT to create a string representation"""
        return self.REPORTING_FORMAT.format(
            tpmCH=self.metrics.tpmCH,
            geometric_mean=self.metrics.geometric_mean,
            query_set_time=self.metrics.query_set_time,
            queries_per_hour=self.metrics.queries_per_hour,
            effective_queries_per_hour=self.
                                        metrics.effective_queries_per_hour)

    def get_norm_factors(self, norm_factors):
        """Converts a normalization factors dict to a list"""
        if norm_factors is None:
            norm_factors = self.NORMALIZATION_FACTORS
        if isinstance(norm_factors, dict):
            return [norm_factors[i]
                    for i in range(1, self.NUMBER_QUERIES + 1)]
        else:
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
            geometric_mean *= transaction_type['norm_latency'].mean()
        return math.pow(geometric_mean, 1 / len(self.olap_groups))

    def get_query_set_time(self):
        """Computes the sum of mean normalized latencies of OLAP queries"""
        return sum(query['norm_latency'].mean()
                for query in self.olap_groups)

    def get_queries_per_hour(self, query_set_time=None):
        """Using query set time, computes number of queries that can be
        theoretically executed in an hour. If none query set time is given,
        generates it first."""
        if query_set_time is None:
            query_set_time = self.get_query_set_time()
        return 60 * 60 / query_set_time * self.NUMBER_QUERIES

    def get_effective_queries_per_hour(self, queries_per_hour=None):
        """Calculates number of effective queries per hour, i.e. how many
        queries with normalized run time would be executed by all active
        workers"""
        if queries_per_hour is None:
            queries_per_hour = self.get_queries_per_hour()
        olap_data = self.data[self.data['transactiontype']
                                    >= self.OLAP_QUERY_LOWER_ID]
        num_workers = len(olap_data['workerid'].unique())
        return queries_per_hour * num_workers

    def load_data(self):
        """Loads data from the path and  converts it to a more usable format
        """
        data = pd.read_csv(self.data_path, na_filter=False)

        # prepare the data set
        data.columns = ['transactiontype', 'starttime',
                         'latency', 'workerid', 'phase']
        data['starttime'] -= data['starttime'].min()  # measure time from zero
        data['latency'] /= 10 ** 6  # convert latency to seconds
        data['diff'] = data['phase'].diff()  # phase switches are marker with 1

        norm_data = self.normalize_data(data, self.scale_factor,
                                                self.norm_factors)
        return norm_data

    def normalize_data(self, data, scale_factor, norm_factors):
        """Normalize the OLAP query latencies in data using given normalization
        factors and scale factor and stores it in the norm_latency column.
        Stores the normalized data in the norm_latency column.
        """
        norm_data = []
        # set to 1 for NewOrder transactions
        data['neworder_count'] = np.where(data['transactiontype']
                                                == self.NEW_ORDER_ID, 1, 0)
        # cumulative sum of NewOrder transactions
        data['neworder_cum_sum'] = data['neworder_count'].cumsum()

        norm_vector = pd.Series(norm_factors,
                                index=range(self.OLAP_QUERY_LOWER_ID,
                                             self.OLAP_QUERY_HIGHER_ID + 1),
                                name="normfactors")

        norm_data = data.join(norm_vector, "transactiontype")
        norm_data['normfactors'].fillna(0, inplace=True)
        norm_data['norm_latency'] = norm_data['latency'] / \
                (scale_factor + norm_data['normfactors'] *
                     norm_data['neworder_cum_sum'])
        return norm_data

    def get_metrics(self):
        """Calculates CH-BenCHmark metrics and returns a named tuple"""

        query_set_time = self.get_query_set_time()
        queries_per_hour = self.get_queries_per_hour(query_set_time)
        return CH_METRICS(self.get_tpmch(), self.get_geometric_mean(),
                query_set_time, queries_per_hour,
                self.get_effective_queries_per_hour(queries_per_hour))

    def plot_latencies(self):
        """Plots the normalized data in a bar diagramm"""
        latencies = pd.Series(
            [query['norm_latency'].mean() for query in self.olap_groups],
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
        if not hasattr(self, 'data'):
            raise ValueError("Can not plot graphs if no data was kept"
                                " (initialized with keep_data=False).")
        if latencies:
            self.plot_latencies()
        if throughput:
            self.plot_throughput()


def main():
    """Main module. Loads the raw output, delegates metrics computing"""
    # handle arguments by hand since docopt is not a part of stdlib
    # and argparse is...suboptimal
    if not 3 <= len(sys.argv) <= 4:
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

    try:
        scale_factor = float(sys.argv[2])
    except ValueError:
        print("Please provide a decimal scale factor as the second argument.")
        print(__doc__)
        sys.exit(0)

    try:
        metrics_calc = MetricsCalculator(first_argument, scale_factor)
    except IOError:
        print ("Could not read {}.".format(first_argument))
        print(__doc__)
        sys.exit(-1)

    print(metrics_calc)

    if plot_graphs:
        if not HAS_PYLAB:
            print("Matplotlib is not installed. Skipping graph plotting.")
            sys.exit(-1)
        metrics_calc.plot()


if __name__ == "__main__":
    main()
