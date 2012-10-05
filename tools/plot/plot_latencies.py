#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Wed Oct  3 12:30:54 2012

@author: alendit
"""
import numpy as np
import pylab as p
import sys


  
def extract_latency_data(filename, interval=None):
    raw = np.genfromtxt(filename, delimiter=",")
    raw = raw[1:]
    queries = raw[:, 0]
    
    convert = 10 ** 6 # microseconds
    
    result = []
    
    for q in set(queries):
        l_q = int(q)
        query_lat = raw[:,2][queries == q]
        l_count = len(query_lat)
        l_mean = query_lat.mean() / convert
        l_min = query_lat.min() / convert
        l_max = query_lat.max() / convert
        print locals()['l_q']
        
        print "{l_q} {l_count}\n{l_mean} {l_min} {l_max}".format(**locals())
        result.append([l_q, l_count, l_mean, l_min, l_max])

    return np.array(result)

def extract_troughput_data(filename, inteval=None):
    # in sec
    SLICE_SIZE = 5

    raw = np.genfromtxt(filename, delimiter=',')
    # filter the invalid transaction
    raw = raw[1:]
    test_start = raw[:, 1].min()
    result = []

    for t in xrange(0, 61, 5):
        start = t
        end = t + SLICE_SIZE
        time_slice = raw[(raw[:, 1] >= start + test_start) & (raw[:, 1] < end + test_start)][:, 2]
        throughput = len(time_slice) / SLICE_SIZE
        result.append(throughput)

    return np.array(result)

def plot_latency_data(data, filename=None, ymax=1.5):
    fig = p.figure()
    
    ax = fig.add_subplot(111)
    
    x = result[:, 0]
    
    y = result[:, 2]
    
    width = .2
    
    
    ax.bar(x, y, width=width, color='yellow')
    ax.bar(x+width, result[:, 3], width=width, color='green')
    ax.bar(x+width*2, result[:, 4], width=width, color='red')    
    
    ax.set_ylabel("Seconds")
    ax.set_xlabel("Query Number")
    
    ax.set_ylim(ymax=ymax)
    
    
    ax.set_xticks(x + width * 1.5)
    ax.set_xticklabels(x.astype('I') - 1)
    
    if filename:
        title = filename
        ax.set_title(title)
        p.savefig(title)
        
    p.show()

def plot_throughput_data(data, filename=None):
    fig = p.figure()

    ax = fig.add_subplot(111)

    x = np.arange(len(data))

    ax.plot(x, data)

    ax.set_xticks(x)
    ax.set_xticklabels([t * 5 for t in x])

    p.show()

if __name__ == '__main__':
   
    result = extract_latency_data(sys.argv[1])
    ymax = result[:, 4].max() + .1
    output = sys.argv[2] if len(sys.argv) > 2 else None
    plot_latency_data(result, output, ymax)