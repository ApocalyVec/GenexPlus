import argparse
import math
import random
import re
import shutil
from pyspark import SparkContext, SparkConf
import os
import time
from cluster_operations import cluster, cluster_two_pass
from data_operations import normalize_ts_with_min_max, get_data
from group_operations import get_subsquences
from group_operations import generate_source
from query_operations import query
from filter_operation import exclude_same_id
from visualize_sequences import plot_cluster, plot_query_result
import matplotlib.pyplot as plt



def get_global_min_max(ts_dict):
    min_so_far = math.inf
    max_so_far = - math.inf

    for id, data in ts_dict.items():
        for value in data:
            if value < min_so_far:
                min_so_far = value
            if value > max_so_far:
                max_so_far = value

    return min_so_far, max_so_far

def ts_dict_to_list(ts_dict):
    ts_list = []

    for id, data in ts_dict.items():
        ts_list.append([id, data])

    return ts_list


def genex(ts_dict, query, similarity_threshold):
    """
    make sure that all time series in the ts_dict are of the same length
    :param ts_dict:
    :param query:
    """
    javaHome_path = '/Library/Java/JavaVirtualMachines/jdk1.8.0_151.jdk/Contents/Home'

    os.environ['JAVA_HOME'] = javaHome_path

    conf = SparkConf().setAppName("GenexPlus").setMaster("local[*]")  # using all available cores
    sc = SparkContext(conf=conf)

    ts_len = 0
    # get the length of the longest ts
    for id, data in ts_dict.items():
        ts_len = len(data)


    global_min, global_max = get_global_min_max(ts_dict)

    normalized_ts_dict = normalize_ts_with_min_max(ts_dict, global_min, global_max)

    global_dict = sc.broadcast(normalized_ts_dict)
    time_series_dict = sc.broadcast(ts_dict)

    # make the ts dict into a list so that we can parallelize the list
    ts_list = ts_dict_to_list(normalized_ts_dict)
    ts_list_rdd = sc.parallelize(ts_list[1:], numSlices=16)

    # grouping
    group_rdd = ts_list_rdd.flatMap(lambda x: get_subsquences(x, 0, ts_len)).map(
        lambda x: (x[0], [x[1:]])).reduceByKey(
        lambda a, b: a + b)
    # clustering

    # group_res = group_rdd.collect()
    # cluster(group_res[1][1], group_res[1][0], similarity_threshold, global_dict.value)  # testing group with length of 9

    cluster_rdd = group_rdd.map(lambda x: cluster(x[1], x[0], similarity_threshold, global_dict.value))

    cluster_result = cluster_rdd.collect()

    print()

    """
    grouping
    """



file = '2013e_001_2_channels_02backs.csv'
features_to_append = [0, 1, 2, 3, 4]
res_list, time_series_dict, global_min, global_max = generate_source(file, features_to_append)

genex(time_series_dict, 1, 0.1)
