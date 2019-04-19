import math
import shutil
from pyspark import SparkContext
import os

from cluster_operations import cluster
from data_operations import normalize_ts_with_min_max, get_data
from group_operations import get_subsquences
from group_operations import generate_source
from query_operations import query
from visualize_sequences import plot_cluster, plot_query_result
import matplotlib.pyplot as plt

# Similarity Threshold
st = 0.2

if __name__ == '__main__':
    # TODO
    # Ask Leo to put his path to save pickle file
    Yu_path = ['/Library/Java/JavaVirtualMachines/jdk1.8.0_171.jdk/Contents/Home',
               '/Users/yli14/BrainWave/GenexPlus/res/cluster',
               '/Users/yli14/Desktop/DatasetBrainWave/2013e_001_2_channels_02backs.csv'
               ]
    Leo_path = ['/Library/Java/JavaVirtualMachines/jdk1.8.0_151.jdk/Contents/Home',
                '/Users/Leo/Documents/OneDrive/COLLEGE/COURSES/research/genex/genexPlus/test/txt',
                '/Users/Leo/Documents/OneDrive/COLLEGE/COURSES/research/genex/genexPlus/2013e_001_2_channels_02backs.csv']
    path = Leo_path
    os.environ['JAVA_HOME'] = path[0]
    # create a spark job
    sc = SparkContext("local", "First App")
    # TODO
    path_save_res = path[1]
    # if path exist, the job can't be executed
    if os.path.isdir(path_save_res):
        shutil.rmtree(path_save_res)
    # TODO
    file = path[2]
    # add test for commit
    features_to_append = [0, 1, 2, 3, 4]

    # res_list: list of raw time series data to be on distributed
    # timeSeries: a dictionary version of as res_list, used for sebsequence look up
    res_list, time_series_dict, global_min, global_max = generate_source(file, features_to_append)
    print("Global Max is " + str(global_max))
    print("Global Min is " + str(global_min))

    normalized_ts_dict = normalize_ts_with_min_max(time_series_dict, global_min, global_max)

    # TODO
    # add clustering method after grouping

    # this broadcast object can be accessed from all nodes in computer cluster
    # in order to access the value this, just use val = global_dict.value
    # for future reading data
    # NOTE that the data being broadcasted is the minmax-normalized data
    global_dict = sc.broadcast(normalized_ts_dict)
    time_series_dict = sc.broadcast(time_series_dict)

    global_dict_rdd = sc.parallelize(res_list[1:]).cache()
    # global_dict_res = global_dict_rdd.collect()
    # finish grouping here, result in a key, value pair where
    # key is the length of sub-sequence, value is the [id of source time series, start_point, end_point]
    # res_rdd = global_dict_rdd.flatMap(lambda x: get_all_subsquences(x)).collect()

    # In get_subsquences(x, 100, 110): we are grouping subsequences that are of length 90 to 110


    """
    ##### group
    group_rdd_res: list: items = (length, time series list) -> time series list: items = (id, start, end)
    """

    grouping_range = (90, 91)
    group_rdd = global_dict_rdd.flatMap(lambda x: get_subsquences(x, grouping_range[0], grouping_range[1])).map(
        lambda x: (x[0], [x[1:]])).reduceByKey(
        lambda a, b: a + b)
    group_rdd_res = group_rdd.collect()
    print("grouping done")

    """
    ##### cluster
    
    The following code is for testing clustering operation. Cluster one group without using RDD
    4/15/19
    # print("Test clustering")
    # group_res = group_rdd.collect()
    # cluster(group_res[1][1], group_res[1][0], st, global_dict.value)  # testing group with length of 9
    """
    print("Working on clustering")

    cluster_rdd = group_rdd.map(lambda x: cluster(x[1], x[0], st, global_dict.value))

    cluster_rdd.saveAsPickleFile(path_save_res)  # save all the cluster to the hard drive
    cluster_rdd_reload = sc.pickleFile(path_save_res).collect()  # here we have all the clusters in memory
    # first_dict = cluster_rdd_reload[0]
    print("clustering done")

    # plot all the clusters
    # plot_cluster(cluster_rdd_reload, 2, time_series_dict, 5)

    """
    ##### query
    Current implementation: if we want to find k best matches, we give the first k best matches for given sequence length range
    """

    # query_string = input("")

    query_sequence = get_data('(2013e_001)_(100-0-Back)_(B-DC8)_(232665953.1250)', 14, 114, time_series_dict.value)  # get an example query

    # raise exception if the query_range exceeds the grouping range
    querying_range = (90, 91)
    k = 3  # looking for k best matches
    if querying_range[0] < grouping_range[0] or querying_range[1] > grouping_range[1]:
        raise Exception("query_operations: query: Query range does not match group range")

    # query_result = query(query_sequence, cluster_rdd_reload[0], k, time_series_dict.value)
    print("querying done")
    # TODO implement querying range
    query_result = cluster_rdd.map(lambda clusters: query(query_sequence, clusters, k, time_series_dict.value)).collect()

    plot_query_result(query_sequence, query_result, time_series_dict.value)