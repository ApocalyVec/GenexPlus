import argparse
import math
import random
import re
import shutil
from pyspark import SparkContext
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


# Similarity Threshold
def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def main(args):
    file_path = args.input
    # './dataset/001-SART-August2017-MB.csv'
    Server_path = ['/usr/lib/jvm/java-1.8.0-openjdk-amd64',
                   './res/saved_dataset',
                   file_path
                   ]
    Yu_path = ['/Library/Java/JavaVirtualMachines/jdk1.8.0_171.jdk/Contents/Home',
               './res/saved_dataset',
               './dataset/001-SART-August2017-MB-50.csv']
    Leo_path = ['/Library/Java/JavaVirtualMachines/jdk1.8.0_151.jdk/Contents/Home',
                '/Users/Leo/Documents/OneDrive/COLLEGE/COURSES/research/genex/genexPlus/test/txt',
                '/Users/Leo/Documents/OneDrive/COLLEGE/COURSES/research/genex/genexPlus/2013e_001_2_channels_02backs.csv']

    path = Server_path
    os.environ['JAVA_HOME'] = path[0]
    # create a spark job
    cores = args.cores
    st = args.st
    full_length = args.full_length
    sc = SparkContext('' + 'local' + '[' + str(cores) + ']' + '', "First App")

    # sc = SparkContext("local[4]", "First App")
    # st = 0.25
    new_path = re.match(r"./(.*)\.csv", path[2]).group(1)
    path_save_res = path[1] + '/' + new_path + '_' + str(st)
    # if path exist, the job can't be executed
    if os.path.isdir(path_save_res):
        group_rdd = sc.pickleFile(path_save_res + '/group/')
        cluster_rdd = sc.pickleFile(path_save_res + '/cluster/')
        global_dict_rdd = sc.pickleFile(path_save_res + '/dict/')
        # shutil.rmtree(path_save_res)
    else:
        # TODO
        file = path[2]
        # add test for commit
        features_to_append = [0, 1, 2, 3, 4]

        # res_list: list of raw time series data to be on distributed
        # timeSeries: a dictionary version of as res_list, used for sebsequence look up
        res_list, time_series_dict, global_min, global_max = generate_source(file, features_to_append)
        print('processing dataset' + path[2])
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
        # max(flows, key=lambda k: len(flows[k]))
        # find the key of largest length of
        # max_len_key = max(global_dict.value, key=lambda k: len(global_dict.value[k]))
        # max_length = len(global_dict.value[max_len_key])
        if full_length:
            grouping_range = (1, max([len(v) for v in global_dict.value.values()]))

        else:
            grouping_range = (89, 90)
        #         grouping_range = (1, length)

        global_dict_rdd = sc.parallelize(res_list[1:]).cache()
        global_dict_rdd.saveAsPickleFile(path_save_res + '/dict/')

        # global_dict_res = global_dict_rdd.collect()
        # finish grouping here, result in a key, value pair where
        # key is the length of sub-sequence, value is the [id of source time series, start_point, end_point]
        # res_rdd = global_dict_rdd.flatMap(lambda x: get_all_subsquences(x)).collect()

        # In get_subsquences(x, 100, 110): we are grouping subsequences that are of length 90 to 110

        """
        ##### group
        group_rdd_res: list: items = (length, time series list) -> time series list: items = (id, start, end)
        """
        # add save option or not
        group_start_time = time.time()
        group_rdd = global_dict_rdd.flatMap(lambda x: get_subsquences(x, grouping_range[0], grouping_range[1])).map(
            lambda x: (x[0], [x[1:]])).reduceByKey(
            lambda a, b: a + b)
        group_rdd.saveAsPickleFile(path_save_res + '/group/')
        group_end_time = time.time()
        print('group of timeseries from ' + str(grouping_range[0]) + ' to ' + str(grouping_range[1]) + ' using ' + str(
            group_end_time - group_start_time) + ' seconds')
        # group_rdd_res = group_rdd.collect()
        print("grouping done, saved to dataset")

        """
        ##### cluster

        The following code is for testing clustering operation. Cluster one group without using RDD
        4/15/19
        # print("Test clustering")
        # group_res = group_rdd.collect()
        # cluster(group_res[1][1], group_res[1][0], st, global_dict.value)  # testing group with length of 9
        """

        # print("Test clustering")
        # group_res = group_rdd.collect()
        # # cluster_two_pass(group_res[1][1], group_res[1][0], st, global_dict.value)  # testing group with length of 9
        # cluster(group_res[1][1], group_res[1][0], st, global_dict.value)  # testing group with length of 9

        print("Working on clustering")
        cluster_start_time = time.time()
        cluster_rdd = group_rdd.map(lambda x: cluster(x[1], x[0], st, global_dict.value))

        cluster_rdd.saveAsPickleFile(path_save_res + '/cluster/')  # save all the cluster to the hard drive
        # cluster_rdd_reload = sc.pickleFile(path_save_res).collect()  # here we have all the clusters in memory
        # first_dict = cluster_rdd_reload[0]
        cluster_end_time = time.time()

        print('clustering of timeseries from ' + str(grouping_range[0]) + ' to ' + str(
            grouping_range[1]) + ' using ' + str(cluster_end_time - cluster_start_time) + ' seconds')

        print("clustering done, saved to dataset")

        # plot all the clusters
        # plot_cluster(cluster_rdd_reload, 2, time_series_dict, 5)

        """
            ##### query
            Current implementation: if we want to find k best matches, we give the first k best matches for given sequence length range


            The following line is for testing querying on one cluster
            # query_result = query(query_sequence, cluster_rdd_reload[0], k, time_series_dict.value)

        """

        # print("Using Twopass")
        # total_cluster_count = 0
        # for cluster_dic in cluster_rdd.collect():
        #
        #     representative, cluster_subsequences = random.choice(list(cluster_dic.items()))
        #
        #     cluster_length = representative.get_length()
        #     total_cluster_count = total_cluster_count + len(cluster_dic.keys())
        #
        #     print("length " + str(cluster_length) + " has cluster count of " + str(len(cluster_dic.keys())))
        # print("Total cluster count is: " + str(total_cluster_count))

        # # '(001-SART-August2017-MB)_(211-Current-Item:-3)_(A-DC1)_(64434.0)_(105950.0)'
        # # '(2013e_001)_(100-0-Back)_(B-DC8)_(232665953.1250)'
        # query_id = '(001-SART-August2017-MB)_(211-Current-Item:-3)_(A-DC1)_(64434.0)_(105950.0)'
        # query_sequence = get_data(query_id, 24, 117, time_series_dict.value)  # get an example query
        # filter_rdd = cluster_rdd.filter(lambda x: exclude_same_id(x, query_id))
        # # raise exception if the query_range exceeds the grouping range
        # querying_range = (90, 91)
        # k = 5  # looking for k best matches
        # if querying_range[0] < grouping_range[0] or querying_range[1] > grouping_range[1]:
        #     raise Exception("query_operations: query: Query range does not match group range")
        #
        # # query_result = cluster_rdd.filter(lambda x: x).map(lambda clusters: query(query_sequence, querying_range, clusters, k, time_series_dict.value)).collect()
        # exclude_overlapping = True
        # query_result = filter_rdd.map(
        #     lambda clusters: query(query_sequence, querying_range, clusters, k, time_series_dict.value, exclude_overlapping,
        #                            0.5)).collect()
        #
        # plot_query_result(query_sequence, query_result, time_series_dict.value)
        #
    sc.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', required=True,
                        help='Input file')
    parser.add_argument('-st', '--st', type=float, required=True,
                        help='Similarity threshold for grouping function')
    parser.add_argument('-cores', '--cores', type=int, required=True,
                        help='number of cores')
    # parser.add_argument("--nice", type=str2bool, nargs='?',
    #                     const=True, default=NICE,
    #                     help="Activate nice mode.")
    parser.add_argument('-fl', '--full_length', type=str2bool, nargs='?',
                        help='group in full length', const=True, default=False)
    args = parser.parse_args()
    main(args)
    # main("")

    # Ask Leo to put his path to save pickle file
    # /Users/yli14/BrainWave/GenexPlus/001-SART-August2017-MB-200.csv
    # /Users/yli14/Desktop/DatasetBrainWave/2013e_001_2_channels_02backs.csv
    # /usr/lib/jvm/java-1.8.0-openjdk-amd64
    # /Library/Java/JavaVirtualMachines/jdk1.8.0_171.jdk/Contents/Home
