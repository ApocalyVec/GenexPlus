import shutil
from pyspark import SparkContext
import os

from cluster_operations import cluster
from group_operations import get_subsquences
from group_operations import generateSource

#Similarity Threshold
st = 10

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
    path = Yu_path
    os.environ['JAVA_HOME'] = path[0]
    # create a spark job
    sc = SparkContext("local", "First App")
    # TODO
    path_save_res = path[1]
    # if path exist, the job can't be executed
    if os.path.isdir(path_save_res):
        shutil.rmtree(path_save_res)
    #TODO
    file = path[2]
    # add test for commit
    features_to_append = [0, 1, 2, 3, 4]

    # res_list: list of raw time series data to be on distributed
    # timeSeries: a dictionary version of as res_list, used for sebsequence look up
    res_list, time_series_dict = generateSource(file, features_to_append)
    # TODO
    # add clustering method after grouping

    # this broadcast object can be accessed from all nodes in computer cluster
    # in order to access the value this, just use val = global_dict.value
    # for future reading data
    global_dict = sc.broadcast(time_series_dict)

    global_dict_rdd = sc.parallelize(res_list[1:]).cache()
    # global_dict_res = global_dict_rdd.collect()
    # finish grouping here, result in a key, value pair where
    # key is the length of sub-sequence, value is the [id of source time series, start_point, end_point]
    # res_rdd = global_dict_rdd.flatMap(lambda x: get_all_subsquences(x)).collect()
    group_rdd = global_dict_rdd.flatMap(lambda x: get_subsquences(x,10,15)).map(lambda x: (x[0], [x[1:]])).reduceByKey(
        lambda a, b: a + b)
    group_rdd_res = group_rdd.collect()
    print("grouping done")

    """
    The following code is for testing clustering operation.
    4/15/19
    # print("Test clustering")
    # group_res = group_rdd.collect()
    # cluster(group_res[1][1], group_res[1][0], st, global_dict.value)  # testing group with length of 9
    """



    print("Working on clustering")
    cluster_rdd = group_rdd.map(lambda x: cluster(x[1], x[0], st, global_dict.value))
    cluster_rdd.saveAsPickleFile(path_save_res)
    cluster_rdd_reload = sc.pickleFile(path_save_res).collect()
    print("clustering done")


    # TODO Can we do this without broadcasting.

    # TODO here clusterer x[1] is all the sub-sequences of len x[0], but x[1] is actually the index of seb-sequebces: [id, start, end]

