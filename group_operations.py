import shutil
from pyspark import SparkContext
import os

from cluster_operations import clusterer


def strip_function(x):
    """
    strip function to remove empty
    :param x:
    :return:
    """
    if x != '' and x != ' ':
        return x.strip()


def remove_trailing_zeros(l):
    """
    remove trailing zeros
    :param l: list of data
    :return: data after removing trailing zeros
    """
    index = len(l) - 1
    for i in range(len(l) - 1, -1, -1):
        if l[i] == ',' or l[i] == '0' or l[i] == '':
            continue
        else:
            index = i
            break
    return l[0:index]


def get_all_subsquences(list):
    """
    user defined function for mapping for spark
    :param list: input list, has two rows, the first row is ID, the second is a list of data
    :return: val: a list of a list of value [length, id, start_point, end_point]
    """
    id = list[0]
    val = []
    # in reality, start is 0, end is len(list)
    start = 10
    end = 15
    for i in range(start, end):
        for j in range(0, i):
            val.append([i - j, id, j, i])

    return val


def generateSource(file, features_to_append):
    """
    Not doing sub-sequence here
    get the id of time series and data of time series

    :param file: path to csv file
    :param features_to_append: a list of feature index to append

    :return: a list of data in [id, [list of data]] format
    """
    # List of result
    res = []
    # dict to be broadcasted
    myDict = dict()
    wrap_in_parantheses = lambda x: "(" + str(x) + ")"

    with open(file, 'r') as f:
        for i, line in enumerate(f):
            if not i:
                features = list(map(lambda x: strip_function(x),
                                    line.strip()[:-1].split(',')))
                # print(features)
                label_features_index = [features[feature] for feature in features_to_append]
                # print(label_features_index)

            if line != "" and line != "\n":
                data = remove_trailing_zeros(line.split(",")[:-1])

                # Get feature values for label
                label_features = [wrap_in_parantheses(data[index]) for index in range(0, len(label_features_index) - 1)]
                series_label = "_".join(label_features).replace('  ', '-').replace(' ', '-')

                series_data = list(map(float, data[len(features):]))
                res.append([series_label, series_data])
                myDict[series_label] = series_data
    return res, myDict


#Similarity Threshold
st = 0.1

if __name__ == '__main__':
    # TODO
    Yu_path = ['/Library/Java/JavaVirtualMachines/jdk1.8.0_171.jdk/Contents/Home',
               '/Users/yli14/BrainWave/GenexPlus/txt',
               '/Users/yli14/Desktop/DatasetBrainWave/2013e_001_2_channels_02backs.csv']
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
    # finish grouping here, result in a key, value pair where
    # key is the length of sub-sequence, value is the [id of source time series, start_point, end_point]
    # res_rdd = global_dict_rdd.flatMap(lambda x: get_all_subsquences(x)).collect()
    group_rdd = global_dict_rdd.flatMap(lambda x: get_all_subsquences(x)).map(lambda x: (x[0], [x[1:]])).reduceByKey(
        lambda a, b: a + b)
    # group_res = group_rdd.collect()
    print("grouping done")
    print("Working on clustering")

    cluster_rdd = group_rdd.map(lambda x: clusterer(x[1], x[0], st, global_dict.value)).collect()
    print("clustering done")
    # TODO Can we do this without broadcasting.

    # TODO here clusterer x[1] is all the sub-sequences of len x[0], but x[1] is actually the index of seb-sequebces: [id, start, end]



