import math
import shutil
from pyspark import SparkContext
import os

from cluster_operations import cluster


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


def get_subsquences(list, length1, length2):
    """
    user defined function for mapping for spark
    :param list: input list, has two rows, the first row is ID, the second is a list of data
    :param tuple: start and end
    :return:    val: a list of a list of value [length, id, start_point, end_point]
                int max: the global maximum of all subseqeunces
                int min: the global minimum of all subseqeunces
    """
    id = list[0]
    val = []
    # Length from start+lengthrange1, start + lengthrange2
    # in reality, start is 0, end is len(list)

    for i in range(0, len(list[1])):
        # make sure to get the length2
        for j in range(length1, length2 + 1):
            if i + j < len(list[1]):
                # length, id, start, end
                val.append([j, id, i, i + j])

    return val


def generate_source(file, features_to_append):
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

    # get the max and min
    global_min = math.inf
    global_max = - math.inf

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

                # get the min and max
                if len(series_data) == 0:
                    continue
                ts_max = max(series_data)
                ts_min = min(series_data)
                if ts_max > global_max:
                    global_max = ts_max

                if ts_min < global_min:
                    global_min = ts_min

    # for key, value in myDict.items():
    #     myDict[key] = normalize(value, max, min)

    return res, myDict, global_min, global_max


