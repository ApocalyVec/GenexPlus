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


def get_subsquences(list, startEnd):
    """
    user defined function for mapping for spark
    :param list: input list, has two rows, the first row is ID, the second is a list of data
    :param tuple: start and end
    :return: val: a list of a list of value [length, id, start_point, end_point]
    """
    id = list[0]
    val = []
    # in reality, start is 0, end is len(list)
    start = startEnd[0]
    end = startEnd[1]

    # get the max and min

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




