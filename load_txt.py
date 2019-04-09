

import numpy as np
import pandas as pd
from mrjob.job import MRJob

# df = pd.read_table('/Users/yli14/Desktop/DatasetBrainWave/Driver_all_1_200.txt')

# df = {'co1': [1,2], 'col2' :[[1,2],[1,2]]}
# df = pd.read_csv('/Users/yli14/Desktop/DatasetBrainWave/001-SART-August2017-MB.csv',delimiter=',')

def strip_function(x):
    if x != '' and x != ' ':
        return x.strip()

def remove_trailing_zeros(l):
    index = len(l) - 1
    for i in range(len(l) - 1, -1, -1):
        if l[i] == ',' or l[i] == '0' or l[i] == '':
            continue
        else:
            index = i
            break
    return l[0:index]

# print('')

def generateDict(file, features_to_append, wrap_in_parantheses):
    myDict = dict()
    with open(file, 'r') as f:
        for i,line in enumerate(f):
            if not i:
                # for every line, split by ',' result in a word(x), for every word, we remove
                # the leading and trailing spaces
                # True if x % 2 == 0 else False
                features = list(map(lambda x: strip_function(x),
                                    line.strip()[:-1].split(',')))
                # features = filter(lambda x: x is not None, features)
                print(features)
                label_features_index = [features[feature] for feature in features_to_append]
                print(label_features_index)
                # features =

            # data split by ,
            if line != "" and line != "\n":
                data = remove_trailing_zeros(line.split(",")[:-1])

                # Get feature values for label
                label_features = [wrap_in_parantheses(data[index]) for index in range(0,len(label_features_index)-1)]
                series_label = "_".join(label_features).replace('  ', '-').replace(' ', '-')

                # Get series and append with label
                series_data = data[len(features):]
                myDict[series_label] = series_data
                # print(len(features))
    return myDict



if __name__ == '__main__':
    file = '001-SART-August2017-MB.csv'
    features_to_append = [0, 1, 2, 3, 4]
    wrap_in_parantheses = lambda x: "(" + str(x) + ")"
    mydict = generateDict(file, features_to_append, wrap_in_parantheses)
    print(mydict)