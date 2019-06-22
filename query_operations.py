import math


from classifier_operations import sim_between_seq
from data_operations import get_data, get_data_for_timeSeriesObj
from filter_operation import exclude_overlapping
from load_txt import strip_function, remove_trailing_zeros


def query(query_sequence, query_range, cluster, k, time_series_dict, exclude_overlap, percentage = 1):
    """

    :param query_sequence: list of data: the sequence to be queried
    :param cluster: dict[key = representative, value = list of timeSeriesObj] -> representative is timeSeriesObj
                    the sequences in the cluster are all of the SAME length
    :param k: int
    :return list of time series objects: best k matches. Again note they are all of the SAME length
    """

    # iterate through all the representatives to find which cluster to look at
    min_rprs = None  # the representative that is closest to the query distance
    min_dist = math.inf
    target_cluster = []
    for cur_rprs in cluster.keys():
        # print("actually querying")
        # print('end point is' + str(cur_rprs.end_point))
        # print('start point is' + str(cur_rprs.start_point))
        # TODO do we want to get raw data here, or set the raw in timeSeriesObj before calling query (no parsing)
        if (cur_rprs.end_point - cur_rprs.start_point) in range(query_range[0], query_range[1] + 1):
            # print("it's in")
            cur_dist = sim_between_seq(query_sequence, get_data_for_timeSeriesObj(cur_rprs, time_series_dict))

            if cur_dist < min_dist:
                min_rprs = cur_rprs
                min_dist = cur_dist
        else:
            pass

    if min_rprs:
        print('min representative is ' + min_rprs.id)

        print("Querying Cluster of length: " + str(len(get_data_for_timeSeriesObj(min_rprs, time_series_dict))))
        target_cluster = cluster[min_rprs]
        print('len of cluster is ' + str(len(target_cluster)))
        print("sorting")

        # this sorting is taking a long time!
        target_cluster.sort(key=lambda cluster_sequence: sim_between_seq(query_sequence, get_data_for_timeSeriesObj(cluster_sequence, time_series_dict)))
    #     use a heap?
    #     use quickselect
    #     similar question to k closet point to origin

    # where can we get none?
    if len(target_cluster) != 0:
        # print(target_cluster)
        if exclude_overlap:
            target_cluster = exclude_overlapping(target_cluster, percentage, k)
            print("k is" + str(k))
        return target_cluster[0:k]  # return the k most similar sequences
    # else:
    #     raise Exception("No matching found")
#     raise none exception?
#     Note that this function return None for those times series range not in the query range
def custom_query(query_sequences,query_range, cluster, k, time_series_dict):
    """

    :param query_sequences: list of list: the list of sequences to be queried
    :param cluster: dict[key = representative, value = list of timeSeriesObj] -> representative is timeSeriesObj
                    the sequences in the cluster are all of the SAME length
    :param k: int
    :return list of time series objects: best k matches. Again note they are all of the SAME length
    """

    # iterate through all the representatives to find which cluster to look at
    # try :
    #
    query_result = dict()
    if not isinstance(query_sequences, list) or len(query_sequences) == 0:
        raise ValueError("query sequence must be a list and not empty")
    cur_query_number = 0
    if isinstance(query_sequences[0], list):
        for cur_query in query_sequences:
            if isinstance(cur_query, list):
                query_result[cur_query_number]:get_most_k_sim(cur_query, query_range, cluster, k, time_series_dict)
    else:
        return get_most_k_sim(query_sequences, query_range, cluster, k, time_series_dict)

def get_most_k_sim(query_sequence, query_range, cluster, k, time_series_dict):
    min_rprs = None  # the representative that is closest to the query distance
    min_dist = math.inf
    target_cluster = []
    for cur_rprs in cluster.keys():

        # TODO do we want to get raw data here, or set the raw in timeSeriesObj before calling query (no parsing)
        if (cur_rprs.end_point - cur_rprs.start_point) in range(query_range[0], query_range[1] + 1):

            cur_dist = sim_between_seq(query_sequence, get_data_for_timeSeriesObj(cur_rprs, time_series_dict))

            if cur_dist < min_dist:
                min_rprs = cur_rprs
                min_dist = cur_dist
        else:
            continue

    if min_rprs:
        print('min representative is ' + min_rprs.id)

        print("Querying Cluster of length: " + str(len(get_data_for_timeSeriesObj(min_rprs, time_series_dict))))
        target_cluster = cluster[min_rprs]
        print('len of cluster is ' + str(len(target_cluster)))
        print("sorting")

        target_cluster.sort(key=lambda cluster_sequence: sim_between_seq(query_sequence,
                                                                         get_data_for_timeSeriesObj(cluster_sequence,
                                                                                                    time_series_dict)))

        return target_cluster[0:k]  # return the k most similar sequences



def get_query_sequence_from_file(file):
    resList = []
    with open(file, 'r') as f:
        for i, line in enumerate(f):
            if not i:
                features = list(map(lambda x: strip_function(x),
                                    line.strip()[:-1].split(',')))
            if line != "" and line != "\n":
                data = remove_trailing_zeros(line.split(",")[:-1])
                series_data = data[len(features):]
                resList.append(series_data)
    if len(resList[0]) == 0:
        return resList[1:]
    else:

        return resList


res = get_query_sequence_from_file(r'2013e_001_2_channels_02backs.csv')
print(res[0])
print(isinstance(res[0], list))
print(type(res[0][0]))