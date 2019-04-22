import math

from classifier_operations import sim_between_seq
from data_operations import get_data, get_data_for_timeSeriesObj


def query(query_sequence, query_range, cluster, k, time_series_dict):
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
        # print('end point is' + str(cur_rprs.end_point))
        # print('start point is' + str(cur_rprs.start_point))
        # TODO do we want to get raw data here, or set the raw in timeSeriesObj before calling query (no parsing)
        if (cur_rprs.end_point - cur_rprs.start_point) in range(query_range[0], query_range[1] + 1):
            print("it's in")
            cur_dist = sim_between_seq(query_sequence, get_data_for_timeSeriesObj(cur_rprs, time_series_dict))

            if cur_dist < min_dist:
                min_rprs = cur_rprs
                min_dist = cur_dist
        else:
            pass

    if min_rprs:
        print("Querying Cluster of length: " + str(len(get_data_for_timeSeriesObj(min_rprs, time_series_dict))))
        target_cluster = cluster[min_rprs]

        # this sorting is taking a long time!
        target_cluster.sort(key=lambda cluster_sequence: sim_between_seq(query_sequence,
                                                                         get_data_for_timeSeriesObj(cluster_sequence,
                                                                                                    time_series_dict)))
    if target_cluster:
        return target_cluster[0:k]  # return the k most similar sequences
    # else:
    #     raise Exception("No matching found")
#     raise none exception?
#     Note that this function return None for those times series range not in the query range