from group_operations import generate_source
from dtaidistance import dtw  # source code https://github.com/wannesm/dtaidistance
import datetime

features_to_append = [0, 1, 2, 3, 4]
res_list, time_series_dict, global_min, global_max = generate_source('2013e_001_2_channels_02backs.csv', features_to_append)


def best_match_ts(query, ts_dict):
    """
    Applying the brute-force pattern to find the best match for the query from the set of the time_series_dict

    :param: query list
    :param: time_series_dict

    :return: best_match dict [key: time_series_ID, value: time_series_data, distance: similarity_distance(DTW Algorithm),
                                best_path: side-product from dtw algorithm]
    """
    query_len = len(query)

    best_of_so_far = float('inf')

    best_match = dict()

    start_time = datetime.datetime.now()

    for key, value in ts_dict.items():
        candidates = slice_list(value, query_len)

        for i in range(len(candidates)):
            distance, paths = dtw.warping_paths(query, candidates[i])

            if distance < best_of_so_far:
                best_match['ts_id'] = key
                best_match['value'] = candidates[i]
                best_match['distance'] = distance
                best_match['best_path'] = dtw.best_path(paths)

    end_time = datetime.datetime.now()
    print("Time period of the execution for the brute_force: " + str((end_time - start_time).seconds))

    return best_match


def slice_list(ts_list, length):
    """
    A helper method used to slice one time_series list into multiple sublist based on the value of the second parameter

    :param ts_list: one time_series list
    :param length: the length of one sublist

    :return: one sliced time_series includes all of the candidates of the input time_series

    For example:
    input = [1,3,4,5,6,7,8,4,5,67,5]
    length = 3

    result = [[1, 3, 4], [3, 4, 5], [4, 5, 6], [5, 6, 7], [6, 7, 8], [7, 8, 4], [8, 4, 5], [4, 5, 67], [5, 67, 5]]
    """
    number_of_sublist = len(ts_list) - length + 1

    sliced_list = []

    for i in range(number_of_sublist):
        sliced_list.append(ts_list[i:i+length])

    return sliced_list
