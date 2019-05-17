from group_operations import generate_source
from dtaidistance import dtw  # source code https://github.com/wannesm/dtaidistance

features_to_append = [0, 1, 2, 3, 4]
res_list, time_series_dict, global_min, global_max = generate_source('2013e_001_2_channels_02backs.csv', features_to_append)


def best_match_ts(query, ts_dict):

    query_len = len(query)

    best_of_so_far = float('inf')

    best_match = dict()

    for key, value in ts_dict.items():
        candidates = slice_list(value, query_len)

        for i in range(len(candidates)):
            distance, paths = dtw.warping_paths(query, candidates[i])

            if distance < best_of_so_far:
                best_match['ts_id'] = key
                best_match['value'] = candidates[i]
                best_match['distance'] = distance
                best_match['best_path'] = dtw.best_path(paths)

    return best_match


def slice_list(ts_list, length):

    number_of_sublist = len(ts_list) - length + 1

    sliced_list = []

    for i in range(number_of_sublist):
        sliced_list.append(ts_list[i:i+length])

    return sliced_list