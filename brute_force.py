import matplotlib.pyplot as plt
from dtaidistance import dtw  # source code https://github.com/wannesm/dtaidistance

from data_operations import *
from group_operations import generate_source
from time_series_obj import *


def brute_force(query_list, ts_list, top_k=None, threshold=None, overlapping=None, dis_type=None, exclude_id=False):
    """
    :param query_list: a list of key-value pair pattern [key, [p1,p2,p3....]]
    :param ts_list: lists of key-value pairs [[key,[points]],
                                              [key,[points]],
                                              [key,[points]]......]
    :return: a list of time_series_obj objects each of them store the id, start point, end point for the matched ts
    """

    def get_distance(func, *args):
        distance, paths = func(*args)
        return distance

    def set_dis_type():
        pass

    def slice_ts(ts_key, ts_value, length, overlapping=None):

        if overlapping:
            pass
        # TODO
        else:
            amount = len(ts_value) - length + 1

            for i in range(amount):
                yield TimeSeriesObj(ts_key, i, i + length)

    query_v = query_list[1]
    query_len = len(query_v)
    query_id = query_list[0]

    result_ls = []
    min_dis = float('inf')

    ts_dict = dict(ts_list)

    # Excluding the same ID from the candidates ts
    if exclude_id is True:
        ts_dict.pop(query_id)

    # Setting the threshold for later query process
    if threshold:
        min_dis = threshold

    for k, v in ts_dict.items():
        # Adding overlapping para into the slice function
        for sublist in slice_ts(k, v, query_len, overlapping):
            points = get_data_for_timeSeriesObj(sublist, ts_dict)

            distance = get_distance(dtw.warping_paths, points, query_v)

            if distance < min_dis:
                result_ls.append(sublist)

                if not threshold:
                    min_dis = distance

    if not threshold:
        result_ls = [ls for ls in result_ls
                     if get_distance(dtw.warping_paths, get_data_for_timeSeriesObj(ls, ts_dict), query_v) <= min_dis]

    result_ls.sort(
        key=lambda each: get_distance(dtw.warping_paths, get_data_for_timeSeriesObj(each, ts_dict), query_v))

    if top_k and top_k < len(result_ls):
        result_ls = result_ls[:top_k]

    return result_ls


def plot_query_result(query_sequence, query_result, time_series_dict):
    """

    :param query_sequence:
    :param query_result: list of list of timeSeriesObj
    """

    plt.figure(figsize=(15, 15))
    plt.plot(query_sequence, label='QUERY')

    for ss in query_result:
        plt.plot(get_data_for_timeSeriesObj(ss, time_series_dict),
                 label='Rank' + str(query_result.index(ss) + 1) + ss.id + '_' + str(
                     ss.start_point) + '_' + str(ss.end_point))
        plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))

    plt.show()

if __name__ == '__main__':
    # Prepare the time_series
    features_to_append = [0, 1, 2, 3, 4]
    time_series_list, global_min, global_max = generate_source('2013e_001_2_channels_02backs.csv', features_to_append)
    time_series_list = normalize_ts_with_min_max(time_series_list, global_min, global_max)
    time_series_dict = dict(time_series_list)

    # Prepare the query_ls like [key, [point1, point2, points3...]]
    id = '(2013e_001)_(100-0-Back)_(A-DC4)_(232665953.1250)_(232695953.1250)'
    query = get_data(id, 7, 30, time_series_dict)
    query_ls = [id, query]

    match_ls = brute_force(query_ls, time_series_list)
    plot_query_result(query, match_ls, time_series_dict)
