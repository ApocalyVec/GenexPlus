import math


def get_data(tid, start, end, time_series_dict):
    """

    :param id:
    :param start:
    :param end:
    :param timeSeries: raw time series data

    :return list: return a sub-sequence indexed by id, start point and end point in data
    """
    # TODO should we make timeSeries a dic as well as a list (for distributed computing)

    if tid not in time_series_dict.keys():
        raise Exception("data_operations: get_data: subsequnce of ID " + tid + " not found in TimeSeries!")
    else:
        return time_series_dict[tid][start:end]


def get_data_for_timeSeriesObj(time_series_obj, time_series_dict):
    """

    :type time_series_obj: TimeSeriesObj
    :param time_series_obj:
    :param time_series_dict:
    """
    tid = time_series_obj.id
    if tid not in time_series_dict.keys():
        raise Exception("data_operations: get_data: subsequnce of ID " + tid + " not found in TimeSeries!")
    else:
        return time_series_dict[tid][time_series_obj.start_point:time_series_obj.end_point]


def normalize_ts_dict(time_series_dict):
    """
    this function is now performed by group_operations: generateSource
    :param time_series_dict:
    :return dict: normalized dataset
    """
    # get the max and min
    global_min = math.inf
    global_max = - math.inf

    for ts_key in time_series_dict.keys():
        if len(time_series_dict[ts_key]) == 0:
            continue
        ts_max = max(time_series_dict[ts_key])
        ts_min = min(time_series_dict[ts_key])

        if ts_max > global_max:
            global_max = ts_max

        if ts_min < global_min:
            global_min = ts_min

    normalized_time_series = dict()

    for ts_key in time_series_dict.keys():
        normalized_time_series[ts_key] = []
        for point in time_series_dict[ts_key]:
            normalized_time_series[ts_key].append((point - global_min) / (global_max - global_min))

    return normalized_time_series
    # print("Global Max is " + str(global_max))
    # print("Global Min is " + str(global_min))


def normalize_ts_with_min_max(time_series_dict, global_min, global_max):
    """

    :param time_series_dict:
    :param min:
    :param max:
    """
    normalized_time_series = dict()

    for ts_key in time_series_dict.keys():
        normalized_time_series[ts_key] = []
        for point in time_series_dict[ts_key]:
            normalized_time_series[ts_key].append((point - global_min) / (global_max - global_min))

    return normalized_time_series
