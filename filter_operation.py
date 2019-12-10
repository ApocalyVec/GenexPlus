def exclude_same_id(cluster, query_id):
    """
    This function is used to exclude all the time series in the cluster with
    the same id of the query id
    :param cluster:
    :param query_id:
    :return: cluster
    """
    for cur_rprs in cluster.keys():
        cur_list = cluster[cur_rprs]
        new_list = []
        # print('before filter size is ' + str(len(cur_list)))
        for ts_object in cur_list:
            if ts_object.id != query_id:
                new_list.append(ts_object)
        # print('after filter size is ' + str(len(new_list)))
        cluster[cur_rprs] = new_list

    return cluster


def exclude_overlapping(target_cluster, percentage, k):
    used_time_series = dict()
    new_target_cluster = []

    for ts_object in target_cluster:
        if k > 0:
            if ts_object.id not in used_time_series.keys():
                start_end_point_list = [(ts_object.start_point, ts_object.end_point)]
                used_time_series[ts_object.id] = start_end_point_list
                new_target_cluster.append(ts_object)
                k -= 1
            else:
                qualified = True
                for start_end_points in used_time_series[ts_object.id]:
                    # for a range [a1, a2] and [b1,b2], determine if they are overlapping
                    if overlapping_percentage(start_end_points, ts_object) > percentage:
                        qualified = False
                        break

                if qualified:
                    used_time_series[ts_object.id].append((ts_object.start_point, ts_object.end_point))
                    new_target_cluster.append(ts_object)
                    k -= 1
    # print("len of result cluster is " + str(len(new_target_cluster)))
    return new_target_cluster


def overlapping_percentage(start_end_points, ts_object):
    # if negative, it's naturally less than overlapping rage
    range = (min(start_end_points[1], ts_object.end_point) - max(start_end_points[0], ts_object.start_point))
    percentage = range / (start_end_points[1] - start_end_points[0])

    return percentage

def include_in_range(cluster, query_range):
    target_cluster = []
    for cur_rprs in cluster.keys():
        # print("actually querying")
        # print('end point is' + str(cur_rprs.end_point))
        # print('start point is' + str(cur_rprs.start_point))
        # TODO do we want to get raw data here, or set the raw in timeSeriesObj before calling query (no parsing)
        if (cur_rprs.end_point - cur_rprs.start_point) in range(query_range[0], query_range[1] + 1):
            # print("it's in")
            target_cluster.append(cur_rprs)
        else:
            continue
    return target_cluster