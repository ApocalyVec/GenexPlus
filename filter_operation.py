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
        print('before filter size is ' + str(len(cur_list)))
        for ts_object in cur_list:
            if ts_object.id != query_id:
                new_list.append(ts_object)
        print('after filter size is ' + str(len(new_list)))
        cluster[cur_rprs] = new_list

    return cluster

def exclude_overlapping(cluster, percentage):
    pass
