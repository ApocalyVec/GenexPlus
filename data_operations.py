def get_data(id, start, end, timeSeries):
    """

    :param id:
    :param start:
    :param end:
    :param timeSeries: raw time series data

    :return list: return a sub-sequence indexed by id, start point and end point in data
    """
    # TODO should we make timeSeries a dic as well as a list (for distributed computing)
    for sequence in timeSeries:
        if id == sequence[0]:  # if the id matches
            return sequence[start:end]
    raise Exception("data_operations: get_data: subsequnce of ID " + id + " not found in TimeSeries!")