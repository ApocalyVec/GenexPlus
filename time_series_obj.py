class time_series_obj:
    def __init__(self, id, start_point, end_point, raw_data, group_represented = None):
        self.id = id
        self.start_point = start_point
        self.end_point = end_point
        self.raw_data = raw_data
        # represent which cluster
        self.group_represented = group_represented


    def set_group_represented(self, group_id):
        self.group_represented = group_id

    def remove_group_represented(self):
        self.group_represented = None

    def set_raw_data(self, new_raw_data):
        self.raw_data = new_raw_data

    def get_raw_data(self):
        return self.raw_data

    def toString(self):
        return self.id + str(self.start_point) + str(self.end_point)