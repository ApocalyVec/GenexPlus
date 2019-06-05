from __future__ import unicode_literals, print_function

import datetime

from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.styles import Style

from CLI_exceptions import DuplicateIDError

"""processes commands"""


class GenexPlusProject:
    """
    GenexPlus workspace

    a workspace holds the reference to the original time series, groups, and clusters

    user may load data into a workspace, group and cluster the data that has already been loaded

    attrib[log]: dict: key = action, value = (time stamp, data)
    """
    def __init__(self, project_name):
        self.project_name = project_name

        # raw time series data
        self.time_series_dict = None
        self.normalized_ts_dict = None
        self.time_series_list = None

        # gp project info
        self.time_series_status_dict = {}

        # group data
        self.group_rdd_res = None
        # cluster data
        self.cluster_rdd_res = None

        self.log = {}
        print("initialized")


    def get_project_name(self):
        return self.project_name

    def set_project_name(self, name):
        self.project_name = name


    def load_time_series(self, time_series_dict, normalized_ts_dict, time_series_list):  # load raw and normalized data
        """

        :param time_series_dict:
        :param normalized_ts_dict:
        :param time_series_list:

        """

        if self.time_series_dict is not None:  # check if there is duplicate IDs
            duplicate_id_list = []  # the list contains duplicate ids between existing time series and the newly loaded time series

            for new_id in time_series_dict.keys():
                print('out there')
                if new_id in self.time_series_dict.keys():  # if a new id already exists in loaded time series
                    print('in here')
                    duplicate_id_list.append(new_id)
            if len(duplicate_id_list) != 0:  # there should NOT be any duplicate IDs
                raise DuplicateIDError(duplicate_id_list)

        self.time_series_dict = time_series_dict
        self.normalized_ts_dict = normalized_ts_dict
        self.time_series_list = time_series_list

        # write the load action just did to log
        self.write_to_log('load', time_series_dict.keys())  # log only contains the IDs that are loaded

        # set the just loaded time series status to 'loaded'
        for id in time_series_dict.keys():
            self.time_series_status_dict[id] = 'loaded'

    def write_to_log(self, action, data):
        if action not in self.log.keys():
            self.log[action] = list()

        self.log[action].append((str(datetime.datetime.now()), data))

    def print_log(self):

        for action, action_entries in self.log.items():
            if action == 'load':
                for load_entry in action_entries:  # load_entry: tuple (time stamp string, list of id's)
                    print("Loaded the following time series at " + str(load_entry[0]) + " :")
                    for id in load_entry[1]:
                        print('     ' + id)
            if action == 'group':
                for group_entry in action_entries:  # load_entry: tuple (time stamp string, list of id's)
                    print("Grouped the following time series at " + str(group_entry[0]) + " :")
                    for id in group_entry[1]:
                        print('     ' + id)

        # print(self.log)
    def print_ts(self):
        """
        print the ids and status of all the loaded time series
        """
        if len(self.time_series_status_dict.keys()) != 0:  # if there are loaded time series
            print('time series ID   Status')
            for ts_id, ts_status in self.time_series_status_dict.items():
                print(ts_id + '     ' + ts_status)
        else:
            print("No time series ID available since no time series has been loaded")
            print("Use the load command to load time series")

    def set_group_data(self, group_rdd_res, group_time):
        self.group_rdd_res = group_rdd_res
        # self.write_to_log('group', (self.ts_dict.keys(), group_time))

        self.write_to_log('group', (self.time_series_dict.keys()))

        # set the just grouped time series status to 'grouped'
        for id in self.time_series_status_dict:
            self.time_series_status_dict[id] = 'grouped'

    def set_cluster_data(self, cluster_rdd):
        self.cluster_rdd_res = cluster_rdd

    def invalid_load_prompt(self):
        msg = FormattedText([
            ('class:error', 'Invalid load'),
            ('', '\n'),
            ('class:example', 'Example command: load yourFileName.txt'),
        ])

        return msg
