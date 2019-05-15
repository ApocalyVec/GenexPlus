from __future__ import unicode_literals, print_function

import datetime

from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.styles import Style

"""processes commands"""


class GenexPlusProject:
    """
    GenexPlus workspace

    a workspace holds the reference to the original time series, groups, and clusters

    user may load data into a workspace, group and cluster the data that has already been loaded
    """
    def __init__(self, project_name):
        self.project_name = project_name
        self.time_series_dict = None
        self.normalized_ts_dict = None
        self.time_series_list = None

        # group data
        self.group_rdd_res = None

        self.log = list()


    def get_project_name(self):
        return self.project_name

    def load_time_series(self, time_series_dict, normalized_ts_dict, time_series_list):  # load raw and normalized data
        self.time_series_dict = time_series_dict
        self.normalized_ts_dict = normalized_ts_dict
        self.time_series_list = time_series_list

        # add to log
        load_log_msg = "[" + str(datetime.datetime.now()) + "]: loaded time series:\n"
        for id in time_series_dict.keys():
            load_log_msg = load_log_msg + id + '\n'
        self.log.append(load_log_msg)

    def set_group_data(self, group_rdd_res):
        self.group_rdd_res = group_rdd_res

    def invalid_load_prompt(self):
        msg = FormattedText([
            ('class:error', 'Invalid load'),
            ('', '\n'),
            ('class:example', 'Example command: load yourFileName.txt'),
        ])

        return msg
