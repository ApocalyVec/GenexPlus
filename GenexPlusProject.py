from __future__ import unicode_literals, print_function
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.styles import Style

"""processes commands"""


class GenexPlusProject:
    """
    GenexPlus workspace

    a workspace holds the reference to the original time series, groups, and clusters

    user may load data into a workspace, group and cluster the data that has already been loaded
    """
    def __init__(self, name):
        self.project_name = name
        self.groups = None

    def get_project_name(self):
        return self.project_name

    def load_data(self, filename, format='original'):
        msg = FormattedText([
            ('class:normal', 'loading'),
            ('', ' '),
            ('class:prompt', filename),
            # ('class:bbb', 'fileName'),
        ])









        return msg

    def invalid_load_prompt(self):
        msg = FormattedText([
            ('class:error', 'Invalid load'),
            ('', '\n'),
            ('class:example', 'Example command: load yourFileName.txt'),
        ])

        return msg
