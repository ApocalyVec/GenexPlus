from prompt_toolkit import print_formatted_text
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit.styles import Style

style = Style.from_dict({
    # User input (default text).
    '': '#ff0066',

    # Prompt.
    'error': '#ff0000',
    'normal': '#00cbff',
    'prompt': '#00ffff bg:#444400',
    'file': '#000651 bg:#cccccc italic',
    'example': 'ansicyan underline'
    # 'pound':    '#00aa00',
    # 'host':     '#00ffff bg:#444400',
    # 'path':     'ansicyan underline',
})

def gp_not_opened_error():
    err_msg = FormattedText([
        ('class:error', 'No GenexPlus Project Opened, please use the open command to open a GenexPlus Project'),
    ])
    print_formatted_text(err_msg, style=style)


def spark_context_not_set_error():
    err_msg = FormattedText([
        ('class:error', 'Spark Context not set, please use set command to set spark context'),
    ])
    print_formatted_text(err_msg, style=style)


def no_group_before_cluster_error():
    err_msg = FormattedText([
        ('class:error', 'Please group the data before clustering'),
    ])
    print_formatted_text(err_msg, style=style)


def no_cluster_before_query_error():
    err_msg = FormattedText([
        ('class:error', 'Please cluster the data before querying'),
    ])
    print_formatted_text(err_msg, style=style)


def no_load_before_group_error():
    err_msg = FormattedText([
        ('class:error', 'Please load the data before grouping'),
    ])
    print_formatted_text(err_msg, style=style)


def get_arg_error():
    err_msg = FormattedText([
        ('class:error', 'get arg error'),
    ])
    print_formatted_text(err_msg, style=style)


def load_file_not_found_error(missing_file):
    err_msg = FormattedText([
        ('class:error',
         'File ' + missing_file + ' not found'),
    ])
    print_formatted_text(err_msg, style=style)


def no_query_result_before_plot():
    err_msg = FormattedText([
        ('class:error',
         'Please get query result before plot it'),
    ])
    print_formatted_text(err_msg, style=style)