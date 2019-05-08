"""
prompt_toolkit: https://python-prompt-toolkit.readthedocs.io/en/master/pages/printing_text.html

open: will open existing workspace. If designated workspace doesn't exist, it will notify user a new
    ex: $ open my_project
load: load target data set in to the workspace - put data set onto slaves and do the grouping

set: set sparkcontext of the system, this is required for the following operations: group, cluster and query
    arguements: 1. path to java home, 2. number of cores
"""
import os
import time

from prompt_toolkit import prompt, print_formatted_text
from prompt_toolkit.formatted_text import FormattedText, PygmentsTokens
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter, Completion, Completer
import click
import fuzzyfinder
from prompt_toolkit.styles import Style
import pickle

# commands
from pyspark import SparkContext

from GenexPlusProject import GenexPlusProject
from data_operations import normalize_ts_with_min_max
from group_operations import generate_source, get_subsquences

GPKeywords = ['load', 'group', 'cluster', 'query', 'plot', 'help', 'exit', 'open', 'set']

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

def sparkContextNotSetError():
    err_msg = FormattedText([
        ('class:error', 'Spark Context not set, please use set command to set spark context'),
    ])
    print_formatted_text(err_msg, style=style)

class GPCompleter(Completer):
    def get_completions(self, document, complete_evennt):
        word_before_cursor = document.get_word_before_cursor(WORD=True)
        matches = fuzzyfinder(word_before_cursor, GPKeywords)
        for m in matches:
            yield Completion(m, start_position=-len(word_before_cursor))


GPCompleter = WordCompleter(['load', 'group', 'cluster', 'query', 'plot'],
                            ignore_case=True)

gp_project = None  # GenexPlus workspace

java_home_path = None
num_cores = None
sc = None

features_to_append = [0, 1, 2, 3, 4]  # TODO what is this?

print("Java Home Path is set to None")

while 1:

    if gp_project is not None:
        prompt_message = [('class:prompt', 'GenexPlus: ' + gp_project.get_project_name() + ' >'),
                          ]
    else:
        prompt_message = [('class:prompt', 'GenexPlus >'),
                          ]

    user_input = prompt(prompt_message,
                        history=FileHistory('history.txt'),
                        auto_suggest=AutoSuggestFromHistory(),
                        completer=GPCompleter,
                        style=style
                        )
    # click.echo_via_pager(user_input)  # replaces the print statement via pager
    message = ''

    if user_input is not None:
        args = user_input.split()

        if len(args) != 0:

            if args[0] == 'open':  # for user input 'open', open a existing or creat a new gp project
                if len(args) != 2:  # if wronge number of arguments is given
                    err_msg = FormattedText([
                        ('class:error', 'Wrong number of arguments, please specify one file to open'),
                    ])
                    print_formatted_text(err_msg, style=style)
                else:
                    try:
                        print("Opening " + args[1])
                        gp_project_file = open(args[1] + '.pickle', "rb")
                        gp_project = pickle.load(gp_project_file)
                    except FileNotFoundError:
                        isCreateNewProject = prompt("Project " + args[
                            1] + " does not exit, would you like to create a new GenexPlus Project? [y/n]")
                        if isCreateNewProject == 'y':
                            # creating new GenexPlus project
                            print("Creating " + args[1])
                            gp_project = GenexPlusProject(args[1])

                            gp_project_fn = args[1] + '.pickle'

                            gp_project_file = open(gp_project_fn, "wb")

                            pickle.dump(gp_project, gp_project_file)  # save gp_project object to gp_project_file
                        # else isCreateNewProject == 'n' or 'N' or 'No' or 'no':

            elif args[0] == 'close':  # close opened gp_project
                if gp_project is not None:
                    print("Closing" + gp_project.get_project_name())
                    gp_project = None


            elif args[0] == 'set':  # close opened gp_project
                if len(args) != 3:  # if wronge number of arguments is given
                    err_msg = FormattedText([
                        ('class:error',
                         'Wrong number of arguments, please specify the path to Java Home and the number of cores'),
                    ])
                    print_formatted_text(err_msg, style=style)
                else:
                    java_home_path = args[1]
                    num_cores = args[2]
                    os.environ['JAVA_HOME'] = java_home_path
                    sc = SparkContext('' + 'local' + '[' + str(num_cores) + ']' + '', "GenexPlus")
                    print("Java home set at " + java_home_path)
                    print("Number of cores set to " + num_cores)

            elif args[0] == 'load':  # load given csv file
                if gp_project is None:
                    gp_not_opened_error()
                else:
                    if len(args) != 2:  # if wronge number of arguments is given
                        err_msg = FormattedText([
                            ('class:error',
                             'Wrong number of arguments, please specify the path to the the data you wish to load'),
                        ])
                        print_formatted_text(err_msg, style=style)
                    else:
                        res_list, time_series_dict, global_min, global_max = generate_source(args[1],
                                                                                             features_to_append)
                        print("loaded file " + args[1])
                        print("Global Max is " + str(global_max))
                        print("Global Min is " + str(global_min))
                        normalized_ts_dict = normalize_ts_with_min_max(time_series_dict, global_min, global_max)

                        # gp_project.save_time_series(time_series_dict, normalized_ts_dict, args[1])  # TODO include load history
                        gp_project.save_time_series(time_series_dict, normalized_ts_dict, res_list)

            elif args[0] == 'save':  # TODO save changes to the GenexPlusProject pickle file
                print("saved")

            elif args[0] == 'group':
                if gp_project is None:
                    gp_not_opened_error()
                elif sc is None:
                    sparkContextNotSetError()
                else:
                    global_dict = sc.broadcast(gp_project.normalized_ts_dict)
                    time_series_dict = sc.broadcast(gp_project.time_series_dict)
                    global_dict_rdd = sc.parallelize(res_list[1:], numSlices=16)

                    # TODO only grouping full length
                    grouping_range = (1, max([len(v) for v in global_dict.value.values()]))

                    group_start_time = time.time()
                    group_rdd = global_dict_rdd.flatMap(
                        lambda x: get_subsquences(x, grouping_range[0], grouping_range[1])).map(
                        lambda x: (x[0], [x[1:]])).reduceByKey(
                        lambda a, b: a + b)

                    group_end_time = time.time()
                    print('group of timeseries from ' + str(grouping_range[0]) + ' to ' + str(
                        grouping_range[1]) + ' using ' + str(
                        group_end_time - group_start_time) + ' seconds')
                    group_rdd_res = group_rdd.collect()

                    gp_project.set_group_data(group_rdd_res)

                    print("grouping done, saved to dataset")

            # elif args[0] == 'group':  # group all loaded data
            #     if gp_project == None:
            #         err_msg = FormattedText([
            #             ('class:error', 'No GenexPlus Project Opened, please use the command open to open a GenexPlus Project'),
            #         ])
            #         print_formatted_text(err_msg, style=style)
            #     else:

            elif args[0] == 'show':  # TODO
                if gp_project is None:
                    gp_not_opened_error()
                else:
                    for entry in gp_project.get_load_history():
                        print(entry)

            elif args[0] == 'exit':  # for user input 'exit'
                message = FormattedText([
                    ('class:normal', 'Exited'),
                ])

                print_formatted_text(message, style=style)
                break

    print_formatted_text(message, style=style)

    # TODO progress bar
