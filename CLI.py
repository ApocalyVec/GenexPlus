"""
prompt_toolkit: https://python-prompt-toolkit.readthedocs.io/en/master/pages/printing_text.html

open: will open existing workspace. If designated workspace doesn't exist, it will notify user a new
    ex: $ open my_project
load: load target data set in to the workspace - put data set onto slaves and do the grouping
    load command does not allow duplicate IDs

set: set sparkcontext of the system, this is required for the following operations: group, cluster and query
    arguements: 1. path to java home, 2. number of cores

group: group the loaded time series

cluster: cluster the grouped subsequences

query:


Commands for getting information:

get <something>

something can be:
    id: get all

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
from pyspark import SparkContext, SparkConf

from CLIExceptions import DuplicateIDError
from GenexPlusProject import GenexPlusProject
from cluster_operations import cluster
from data_operations import normalize_ts_with_min_max, get_data
from filter_operation import exclude_same_id
from group_operations import generate_source, get_subsquences
from query_operations import query
from visualize_sequences import plot_query_result

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


def get_arg_error():
    err_msg = FormattedText([
        ('class:error', 'Please group the data before clustering'),
    ])
    print_formatted_text(err_msg, style=style)

def load_file_not_found_error(missing_file):
    err_msg = FormattedText([
        ('class:error',
         'File ' + missing_file + ' not found'),
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
                        gp_project_fn = args[1] + '.pickle'
                        gp_project_file = open(gp_project_fn, "rb")
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
                if len(args) != 2:  # if wronge number of arguments is given
                    err_msg = FormattedText([
                        ('class:error',
                         'Wrong number of arguments, please specify the path to Java Home (the number of cores is no '
                         'longer needed, the program will use all available cores)'),
                    ])
                    print_formatted_text(err_msg, style=style)
                else:
                    java_home_path = args[1]
                    os.environ['JAVA_HOME'] = java_home_path

                    conf = SparkConf().setAppName("GenexPlus").setMaster("local[*]")  # using all available cores
                    sc = SparkContext(conf=conf)
                    # sc = SparkContext('' + 'local' + '[' + str(num_cores) + ']' + '', "GenexPlus")
                    print("Java home set at " + java_home_path)

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

                    elif not os.path.isfile(args[1]):
                        load_file_not_found_error(args[1])
                    else:

                        time_series_list, time_series_dict, global_min, global_max = generate_source(args[1],
                                                                                                     features_to_append)
                        print("loaded file " + args[1])
                        print("Global Max is " + str(global_max))
                        print("Global Min is " + str(global_min))
                        normalized_ts_dict = normalize_ts_with_min_max(time_series_dict, global_min, global_max)

                        # gp_project.save_time_series(time_series_dict, normalized_ts_dict, args[1])  # TODO include load history
                        try:
                            gp_project.load_time_series(time_series_dict, normalized_ts_dict, time_series_list)
                        except DuplicateIDError as e:
                            err_msg = FormattedText([
                                ('class:error',
                                 'Error: duplicate ID(s) found in existing time series and newly loaded time series, dupplicate ID(s):'),
                                ('class:error', str(list(((duplicate_id + "\n") for duplicate_id in e.duplicate_id_list))))
                            ])

                            print_formatted_text(err_msg, style=style)


            elif args[0] == 'save':  # TODO save changes to the GenexPlusProject pickle file

                gp_project_file = open(gp_project_fn, "wb")
                pickle.dump(gp_project, gp_project_file)

                print("saved")

            elif args[0] == 'group':
                if gp_project is None:
                    gp_not_opened_error()
                elif sc is None:
                    spark_context_not_set_error()
                else:
                    global_dict = sc.broadcast(gp_project.normalized_ts_dict)
                    time_series_dict = sc.broadcast(gp_project.time_series_dict)
                    global_dict_rdd = sc.parallelize(gp_project.time_series_list[1:],
                                                     numSlices=128)  # change the number of slices to mitigate larger datasets

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

            elif args[0] == 'cluster':
                if gp_project is None:
                    gp_not_opened_error()
                elif sc is None:
                    spark_context_not_set_error()
                elif gp_project.time_series_dict is None:
                    no_group_before_cluster_error()
                else:
                    print("Working on clustering")
                    cluster_start_time = time.time()
                    # TODO Question: why do we call cluster on the global_dict?
                    cluster_rdd = group_rdd.map(lambda x: cluster(x[1], x[0], 0.1,
                                                                  global_dict.value))  # TODO have the user decide the similarity threshold

                    cluster_rdd.collect()
                    # first_dict = cluster_rdd_reload[0]
                    gp_project.set_cluster_data(cluster_rdd)  # save cluster information to the projecty

                    cluster_end_time = time.time()

                    print('clustering of timeseries from ' + str(grouping_range[0]) + ' to ' + str(
                        grouping_range[1]) + ' using ' + str(cluster_end_time - cluster_start_time) + ' seconds')

                    print("clustering done, saved to dataset")


            elif args[0] == 'get':
                if gp_project is None:
                    gp_not_opened_error()
                    continue

                if len(args) != 2:
                    get_arg_error()
                else:
                    if args[1] == 'id':
                        if gp_project.time_series_dict is not None:
                            for key in gp_project.time_series_dict.keys():
                                print(key)
                        else:
                            print("No time series ID available since no time series has been loaded")
                            print("Use the load command to load time series")
                    elif args[1] == 'log':
                        gp_project.print_log()
                    # elif args[1] == ''  # TODO add more argument type to the get command

                    else:
                        print(args[1] + " is not a valid arguement for the command get")

            elif args[0] == 'query':
                print("querying ")

                query_id = '(2013e_001)_(100-0-Back)_(A-DC4)_(232665953.1250)_(232695953.1250)'
                query_sequence = get_data(query_id, 24, 117, time_series_dict.value)  # get an example query
                filter_rdd = cluster_rdd.filter(lambda x: exclude_same_id(x, query_id))
                # raise exception if the query_range exceeds the grouping range
                querying_range = (90, 91)
                k = 5  # looking for k best matches
                if querying_range[0] < grouping_range[0] or querying_range[1] > grouping_range[1]:
                    raise Exception("query_operations: query: Query range does not match group range")

                # query_result = cluster_rdd.filter(lambda x: x).map(lambda clusters: query(query_sequence, querying_range, clusters, k, time_series_dict.value)).collect()
                exclude_overlapping = True
                query_result = filter_rdd.map(
                    lambda clusters: query(query_sequence, querying_range, clusters, k, time_series_dict.value,
                                           exclude_overlapping,
                                           0.5)).collect()

                plot_query_result(query_sequence, query_result, time_series_dict.value)

            elif args[0] == 'exit':  # for user input 'exit'
                message = FormattedText([
                    ('class:normal', 'Exited'),
                ])

                print_formatted_text(message, style=style)

                # cleaning up
                if sc is not None:
                    sc.stop()
                break

    print_formatted_text(message, style=style)

    # TODO progress bar
