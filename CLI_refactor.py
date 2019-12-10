"""
prompt_toolkit: https://python-prompt-toolkit.readthedocs.io/en/master/pages/printing_text.html

open: will open existing workspace. If designated workspace doesn't exist, it will notify user a new
    ex: $ open my_project
load: load target data set in to the workspace - put data set onto slaves and do the grouping
    load command does not allow duplicate IDs

set: set sparkcontext of the system, this is required for the following operations: group, cluster and query
    arguements: 1. path to java home, 2. number of cores

group: group the loaded time series
    group function will check if a time series with given id is grouped already, it will only group time series that are at status: loaded

cluster: cluster the grouped subsequences

query:


Commands for getting information:

get <something>

something can be:
    id: get the IDs and the status of all the loaded time series
    log

"""
import os
import shutil
import time

from prompt_toolkit import prompt, print_formatted_text
from prompt_toolkit.formatted_text import FormattedText, PygmentsTokens
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import WordCompleter, Completion, Completer
# import click
import fuzzyfinder
from prompt_toolkit.styles import Style
import pickle

# commands
from pyspark import SparkContext, SparkConf
from GenexPlusProject import GenexPlusProject
from cluster_operations import cluster
from data_operations import normalize_ts_with_min_max, get_data
from filter_operation import exclude_same_id, include_in_range
from group_operations import generate_source, get_subsquences
from query_operations import query
from visualize_sequences import plot_query_result

GPKeywords = ['load', 'group', 'cluster', 'query', 'plot', 'help', 'exit', 'open', 'set', 'delete']
# open: if the folder name is not none, we go into it and recursively load whatever left there
# eg: cluster, group, dict, and print out which of those are not empty
# if the folder name doesn't exist, we create a new folder

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


def no_query_result_before_plot():
    err_msg = FormattedText([
        ('class:error',
         'Please get query result before plot it'),
    ])
    print_formatted_text(err_msg, style=style)


def create_new_project(SAVED_DATASET_DIR, args):
    isCreateNewProject = prompt("Project " + args[
        1] + " does not exit, would you like to create a new GenexPlus Project? [y/n]")
    if isCreateNewProject == 'y':
        # creating new GenexPlus project
        print("Creating " + args[1])
        try:
            os.makedirs(SAVED_DATASET_DIR + os.sep + args[1])
        except FileExistsError:
            # directory already exists
            pass


def load_broadcast_infor(normalized_ts_dict, time_series_dict, res_list):
    if sc is None:
        spark_context_not_set_error()
    else:

        global_dict = sc.broadcast(normalized_ts_dict)
        # change naming here from ts_dict to global_time_series_dict
        # because it might cause problem when saving
        global_time_series_dict = sc.broadcast(time_series_dict)
        global_dict_rdd = sc.parallelize(res_list[1:],
                                         numSlices=128)  # change the number of slices to mitigate larger datasets

        grouping_range = (1, max([len(v) for v in global_dict.value.values()]))


class GPCompleter(Completer):
    def get_completions(self, document, complete_evennt):
        word_before_cursor = document.get_word_before_cursor(WORD=True)
        matches = fuzzyfinder(word_before_cursor, GPKeywords)
        for m in matches:
            yield Completion(m, start_position=-len(word_before_cursor))


GPCompleter = WordCompleter(['load', 'group', 'cluster', 'query', 'plot'],
                            ignore_case=True)
SAVED_DATASET_DIR = './res/saved_dataset'

gp_project = None  # GenexPlus workspace
java_home_path = None
sc = None

features_to_append = [0, 1, 2, 3, 4]  # TODO what is this?
grouping_range = None
print("Java Home Path is set to None")

ts_dict = None
normalized_ts_dict = None
ts_list = None

group_rdd = None
cluster_rdd = None
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
                    found_project = False
                    if os.path.isdir(SAVED_DATASET_DIR + os.sep + args[1]):
                        print("Project already exist")
                        found_project = True
                    else:
                        create_new_project(SAVED_DATASET_DIR, args)
                        gp_project = GenexPlusProject(args[1])

                    if found_project:
                        working_dir = None
                        sub_dir_list = [os.path.join(SAVED_DATASET_DIR, o) for o in os.listdir(SAVED_DATASET_DIR)
                                        if os.path.isdir(os.path.join(SAVED_DATASET_DIR, o))]
                        for valid_work_sub_dir in sub_dir_list:
                            if args[1] in valid_work_sub_dir:
                                working_dir = valid_work_sub_dir
                                print("find a matching directory" + valid_work_sub_dir)
                                index = valid_work_sub_dir.rfind(os.sep)
                                gp_project = GenexPlusProject(valid_work_sub_dir[index + 1:])
                                msg = FormattedText([
                                    ('class:file', "find a matching directory" + valid_work_sub_dir)
                                ])
                                print_formatted_text(msg, style=style)
                        if working_dir:
                            # find a name matching, want to verify which part is already done
                            try:
                                if os.path.isdir(working_dir + '/dict/'):

                                    preprocess_path = working_dir + '/dict/'
                                    if os.path.getsize(preprocess_path + 'time_series.pickle') > 0:
                                        print(
                                            "try to load time series information from " + working_dir + '/dict/' + "...")
                                        with open(preprocess_path + 'time_series.pickle', 'rb') as handle:
                                            ts_dict = pickle.load(handle)
                                    if os.path.getsize(preprocess_path + 'normalized_time_series.pickle') > 0:
                                        with open(preprocess_path + 'normalized_time_series.pickle', 'rb') as handle:
                                            normalized_ts_dict = pickle.load(handle)
                                    if os.path.getsize(preprocess_path + 'ts_list.pickle') > 0:
                                        with open(preprocess_path + 'ts_list.pickle', 'rb') as handle:
                                            ts_list = pickle.load(handle)
                                    if ts_dict is not None and normalized_ts_dict is not None and ts_list is not None:

                                        # print("load time series information successfully from " + preprocess_path)
                                        msg = FormattedText([
                                            ('class:file',
                                             "load time series information successfully from " + preprocess_path)
                                        ])
                                        print_formatted_text(msg, style=style)

                                    else:
                                        ts_dict = None
                                        normalized_ts_dict = None
                                        ts_list = None
                                        err_msg = FormattedText([
                                            ('class:error',
                                             "some part of processing information missed, make them all empty")
                                        ])
                                        print_formatted_text(err_msg, style=style)
                                        # print("some part of processing information missed")
                                #         Group
                                if os.path.isdir(working_dir + '/group/'):
                                    if os.path.getsize(working_dir + '/group/') > 0:
                                        print("try to load group information from " + working_dir + '/group/' + "...")
                                        if sc:
                                            group_rdd = sc.pickleFile(working_dir + '/group/')
                                            print("load group information successfully")
                                        else:
                                            spark_context_not_set_error()
                                    else:
                                        print("group information missed")
                                #         Cluster
                                if os.path.isdir(working_dir + '/cluster/'):
                                    if os.path.getsize(working_dir + '/cluster/') > 0:
                                        print(
                                            "try to load cluster information from " + working_dir + '/cluster/' + "...")
                                        # cluster_rdd_reload = sc.pickleFile(working_dir + '/cluster/')
                                        # print("load cluster information successfully")
                                        if sc:
                                            cluster_rdd = sc.pickleFile(working_dir + '/cluster/')
                                            print("load group information successfully")
                                        else:
                                            spark_context_not_set_error()
                                    else:
                                        print("cluster information missed")


                            except FileNotFoundError:
                                print("one of the three folder is empty")


                        else:
                            print("the project is empty 2")
                        # might be none here, so we must check if it's none or not, otherwise it will cause
                        # Ran out of input error
                        # https://stackoverflow.com/questions/24791987/why-do-i-get-pickle-eoferror-ran-out-of-input-reading-an-empty-file
                        # if os.path.getsize(gp_project_fn) > 0:
                        #     with open(gp_project_fn, 'rb') as f:
                        #         gp_project = pickle.load(f)
                        # else:
                        #     print("This file is empty")

            elif args[0] == 'close':  # close opened gp_project
                if gp_project is not None:
                    print("Closing " + gp_project.get_project_name())
                    gp_project = None

            elif args[0] == 'set':  # set Java Home
                if len(args) != 2:  # if wrong number of arguments is given
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
                    print("Java home set at " + java_home_path)

            elif args[0] == 'load':  # load given csv file
                if gp_project is None:
                    gp_not_opened_error()
                if sc is None:
                    spark_context_not_set_error()
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
                        update_pre_processing = False
                        if ts_list is not None and ts_dict is not None and normalized_ts_dict is not None:

                            is_Update_pre_infor = prompt("Project " + args[
                                1] + " pre-processing information exist, would you like to update ? [y/n]")

                            if is_Update_pre_infor == 'y':
                                # creating new GenexPlus project
                                update_pre_processing = True
                                print("update pre-processing information " + args[1])

                        # change here
                        if update_pre_processing or not ts_list or not ts_dict or not normalized_ts_dict:
                            ts_list, global_min, global_max = generate_source(args[1], features_to_append)

                            print("loaded file " + args[1])
                            print("Global Max is " + str(global_max))
                            print("Global Min is " + str(global_min))

                            #  get a normalize version of the time series
                            norm_ts_list = normalize_ts_with_min_max(ts_list, global_min, global_max)

                            global_norm_list = sc.parallelize(norm_ts_list)


            elif args[0] == 'save':  # TODO save changes to the GenexPlusProject pickle file
                path_to_save = SAVED_DATASET_DIR + os.sep + gp_project.get_project_name()
                if not os.path.isdir(SAVED_DATASET_DIR + os.sep + gp_project.get_project_name()):
                    os.mkdir(path_to_save)

                if ts_list is not None and ts_dict is not None and normalized_ts_dict is not None:
                    print("saving pre-processing information")

                    t_filename = path_to_save + '/dict/' + 'time_series.pickle'
                    os.makedirs(os.path.dirname(t_filename), exist_ok=True)
                    with open(path_to_save + '/dict/' + 'time_series.pickle', 'wb') as handle:
                        pickle.dump(ts_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)

                    n_filename = path_to_save + '/dict/' + 'normalized_time_series.pickle'
                    os.makedirs(os.path.dirname(n_filename), exist_ok=True)
                    with open(path_to_save + '/dict/' + 'normalized_time_series.pickle', 'wb') as handle:
                        pickle.dump(normalized_ts_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)

                    r_filename = path_to_save + '/dict/' + 'ts_list.pickle'
                    os.makedirs(os.path.dirname(r_filename), exist_ok=True)
                    with open(path_to_save + '/dict/' + 'ts_list.pickle', 'wb') as handle:
                        pickle.dump(ts_list, handle, protocol=pickle.HIGHEST_PROTOCOL)
                    # gp_project_file = open(gp_project_fn, "wb")
                    # pickle.dump(gp_project, gp_project_file)
                    print("preprocessing information saved to " + path_to_save + "/dict/")
                if sc is None:
                    spark_context_not_set_error()

                if group_rdd:
                    if os.path.isdir(path_to_save + '/group/'):
                        is_Update_group_infor = prompt(
                            "Project " + gp_project.get_project_name() + "r's group folder information exist, would you like to update ? [y/n]")
                        if is_Update_group_infor == 'y':
                            # creating new GenexPlus project
                            shutil.rmtree(path_to_save + '/group/')
                            group_rdd.saveAsPickleFile(path_to_save + '/group/')

                    else:
                        group_rdd.saveAsPickleFile(path_to_save + '/group/')
                    print("group rdd information saved to " + path_to_save + '/group/')
                else:
                    print("group not yet done")
                if cluster_rdd:
                    if os.path.isdir(path_to_save + '/cluster/'):
                        is_Update_cluster_infor = prompt(
                            "Project " + gp_project.get_project_name() + "r's cluster folder information exist, would you like to update ? [y/n]")
                        if is_Update_cluster_infor == 'y':
                            # creating new GenexPlus project
                            shutil.rmtree(path_to_save + '/cluster/')
                            cluster_rdd.saveAsPickleFile(path_to_save + '/cluster/')
                    else:
                        cluster_rdd.saveAsPickleFile(path_to_save + '/cluster/')
                    print("cluster rdd information saved to " + path_to_save + '/cluster/')

                else:
                    print("cluster not yet done")

                print("Saving process finished")


            elif args[0] == 'gac':  # gac stands for group&cluster
                if gp_project is None:
                    gp_not_opened_error()

                elif sc is None:
                    spark_context_not_set_error()
                else:
                    grouping_range = (1, max([len(v) for v in dict(norm_ts_list).values()]))
                    group_rdd = global_norm_list.flatMap(
                        lambda x: get_subsquences(x, grouping_range[0], grouping_range[1])).map(
                        lambda x: (x[0], [x[1:]])).reduceByKey(
                        lambda a, b: a + b)
                    global_norm_dict = sc.broadcast(dict(norm_ts_list))
                    cluster_rdd = group_rdd.map(lambda x: cluster(x[1], x[0], 0.1, global_norm_dict.value))


            elif args[0] == 'group':
                if gp_project is None:
                    gp_not_opened_error()
                elif sc is None:
                    spark_context_not_set_error()
                elif ts_list is None or ts_dict is None or normalized_ts_dict is None:
                    get_arg_error()
                else:
                    update_group = False
                    if group_rdd:

                        is_Update_group_infor = prompt(
                            "Project " + gp_project.get_project_name() + "r's group information exist, would you like to update ? [y/n]")
                        if is_Update_group_infor == 'y':
                            # creating new GenexPlus project
                            update_group = True
                            print("update group information for" + gp_project.get_project_name())
                    # only group the time series that are at status: loaded
                    # ts_dict_to_be_grouped = gp_project.get_ungrouped_ts_dict()
                    if update_group or group_rdd is None:
                        # norm_ts_list was set and parallelized in load
                        grouping_range = (1, max([len(v) for v in dict(norm_ts_list).values()]))

                        group_start_time = time.time()
                        # global_norm_list was set in load
                        group_rdd = global_norm_list.flatMap(
                            lambda x: get_subsquences(x, grouping_range[0], grouping_range[1])).map(
                            lambda x: (x[0], [x[1:]])).reduceByKey(
                            lambda a, b: a + b)


                        group_end_time = time.time()
                        print('group of timeseries from ' + str(grouping_range[0]) + ' to ' + str(
                            grouping_range[1]) + ' using ' + str(
                            group_end_time - group_start_time) + ' seconds')
                        # group_rdd_res = group_rdd.collect()

                        # gp_project.set_group_data(group_rdd_res, (group_end_time - group_start_time))

                        # print("grouping done, saved to dataset")

            elif args[0] == 'cluster':
                if gp_project is None:
                    gp_not_opened_error()
                elif sc is None:
                    spark_context_not_set_error()
                elif group_rdd is None:
                    no_group_before_cluster_error()
                # elif gp_project.ts_dict is None:
                #     no_group_before_cluster_error()
                else:
                    update_cluster = False
                    if cluster_rdd:
                        is_Update_cluster_infor = prompt(
                            "Project " + gp_project.get_project_name() + "r's cluster information exist, would you like to update ? [y/n]")
                        if is_Update_cluster_infor == 'y':
                            # creating new GenexPlus project
                            update_cluster = True
                            print("update cluster information for project" + gp_project.get_project_name())
                    if update_cluster or cluster_rdd is None:
                        print("Working on clustering")
                        cluster_start_time = time.time()
                        global_normalized_dict = sc.broadcast(normalized_ts_dict)
                        # change naming here from ts_dict to global_time_series_dict
                        # because it might cause problem when saving
                        global_dict = sc.broadcast(ts_dict)
                        global_dict_rdd = sc.parallelize(ts_list[1:],
                                                         numSlices=128)  # change the number of slices to mitigate larger datasets

                        grouping_range = (1, max([len(v) for v in global_normalized_dict.value.values()]))

                        # cluster_rdd_collected = cluster_rdd.collect()
                        # first_dict = cluster_rdd_reload[0]
                        # gp_project.set_cluster_data(cluster_rdd)  # save cluster information to the projecty

                        cluster_end_time = time.time()

                        print('clustering of timeseries from ' + str(grouping_range[0]) + ' to ' + str(
                            grouping_range[1]) + ' using ' + str(cluster_end_time - cluster_start_time) + ' seconds')
                        # TODO Question: why do we call cluster on the global_dict?
                        cluster_rdd = group_rdd.map(lambda x: cluster(x[1], x[0], 0.1,
                                                                      global_normalized_dict.value))  # TODO have the user decide the similarity threshold

                        # print("clustering done, saved to dataset")


            elif args[0] == 'get':
                if gp_project is None:
                    gp_not_opened_error()
                    continue

                if len(args) != 2:
                    get_arg_error()
                else:
                    if args[1] == 'ts':  # get the ids and status of all the loaded time series
                        gp_project.print_ts()

                    elif args[1] == 'log':
                        gp_project.print_log()
                    # elif args[1] == ''  # TODO add more argument type to the get command

                    else:
                        print(args[1] + " is not a valid arguement for the command get")


            elif args[0] == 'query':  # TODO check if the ts's are clustered
                # resolve picked query

                # if len(args) != 4 or len(args) != 5:
                #
                #
                # if args[1] == 'bf':
                #
                if cluster_rdd is None:
                    no_cluster_before_query_error()
                else:
                    print("querying ")
                    global_normalized_dict = sc.broadcast(normalized_ts_dict)
                    # change naming here from ts_dict to global_time_series_dict
                    # because it might cause problem when saving
                    global_dict = sc.broadcast(ts_dict)
                    global_dict_rdd = sc.parallelize(ts_list[1:],
                                                     numSlices=128)  # change the number of slices to mitigate larger datasets

                    grouping_range = (1, max([len(v) for v in global_normalized_dict.value.values()]))
                    # print("grouping_range" + str(grouping_range))
                    query_id = '(2013e_001)_(100-0-Back)_(A-DC4)_(232665953.1250)_(232695953.1250)'
                    query_sequence = get_data(query_id, 24, 117, global_dict.value)  # get an example query
                    print(len(query_sequence))
                    # cluster_rdd.collect()
                    # repartition(16).
                    # raise exception if the query_range exceeds the grouping range
                    # TODO after getting range and filtering, repartition!!
                    querying_range = (90, 91)
                    k = 5  # looking for k best matches
                    print("start query")
                    if querying_range[0] < grouping_range[0] or querying_range[1] > grouping_range[1]:
                        raise Exception("query_operations: query: Query range does not match group range")
                    filter_rdd = cluster_rdd.filter(lambda x: include_in_range(x, querying_range)).filter(
                        lambda x: exclude_same_id(x, query_id)).repartition(32)

                    # clusters = cluster_rdd.collect()
                    # query_result = cluster_rdd.filter(lambda x: x).map(lambda clusters: query(query_sequence, querying_range, clusters, k, ts_dict.value)).collect()
                    exclude_overlapping = True
                    query_result = filter_rdd.map(
                        lambda clusters: query(query_sequence, querying_range, clusters, k,
                                               global_dict.value,
                                               exclude_overlapping,
                                               0.5)).collect()
                    # changed here
                    # plot_query_result(query_sequence, query_result, global_time_series_dict.value)

            elif args[0] == 'plot':
                if query_result is None:
                    no_query_result_before_plot()
                else:
                    plot_query_result(query_sequence, query_result, global_dict.value)
                    print("plot done")

            elif args[0] == 'exit':  # for user input 'exit'
                message = FormattedText([
                    ('class:normal', 'Exited'),
                ])

                print_formatted_text(message, style=style)

                # cleaning up
                if sc is not None:
                    sc.stop()
                break

            elif args[0] == 'delete':
                # if gp_project is None:
                #     message = FormattedText([
                #         ('class:error', 'please open a project first in order to remove'),
                #     ])
                #
                #     print_formatted_text(message, style=style)
                #
                # else:
                if len(args) != 2:  # if wronge number of arguments is given
                    err_msg = FormattedText([
                        ('class:error',
                         'Wrong number of arguments, please specify the project to be deleted)'),
                    ])
                    print_formatted_text(err_msg, style=style)
                else:

                    is_remove_project = prompt("Are you sure to remove everything in project  " + args[
                        1] + " ? [y/n]")
                    if is_remove_project == 'y':
                        # creating new GenexPlus project
                        update_cluster = True
                        print("try delete project " + args[1])
                    try:
                        shutil.rmtree(SAVED_DATASET_DIR + os.sep + args[1])
                        print("project " + args[1] + "deleted")
                    except FileNotFoundError:
                        err_msg = FormattedText([
                            ('class:error',
                             'there is no such project, please double check')])
                        print_formatted_text(err_msg, style=style)

    print_formatted_text(message, style=style)

    # TODO progress bar
