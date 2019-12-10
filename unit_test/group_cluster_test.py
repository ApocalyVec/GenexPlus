from pyspark import SparkContext, SparkConf
from GenexPlusProject import GenexPlusProject
from cluster_operations import cluster
from data_operations import normalize_ts_with_min_max, get_data
from filter_operation import exclude_same_id, include_in_range
from group_operations import generate_source, get_subsquences
from query_operations import query
from visualize_sequences import plot_query_result
import time

conf = SparkConf().setAppName("GenexPlus").setMaster("local[*]")  # using all available cores
sc = SparkContext(conf=conf)

features_to_append = [0,1,2,3,4]
ts_list, global_min, global_max = generate_source('2013e_001_2_channels_02backs.csv', features_to_append)
norm_ts_list = normalize_ts_with_min_max(ts_list, global_min, global_max)

global_norm_list = sc.parallelize(norm_ts_list)
grouping_range = (1, max([len(v) for v in dict(norm_ts_list).values()]))


# group operation
group_rdd = global_norm_list.flatMap(
    lambda x: get_subsquences(x, grouping_range[0], grouping_range[1])).map(
    lambda x: (x[0], [x[1:]])).reduceByKey(
    lambda a, b: a + b)

group_result = group_rdd.collect()
global_norm_dict = sc.broadcast(dict(norm_ts_list))

# start = time.time()
# for group_entry in group_result:
#     result = cluster(group_entry[1], group_entry[0], 0.1, dict(norm_ts_list))
# end = time.time()
# print('Clustering all group WITHOUT Spark took ' + str(end - start) + ' seconds')


start = time.time()
cluster_rdd = group_rdd.map(lambda x: cluster(x[1], x[0], 0.1, global_norm_dict.value)).collect()
end = time.time()
print('Clustering all group WITH Spark took ' + str(end - start) + ' seconds')
