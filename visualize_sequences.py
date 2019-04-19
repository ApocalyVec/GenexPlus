import matplotlib.pyplot as plt
from data_operations import get_data
# The problem is we want to query the cluster given length and id
# since the dictionary key is the object, we might want to change it to
# a certain format like: id_length_num
# where num is the cluster_count
def plot_cluster(cluster_dict, dict_number, time_series_dict, sequence_number):

    for dict_with_same_length in cluster_dict[0: dict_number]:
        for key,value in dict_with_same_length.items():

            plt.figure(figsize=(15, 15))
            for ss in value[0:sequence_number]:
                val = get_data(ss.id, ss.start_point, ss.end_point, time_series_dict.value)
                if ss.is_representative :
                    plt.plot(val, label='id %s' % ss.get_group_represented() + ss.id + '_'+  str(ss.start_point) + '_' + str(ss.end_point))
                else:
                    plt.plot(val, label='id %s' % ss.get_group_represented() + ss.id + '_'+ str(ss.start_point) + '_' + str(ss.end_point))

            plt.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.show()