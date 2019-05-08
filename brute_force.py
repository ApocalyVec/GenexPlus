from group_operations import generate_source

features_to_append = [0, 1, 2, 3, 4]
res_list, time_series_dict, global_min, global_max = generate_source('2013e_001_2_channels_02backs.csv', features_to_append)
print()
