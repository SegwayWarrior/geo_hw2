import utm
import pandas as pd
import csv

op_csv_path = 'link_slope_groundtruth.csv'
list = [0,1,2]
df = pd.DataFrame(list)
df.to_csv(op_csv_path,
     index=False,
     header=False,
     mode='w',#write data to csv file
     chunksize=1)

def calculate_link_slope(links):
    all_link_slope_info = []
    for link in links:
        node_list = []

        node_list.append(link[14].split('|'))
        node_list = node_list[0]
        link_slope_info = []
        link_slope_info.append(link[0])
        def get_link_slope():

            node_count = 0
            prev_node_info = []
            for node in node_list:
                node_info = []
                node_info = node.split('/')
                if node_info[2] == '':
                    # No alt provided so slope is NA
                    link_slope = 'NA'
                    link_slope_info.append(link_slope)
                    return

                if node == node_list[0]:
                    prev_node_info = node_info

                if node != node_list[0]:
                    node_lat = node_info[0]
                    node_lon = node_info[1]
                    node_alt = node_info[2]
                    node_utm_e, node_utm_n,_,_ = utm.from_latlon(float(node_lat), float(node_lon))

                    prev_node_lat = prev_node_info[0]
                    prev_node_lon = prev_node_info[1]
                    prev_node_alt = prev_node_info[2]
                    prev_node_utm_e, prev_node_utm_n,_,_ = utm.from_latlon(float(prev_node_lat), float(prev_node_lon))


                    node_x_y_dist_change = ((node_utm_e - prev_node_utm_e)**2 + (node_utm_n - prev_node_utm_n)**2)**(0.5)
                    node_alt_change = float(node_alt) - float(prev_node_alt)
                    node_slope = node_alt_change / node_x_y_dist_change

                    sub_link_slope = str(node_count) + ':' + str(node_slope)
                    link_slope_info.append(sub_link_slope)
                    node_count += 1
                    prev_node_info = node_info

        get_link_slope()
        all_link_slope_info.append(link_slope_info)

    return all_link_slope_info

link_data = (pd.read_csv("probe_data_map_matching/Partition6467LinkData.csv",header=None)).values
# print(link_data)
link_list = calculate_link_slope(link_data)
# print(link_list)

op_csv_path = 'link_slope_groundtruth.csv'
df = pd.DataFrame(link_list)
df.to_csv(op_csv_path,
     index=False,
     header=False,
     mode='w',#write data to csv file
     chunksize=1)
