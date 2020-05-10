import csv, sqlite3
import pandas as pd
import utm

class databaseReader:
  PVID_INDEX = 0
  SLOPE_INDEX = 21
  def __init__(self, database_name):
    self.con = sqlite3.connect(database_name)
    self.cur = self.con.cursor()

  def get_record_by_id(self, idno):
    self.cur.execute("select * from link_data where linkPVID = %d"%(idno))
    rows = self.cur.fetchall()
    return rows

  def filter_matching_links(self, latitude, longitude):
    query = 'select * from link_data where min_latitude < %f and max_latitude\
            > %f and min_longitude < %f and max_longitude > %f'%(latitude, latitude,
                                                                 longitude, longitude)

    self.cur.execute(query)
    rows = self.cur.fetchall()
    print("rows: ")
    print(rows)
    return rows

  def insert_slope_data(self, linkrecord, newslope):

    print('link_record $$$$$$$$$$$$')
    print(linkrecord)
    # linkPVID = linkrecord[self.PVID_INDEX]
    linkPVID = linkrecord
    print('linkPVID')
    print(linkPVID)
    if(linkrecord):
      slope_string = str(linkrecord) + ',' + str(newslope)
    else:
      slope_string = str(newslope)
    slope_string = str(newslope)
    query = " UPDATE link_data SET slope = '%s' WHERE linkPVID=%d;"%(slope_string, int(linkPVID))

    self.cur.execute(query)
    self.con.commit()

  def __del__(self):
    self.con.close()


def map_match(links, probe):
    # Design map matching algorithm and return the mathched link

    closest_node_dist_sqr = 1000
    probe_lat = probe[2]
    probe_lon = probe[3]
    probe_utm_e, probe_utm_n,_,_= utm.from_latlon(float(probe_lat), float(probe_lon))
    closest_link = 0;
    closest_sub_link = 0;
    for link in links:
        node_list = []
        node_list.append(link[14].split('|'))
        node_list = node_list[0]
        for node in node_list:
            node_info = []
            node_info = node.split('/')
            # convert link nodes to cartesian cord
            utm_e, utm_n,_,_ = utm.from_latlon(float(node_info[0]), float(node_info[1]))
            if (utm_e - probe_utm_e)**2 + (utm_n - probe_utm_n)**2 < closest_node_dist_sqr:
                # save link PVID of closest link
                closest_link = link
                closest_node_dist_sqr = (utm_e - probe_utm_e)**2 + (utm_n - probe_utm_n)**2

                # find sub_link
                if node == node_list[0]:
                    closest_sub_link = 0
                else:
                    node_zero_info = []
                    node_zero_info = node_list[0].split('/')
                    zero_utm_e, zero_utm_n,_,_ = utm.from_latlon(float(node_zero_info[0]), float(node_zero_info[1]))
                    # compare distance of node to refNode and probe to refNode
                    if (utm_e - zero_utm_e)**2 + (utm_n - zero_utm_n)**2 < (probe_utm_e- zero_utm_e)**2 + (probe_utm_n- zero_utm_n)**2 :
                        closest_sub_link = node_list.index(node)
                    else:
                        closest_sub_link = node_list.index(node) - 1


    return closest_link, closest_sub_link

def calculate_slope(probe_data, prev_probe_data):
    # calculate the slope of the point with probe data
    # We define slope by the change in altitude between probe points divided by
    # the change in distance (sqrt(X**2 + Y**2)
    probe_lat = probe_data[2]
    probe_lon = probe_data[3]
    probe_alt = probe_data[4]
    probe_utm_e, probe_utm_n,_,_ = utm.from_latlon(float(probe_lat), float(probe_lon))

    prev_probe_lat = prev_probe_data[2]
    prev_probe_lon = prev_probe_data[3]
    prev_probe_alt = prev_probe_data[4]
    prev_probe_utm_e, prev_probe_utm_n,_,_ = utm.from_latlon(float(prev_probe_lat), float(prev_probe_lon))

    x_y_dist_change = ((probe_utm_e - prev_probe_utm_e)**2 + (probe_utm_n - prev_probe_utm_n)**2)**(0.5)
    alt_change = probe_alt - prev_probe_alt

    slope = alt_change / x_y_dist_change

    print('slope(((((((((((((((((())))))))))))))))))')
    print(slope)

    return slope

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
                    link_slope = str(link[0]) + ', NA'
                    all_link_slope_info.append(link_slope)
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
                    node_alt_change = node_alt - prev_node_alt
                    node_slope = node_alt_change / node_x_y_dist_change

                    sub_link_slope = str(node_count) + ':' + str(node_slope)
                    link_slope_info.append(sub_link_slope)
                    node_count += 1
                    prev_node_info = node_info

        get_link_slope()
        all_link_slope_info.append(link_slope_info)

    return all_link_slope_info



def analyse_probedata(probe_csv_path, op_csv_path):
    linkdatabase = databaseReader('roadlink_4.db')
    probe_iterator =  pd.read_csv(probe_csv_path, sep=',', header = None, chunksize=1)
    previous_link_match = None
    previous_probe_point = [None for i in range(7)]

    for probe_data in probe_iterator:

        # All probe details
        sampleID = int(probe_data[0].values[0])
        dateTime = probe_data[1].values[0]
        sourcecode = int(probe_data[2].values[0])
        latitude = float(probe_data[3].values[0])
        longitude = float(probe_data[4].values[0])
        altitude = float(probe_data[5].values[0])
        speed = float(probe_data[6].values[0])
        heading = float(probe_data[7].values[0])
        probe_data_formatted = [sampleID, dateTime, latitude, longitude, altitude,
                               speed, heading]
        print(probe_data)

        # Primitive filtering of link data
        relevant_links = linkdatabase.filter_matching_links(latitude, longitude)

        # core function
        matched_link, matched_sub_link = map_match(relevant_links, probe_data_formatted)
        if previous_probe_point[0] == probe_data_formatted[0] and\
           previous_link_match == matched_link:
          slope_value = calculate_slope(probe_data_formatted, previous_probe_point)
          linkdatabase.insert_slope_data(matched_link, slope_value)

        # Update map matching data
        direction = 'T'
        reference_node_distance = 10
        distance_from_link = 11

        print('matched_link')
        print(matched_link)
        probe_data.insert(8, 'linkPVID', matched_link)
        probe_data.insert(9, 'direction', direction)
        probe_data.insert(10, 'distFromRef', reference_node_distance)
        probe_data.insert(11, 'distFromLink', distance_from_link)

        # Write results back to a csv file

        probe_data.to_csv(op_csv_path,
             index=False,
             header=False,
             mode='a',#append data to csv file
             chunksize=1)


        previous_probe_point = probe_data_formatted
        previous_link_match = matched_link

        # Write slope values back to db


if __name__=='__main__':

    probe_path = 'Partition6467ProbePoints.csv'
    op_path = 'op_mathced.csv'
    analyse_probedata(probe_path, op_path)
