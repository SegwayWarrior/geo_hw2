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
    probe_utm_e, probe_utm_n, probe_zone_num, probe_zone_let = utm.from_latlon(float(probe_lat), float(probe_lon))

    closest_link = 0;
    for link in links:
        print('link^^^^')
        print(link[14])
        node_list = []
        node_list.append(link[14].split('|'))
        node_list = node_list[0]
        print('node_list %%%%')
        print(node_list)
        for node in node_list:
            print('node!!!!!!')
            print(node)
            node_info = []
            node_info = node.split('/')
            # convert to cartesian cord
            utm_e, utm_n, zone_num, zone_let = utm.from_latlon(float(node_info[0]), float(node_info[1]))
            if (utm_e - probe_utm_e)**2 + (utm_n - probe_utm_n)**2 < closest_node_dist_sqr:
                # save link PVID of closest link
                closest_link = int(link[0])
                closest_node_dist_sqr = (utm_e - probe_utm_e)**2 + (utm_n - probe_utm_n)**2


    return closest_link

def calculate_slope(probe_data):
    # calculate the slope of the point with probe data

    return 4.5

def analyse_probedata(probe_csv_path, op_csv_path):
    linkdatabase = databaseReader('roadlink_4.db')
    probe_iterator =  pd.read_csv(probe_csv_path, sep=',', header = None, chunksize=1)
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
        matched_link = map_match(relevant_links, probe_data_formatted)
        slope_value = calculate_slope(probe_data_formatted)

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


        linkdatabase.insert_slope_data(matched_link, slope_value)

        # Write slope values back to db




if __name__=='__main__':

    probe_path = 'Partition6467ProbePoints.csv'
    op_path = 'op_mathced.csv'
    analyse_probedata(probe_path, op_path)
