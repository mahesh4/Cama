import calendar
import datetime
import json
import math
import os.path, shutil
import subprocess
import DropObj
import glob

import numpy
from math import sin, cos, sqrt, atan2, radians

from db_connect import DBConnect

YEAR = ""  # the year to evaluate
PRE_PATH = ""  # file path to the pre-restoration modelling results
POST_PATH = ""  # file path to the post-restoration modelling results
LAT = 0
LON = 0
LAT_MAT = [0]
LON_MAT = [0]


# Mahesh: Done
def pos2dis(lat1, lon1, lat2, lon2):
    # approximate radius of earth in km
    R = 6373.0

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance


# Mahesh: Done
def is_number(in_string):
    try:
        float(in_string)
        return True
    except ValueError:
        return False


# Mahesh: Done
def set_configuration(new_config):
    global YEAR, PRE_PATH, POST_PATH, LAT, LON

    if "year" in new_config:
        YEAR = new_config["year"]
    if "pre_path" in new_config:
        path = new_config["pre_path"].split("/")
        folder_name = path[1]
        file_name = path[2]
        DropObj.download_file(folder_name, file_name)
        PRE_PATH = os.path.join(os.getcwd(), "output", folder_name, file_name)
    if "post_path" in new_config:
        path = new_config["post_path"].split("/")
        folder_name = path[1]
        file_name = path[2]
        DropObj.download_file(folder_name, file_name)
        POST_PATH = os.path.join(os.getcwd(), "output", folder_name, file_name)
    if "lat" in new_config:
        LAT = new_config["lat"]
    if "lon" in new_config:
        LON = new_config["lon"]


# Mahesh: Done
def init_matrix(rows, cols, init_val):
    # noinspection PyUnusedLocal
    return [[init_val for i in range(cols)] for j in range(rows)]


# Mahesh: Done
def days_in_year(year):
    if calendar.isleap(year):
        return 366
    return 365


# Mahesh: Done
def coord_to_grid_cell(p_lat=0.0, p_lon=0.0):
    global LAT, LON

    if p_lat == 0:
        p_lat = LAT
    if p_lon == 0:
        p_lon = LON

    lat_baseline = 34.95
    lon_baseline = 104.05
    return math.floor((lat_baseline - p_lat) * 10) * 90 + math.floor(((lon_baseline + p_lon) * 10) + 1)


# Mahesh: Done
def veg_to_manning(veg_type=""):
    veg_type = veg_type.lower()
    if veg_type == "crop" or veg_type == "crops":
        return 0.05
    elif veg_type == "pasture" or veg_type == "pastures":
        return 0.05
    elif veg_type == "bush" or veg_type == "bushes":
        return 0.16
    elif veg_type == "tree" or veg_type == "trees":
        return 0.2
    else:
        return None  # not a recognized type of vegetation


def update_manning(p_lat, p_lon, p_riv_base, p_riv_new, p_fld_base, p_fld_new, size_wetland):
    # this function updates the manning parameters of the post-restoration model
    # you will need to call the model and scrape the results yourself manually

    cell = coord_to_grid_cell(p_lat, p_lon) - 1  # must offset by 1; this is very sensitive in the raw binary

    # 1) we pull the number of indices from the river height file
    file = open("/var/lib/model/CaMa_Post/map/hamid/rivhgt.bin", 'r')
    index_count = len(numpy.fromfile(file, dtype=numpy.float32))
    file.close()

    # 2) we set all the values to a new base value
    new_riv = numpy.full((index_count, 1), p_riv_base, dtype=numpy.float32)

    # 3) set the specific grid cell to a value double the baseline (?)
    new_riv[cell] = p_riv_new

    # 4) save that to the 'river manning' file
    new_riv.tofile("/var/lib/model/CaMa_Post/map/hamid/rivman.bin")

    # 5) set all values to a different, new base value
    new_fld = numpy.full((index_count, 1), p_fld_base, dtype=numpy.float32)

    # 6) set the specific grid cell to yet another specified manning coefficient
    new_fld[cell] = p_fld_new

    # 7) save that as the 'floodplain manning' file
    new_fld.tofile("/var/lib/model/CaMa_Post/map/hamid/fldman.bin")

    # 8) update the fldhgt.bin
    file = "/var/lib/model/CaMa_Pre/map/hamid/lonlat"
    # file = "/Users/magesh/Documents/Cama/CaMa_Brazos/map/hamid/lonlat"  # for testing in local machine
    lon_lat_1 = numpy.loadtxt(file, usecols=range(2))
    lon_lat = lon_lat_1

    for i in range(9):
        lon_lat = numpy.vstack([lon_lat_1, lon_lat])

    file = open("/var/lib/model/CaMa_Pre/map/hamid/fldhgt_original.bin", "r")
    # file = open("/Users/magesh/Documents/Cama/CaMa_Brazos/map/hamid/fldhgt_original.bin", "r")  # for testing in
    # local machine
    fldhgt_original = numpy.fromfile(file, dtype=numpy.float32)
    file.close()

    lon_lat = numpy.insert(lon_lat, 2, fldhgt_original[0: lon_lat.shape[0]], axis=1)

    lon_lat_4 = lon_lat

    file = "/var/lib/model/CaMa_Pre/map/hamid/wetland_loc_multiple"
    # file = "/Users/magesh/Documents/Cama/CaMa_Brazos/map/hamid/wetland_loc_multiple"  # for testing in local machine
    lon_lat_5 = numpy.loadtxt(file, usecols=range(2))

    for k in range(3, 4):
        lon_5 = lon_lat_5[k, 1]
        lat_5 = lon_lat_5[k, 0]

        lon_lat_2 = lon_lat_1[:, 0:2]
        lon_lat_2 = numpy.insert(lon_lat_2, 2, 0, axis=1)

        for j in range(0, lon_lat_2.shape[0]):
            lon = lon_lat_2[j, 0]
            lat = lon_lat_2[j, 1]
            lon_lat_2[j, 2] = pos2dis(lat_5, lon_5, lat, lon)

        lon_lat_3 = lon_lat_2[lon_lat_2[:, 2].argsort()]

        location = numpy.where((lon_lat[:, 0] == lon_lat_3[0, 0]) & (lon_lat[:, 1] == lon_lat_3[0, 1]))
        lon_lat_4[location[0: size_wetland + 1], 2] = lon_lat[location[0: size_wetland + 1], 2] - 1.5

    with open('/var/lib/model/CaMa_Post/map/hamid/fldhgt.bin', 'w') as fp:
        lon_lat_4[:, 2].astype('float32').tofile(fp)
        print(lon_lat_4.shape)
        fp.close()


def update_wetland(flow_value):
    file_path = "/var/lib/model/CaMa_Pre/map/hamid/wetland_loc_multiple"
    wetland_loc_multiple = numpy.loadtxt(file_path, usecols=range(2))

    file_path = "/var/lib/model/CaMa_Pre/map/hamid/lonlat"
    lon_lat = numpy.loadtxt(file_path)

    # Finding nearest lon_lat to the wetland location
    distance = [pos2dis(wetland_loc_multiple[0][0], wetland_loc_multiple[0][1], location[0], location[1]) for location
                in lon_lat]
    min_lonlat_index = distance.index(min(distance))

    input_path = "/var/lib/model/CaMa_Post/inp/hamid"
    for filename in glob.glob(os.path.join(input_path, '*.bin')):
        file_no = int(filename.split('/')[-1].split('.')[0][7:])
        if file_no <= 20111001:
            with open(filename, 'r') as f:
                flood_input = numpy.fromfile(f, dtype=numpy.float32)
                f.close()

            flood_input[min_lonlat_index] += flow_value

            with open(filename, 'w') as f:
                flood_input.tofile(f)
                f.close()


def delta_max_q_y(p_cell=0):
    global YEAR, PRE_PATH, POST_PATH, LAT, LON

    if not str(YEAR).isdigit():
        raise ValueError("No configuration available for this conversion; use 'set_configuration'.")

    if p_cell == 0:
        p_cell = coord_to_grid_cell()

    grid_number = p_cell
    day_count = days_in_year(YEAR)
    flow_denom = 90 * 61

    raw_pre_input = numpy.fromfile(open(PRE_PATH, 'r'), dtype=numpy.float32)
    raw_post_input = numpy.fromfile(open(POST_PATH, 'r'), dtype=numpy.float32)
    pre_restore_flow = [0] * day_count
    post_restore_flow = [0] * day_count
    for day in range(day_count):
        input_index = numpy.mod(grid_number, flow_denom) + (flow_denom * day)
        pre_restore_flow[day] = raw_pre_input[input_index - 1]
        post_restore_flow[day] = raw_post_input[input_index - 1]

    post_restore_flow_max = numpy.amax(post_restore_flow)

    # compute the difference between the results, in the week surrounding the annual peak
    max_index = post_restore_flow.index(post_restore_flow_max)
    current_day_range = 1  # always start by looking at the preceding or following day, not the current day itself
    left_index = max_index - current_day_range
    right_index = max_index + current_day_range
    delta_max_result = max(pre_restore_flow[max_index] - post_restore_flow[max_index], 0)
    while left_index >= 0 and right_index < day_count and current_day_range < 6 and \
            (pre_restore_flow[left_index] > post_restore_flow[left_index] or
             pre_restore_flow[right_index] > post_restore_flow[right_index]):
        left_max = max(pre_restore_flow[left_index] - post_restore_flow[left_index], 0)
        right_max = max(pre_restore_flow[right_index] - post_restore_flow[right_index], 0)
        delta_max_result += left_max + right_max
        current_day_range += 1
        left_index = max_index - current_day_range
        right_index = max_index + current_day_range

    return delta_max_result


def delta_min_q_y(p_cell=0):
    global YEAR, PRE_PATH, POST_PATH, LAT, LON

    if not str(YEAR).isdigit():
        raise ValueError("No configuration available for this conversion; use 'set_configuration'.")

    if p_cell == 0:
        p_cell = coord_to_grid_cell()

    grid_number = p_cell
    day_count = days_in_year(YEAR)
    flow_denom = 90 * 61

    # let's measure the pre-restoration base flow
    raw_input = numpy.fromfile(open(PRE_PATH, 'r'), dtype=numpy.float32)
    pre_restore_flow = [0] * day_count
    weekly_flow = [0] * (day_count - 6)
    for day in range(day_count):
        input_index = numpy.mod(grid_number, flow_denom) + (flow_denom * day)
        pre_restore_flow[day] = raw_input[input_index - 1]

    for day in range(day_count - 6):
        weekly_flow[day] = sum(pre_restore_flow[day:day + 6])
    week_start = weekly_flow.index(min(weekly_flow))
    pre_avg_min = numpy.average(weekly_flow[week_start:week_start + 6])

    # now we measure the post-restoration base flow (which we expect to have risen)
    raw_input = numpy.fromfile(open(POST_PATH, 'r'), dtype=numpy.float32)
    post_restore_flow = [0] * day_count
    weekly_flow = [0] * (day_count - 6)
    for day in range(day_count):
        input_index = numpy.mod(grid_number, flow_denom) + (flow_denom * day)
        post_restore_flow[day] = raw_input[input_index - 1]

    for day in range(day_count - 6):
        weekly_flow[day] = sum(post_restore_flow[day:day + 6])
    week_start = weekly_flow.index(min(weekly_flow))
    post_avg_min = numpy.average(weekly_flow[week_start:week_start + 6])

    return post_avg_min - pre_avg_min


# Mahesh: Done
def plot_hydrograph_from_wetlands():
    global PRE_PATH, POST_PATH, LAT, LON

    grid_cell = coord_to_grid_cell()

    # plot the data after transforming it
    line1 = map_input_to_flow(PRE_PATH, grid_cell, 0, True)
    line2 = map_input_to_flow(POST_PATH, grid_cell, 0, True)
    return line1, line2


# Mahesh: Done
def map_input_to_flow(file_path, grid_cell, p_year=0, p_clean=False):
    global YEAR

    if p_year == 0:
        p_year = YEAR

    raw_input = numpy.fromfile(open(file_path, 'r'), dtype=numpy.float32)

    if p_clean:
        # ensure that all overly-large values are zeroed out
        for i in range(len(raw_input)):
            if raw_input[i] > 100000:
                raw_input[i] = 0

    year_days = days_in_year(p_year)

    flow = []
    k = 0
    for i in range(year_days):
        k += 1
        idx = (grid_cell % (90 * 61)) + (90 * 61 * i) - 1
        flow.append(raw_input[idx].item())

    return flow


def build_flow_grids():
    # fixed nextxy path for server. Currently configured to cama_pre
    global LAT_MAT, LON_MAT

    # load ancillary data, including reservoir locations and mappings
    next_xy_raw = numpy.loadtxt('/var/lib/model/Resources/nextxy.txt', usecols=range(2))
    next_xx = next_xy_raw[:, 0]
    next_yy = next_xy_raw[:, 1]

    # divvy up the next_xx and next_yy arrays into columns
    # noinspection PyUnusedLocal
    LAT_MAT = [[0 for i in range(90)] for j in range(61)]
    # noinspection PyUnusedLocal
    LON_MAT = [[0 for i in range(90)] for j in range(61)]
    for ctr in range(1, 62):
        first = ((ctr - 1) * 90) + 1
        second = (ctr * 90)
        LON_MAT[ctr - 1] = next_xx[first - 1:second - 1]
        LAT_MAT[ctr - 1] = next_yy[first - 1:second - 1]


def grid_cell_of_wetlands_outlet(p_lat=0.0, p_lon=0.0):
    global LAT, LON, LAT_MAT, LON_MAT

    if p_lat == 0:
        p_lat = LAT
    if p_lon == 0:
        p_lon = LON

    coord_offset = coord_to_grid_cell(p_lat, p_lon)

    if len(LAT_MAT) == 1 | len(LON_MAT) == 1:
        build_flow_grids()  # we haven't cached the flow grids yet

    # find the wetlands outlet, and use that to calculate the offset
    xx_temp = int(coord_offset % 90) + 1
    yy_temp = math.floor(coord_offset / 90) + 1
    candidate = int(xx_temp + (yy_temp * 90)) + 1
    return candidate  # and now we finally have a grid_offset that we can use in our main function


def grid_cell_of_river_mouth(p_lat=0.0, p_lon=0.0):
    global LAT, LON, LAT_MAT, LON_MAT

    if p_lat == 0:
        p_lat = LAT
    if p_lon == 0:
        p_lon = LON

    coord_offset = coord_to_grid_cell(p_lat, p_lon)

    if len(LAT_MAT) == 1 | len(LON_MAT) == 1:
        build_flow_grids()  # we haven't cached the flow grids yet

    # find the nearest reservoir, and use that to calculate the offset
    xx_temp = int(coord_offset % 90) + 1
    yy_temp = math.floor(coord_offset / 90) + 1
    found_mouth = False
    candidate = 0
    while not found_mouth:
        candidate = xx_temp + ((yy_temp - 1) * 90)
        results = (int(LON_MAT[yy_temp - 1][xx_temp - 1]), int(LAT_MAT[yy_temp - 1][xx_temp - 1]))
        xx_temp = results[0]
        yy_temp = results[1]
        if xx_temp == -9999:
            found_mouth = True
    return candidate  # and now we finally have a grid_offset that we can use in our main function


def grid_cell_of_reservoir(p_lat=0.0, p_lon=0.0):
    # fixed reservoir path for server deployment
    global LAT, LON, LAT_MAT, LON_MAT

    if p_lat == 0:
        p_lat = LAT
    if p_lon == 0:
        p_lon = LON

    coord_offset = coord_to_grid_cell(p_lat, p_lon)

    # note: reservoir locations are [lon,lat], in contradiction of ISO 6709
    reservoir_raw = numpy.loadtxt('/var/lib/model/Resources/Reservoir_xy.txt', usecols=range(2))
    reservoir_offset = []
    for row in range(len(reservoir_raw)):
        reservoir_offset.append(coord_to_grid_cell(reservoir_raw[row][1], reservoir_raw[row][0]))

    if len(LAT_MAT) == 1 | len(LON_MAT) == 1:
        build_flow_grids()  # we haven't cached the flow grids yet

    # find the nearest reservoir, and use that to calculate the offset
    xx_temp = int(coord_offset % 90) + 1
    yy_temp = math.floor(coord_offset / 90) + 1
    found_reservoir = False
    candidate = 0
    while not found_reservoir:
        candidate = xx_temp + ((yy_temp - 1) * 90)
        if candidate not in reservoir_offset:
            results = (int(LON_MAT[yy_temp - 1][xx_temp - 1]), int(LAT_MAT[yy_temp - 1][xx_temp - 1]))
            xx_temp = results[0]
            yy_temp = results[1]
        else:
            found_reservoir = True
    return candidate  # and now we finally have a grid_offset that we can use in our main function


# Mahesh: Done
def plot_hydrograph_nearest_reservoir(p_lat=0.0, p_lon=0.0):
    global LAT, LON, YEAR, PRE_PATH, POST_PATH

    if p_lat == 0:
        p_lat = LAT
    if p_lon == 0:
        p_lon = LON

    grid_offset = grid_cell_of_reservoir(p_lat, p_lon)

    line1 = map_input_to_flow(PRE_PATH, grid_offset, YEAR, True)
    line2 = map_input_to_flow(POST_PATH, grid_offset, YEAR, True)
    return line1, line2


def delta_max_all():
    # run three times to compare:
    line1 = delta_max_q_y(grid_cell_of_wetlands_outlet()) * 3600 * 24  # 1) the wetlands outlet
    line2 = delta_max_q_y(grid_cell_of_reservoir()) * 3600 * 24  # 2) the nearest reservoir
    line3 = delta_max_q_y(grid_cell_of_river_mouth()) * 3600 * 24  # 3) the river mouth
    return line1, line2, line3


def config_cama(p_year=0):
    global YEAR

    # this function is for configuring the post-restoration ONLY
    # this is because all the pre-restoration results have been pre-computed

    if p_year == 0:
        p_year = YEAR

    if p_year < 1915 or p_year > 2011:
        return "Invalid CAMA year: " + str(p_year)

    try:
        file = open('/var/lib/cama/model/cama/gosh/cama_template.sh')
        cama_config = file.read()
        file.close()
        cama_config = cama_config.replace("<YEAR>", str(p_year))
        file = open('/var/lib/cama/model/cama/gosh/Distributed_Manning_1915-2011_postWR.sh', 'w')
        file.write(cama_config)
        file.close()
    except IOError as e:
        return "IOError:" + str(e)
    return "Success"


# Mahesh: Done
def peak_flow(folder_name, p_lat=0.0, p_lon=0.0, floodpeak=10):
    """Returns a year which has maximal difference / minimum flow(working)"""
    global LAT, LON

    if p_lat == 0:
        p_lat = LAT
    if p_lon == 0:
        p_lon = LON

    grid_cell = coord_to_grid_cell(p_lat, p_lon)
    # grid_cell = 3674  # DEBUG *****

    # what kind of flood peak window are we using
    if floodpeak == 10:
        logbase = numpy.log(numpy.log(10 / 9))  # 10-year flood
    elif floodpeak == 100:
        logbase = numpy.log(numpy.log(100 / 99))  # 100-year flood
    else:
        return 0

    # for each year in the range
    year_peaks = [0] * 97
    for i in range(1916, 2010):
        # Downloading the file from dropbox
        DropObj.download_file(folder_name, "outflw" + str(i) + ".bin")
        output_file = os.path.join(os.getcwd(), "output", folder_name, "outflw" + str(i) + ".bin")
        year_flow = map_input_to_flow(output_file, grid_cell, i, False)
        year_peaks[i - 1915] = max(year_flow)

    # calculate the gumbel distribution
    flow_mean = numpy.nanmean(year_peaks)
    flow_sdev = numpy.nanstd(year_peaks, ddof=1)  # ddof=1 emulates matlab's bias-compensation default
    kt_gumbel = ((-6 ** 0.5) / 3.14) * (0.5772 + logbase)
    xt_gumbel = flow_mean + (kt_gumbel * flow_sdev)

    # find the year with the maximal difference / minimum flow
    curr_year = 1914
    min_year = 0
    min_val = float("inf")
    for val in year_peaks:
        curr_year += 1
        this_flow = numpy.abs(xt_gumbel - val)
        if this_flow < min_val:
            min_year = curr_year
            min_val = this_flow
    return min_year


def run_cama(p_model, folder_name, metadata, mongo_client):
    # expects cama to be pre-configured
    try:
        database = mongo_client["output"]
        files_collection = database["files"]
        # Check if there is no existing model running
        file = list(files_collection.find({"status": "running", "model": p_model}))
        if len(file) > 0:
            return -1

        # Check if there exist no such document with the folder_name in the DB and in dropbox
        file = files_collection.find_one({"folder_name": folder_name})
        if file is None and not DropObj.folder_exists(folder_name):
            new_file = dict({"model": p_model, "status": "running", "folder_name": folder_name, "metadata": metadata})
            files_collection.insert_one(new_file)

            # Starting the execution of the model
            if p_model == "preflow":
                subprocess.Popen("sudo /var/lib/model/CaMa_Pre/gosh/hamid.sh", shell=True)
            elif p_model == "postflow_wetland" or p_model == "postflow_groundwater":
                subprocess.Popen("sudo /var/lib/model/CaMa_Post/gosh/hamid.sh", shell=True)
            else:
                raise Exception("Invalid model")
        else:
            return -2

    except Exception as e:
        raise e

    return 1


def clean_up():
    """Deleting all content of the output folder"""
    folder = os.path.join(os.getcwd(), "output")
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


def cama_status(folder_name, mongo_client):
    try:
        database = mongo_client["output"]
        files_collection = database["files"]
        file = files_collection.find_one({"folder_name": folder_name})
        if file is None:
            return "Record doesn't exist"
        else:
            return file["status"]
    except Exception as e:
        raise e


def get_flow(p_year, model, folder_name, mongo_client):
    p_year = str(p_year)
    if model == "pre":
        base_path = "/var/lib/model/CaMa_Pre/out/hamid/"
        out_path = base_path + "outflw<YEAR>.bin".replace("<YEAR>", p_year)

    elif model == "post":
        base_path = "/var/lib/model/CaMa_Post/out/hamid/"
        out_path = base_path + "outflw<YEAR>.bin".replace("<YEAR>", p_year)

    else:
        return "Invalid model-type selected"

    if os.path.isfile(out_path):
        with open(out_path, 'r') as file:
            outflow = numpy.fromfile(file, dtype=numpy.float32)
            file.close()
        return outflow

    else:
        return "output_flow doesn't exist for the year " + p_year


def delete_results(folder_name, mongo_client):
    database = mongo_client["output"]
    files_collection = database["files"]
    file = files_collection.find_one({"folder_name": folder_name})
    if file is not None and DropObj.folder_exists(folder_name):
        DropObj.delete_folder(folder_name)
        files_collection.delete_one({"_id": file["_id"]})
        return 1
    else:
        return -1


def do_request(p_request_json, mongo_client):
    check_inputs = True
    if p_request_json["request"] == "veg_lookup" or \
            p_request_json["request"] == "update_manning" or \
            p_request_json["request"] == "cama_status" or \
            p_request_json["request"] == "cama_set" or \
            p_request_json["request"] == "peak_flow" or \
            p_request_json["request"] == "coord_to_grid" or \
            p_request_json["request"] == "cama_run" or \
            p_request_json["request"] == "delete_results" or \
            p_request_json["request"] == "update_wetland":
        check_inputs = False

    try:
        if check_inputs:
            mandatory_keys = ["request", "pre_path", "post_path", "year", "lat", "lon"]
            numeric_keys = ["lat", "lon", "year"]
            given_keys = p_request_json.keys()
            for this_key in mandatory_keys:
                if this_key not in given_keys:
                    print("Missing required input key: " + this_key)
                    return

            for this_key in numeric_keys:
                if not is_number(p_request_json[this_key]):
                    print("Expected number, received: " + this_key + "=" + p_request_json[this_key])
                    return

            # startup and configuration
            config = dict()
            config["pre_path"] = p_request_json["pre_path"]
            config["post_path"] = p_request_json["post_path"]
            config["year"] = int(p_request_json["year"])
            config["lat"] = float(p_request_json["lat"])
            config["lon"] = float(p_request_json["lon"])
            set_configuration(config)

        result = None
        if p_request_json["request"] == "plot_hydrograph_from_wetlands":
            result = plot_hydrograph_from_wetlands()
        elif p_request_json["request"] == "plot_hydrograph_nearest_reservoir":
            result = plot_hydrograph_nearest_reservoir(p_request_json["lat"], p_request_json["lon"])
        elif p_request_json["request"] == "peak_flow":
            result = peak_flow(p_request_json["folder_name"], p_request_json["lat"], p_request_json["lon"],
                               p_request_json["return_period"])
        elif p_request_json["request"] == "plot_hydrograph_deltas":
            result = delta_max_all()
        elif p_request_json["request"] == "veg_lookup":
            result = veg_to_manning(p_request_json["veg_type"])
        elif p_request_json["request"] == "coord_to_grid":
            result = coord_to_grid_cell(p_request_json["lat"], p_request_json["lon"])
        elif p_request_json["request"] == "update_manning":
            result = dict()
            result["succeeded"] = False
            try:
                update_manning(p_request_json["lat"], p_request_json["lon"], p_request_json["riv_pre"],
                               p_request_json["riv_post"], p_request_json["fld_pre"], p_request_json["fld_post"],
                               p_request_json['size_wetland'])
                result["succeeded"] = True
            except Exception as e:
                print(e)
                pass
        elif p_request_json["request"] == "cama_status":
            result = dict()
            response = cama_status(p_request_json["folder_name"], mongo_client)
            result["message"] = response
        elif p_request_json["request"] == "cama_set":
            result = dict()
            response = config_cama(p_request_json["year"])
            if response == "Success":
                result["succeeded"] = True
            else:
                result["succeeded"] = False
                result["message"] = response
        elif p_request_json["request"] == "cama_run":
            result = dict()
            status = run_cama(p_request_json["model"], p_request_json["folder_name"], p_request_json["metadata"], mongo_client)
            if status == -1:
                result["message"] = "There is already a model in execution, pls wait"
            elif status == -2:
                result["message"] = "The folder_name already exists in dropbox, choose a unique folder_name"
            else:
                result["message"] = "Execution queued"
        elif p_request_json["request"] == "get_flow":
            result = get_flow(p_request_json['year'], p_request_json['model_type'])
        elif p_request_json["request"] == "update_wetland":
            update_wetland(p_request_json["flow_value"])
            result = dict()
            result["message"] = "Flow updated successfully"
        elif p_request_json["request"] == "delete_results":
            status = delete_results(p_request_json["folder_name"], mongo_client)
            result = dict()
            if status == -1:
                result["message"] = "Folder doesn't exist in Dropbox or mongoDB"
            else:
                result["message"] = "Delete operation successful"
        else:
            print("Invalid API request: " + p_request_json["request"])  # no valid API request

        # Cleaning up the files folder
        clean_up()

        if result is not None:
            return json.dumps(result)  # this is where the data actually is sent back to the API
    except Exception as e:
        print("An exception occurred:" + str(e))


if __name__ == '__main__':
    db = DBConnect()
    db.connect_db()
    mongo_client = db.get_connection()

    payload = dict({
        "folder_name": "test_preflow",
        "request": "delete_results"
    })

    print(do_request(payload, mongo_client))
    db.disconnect_db()
