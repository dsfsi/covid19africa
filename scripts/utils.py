# This script will be used to update cases, deaths, recovered from 
# https://twitter.com/AfricaCDC daily updates in the form of images

import sys
import os
from os import listdir
from os.path import isfile, join
import pandas as pd
import re
import argparse
import yaml
from datetime import datetime, timedelta
import cv2
from PIL import Image

import pytesseract
# Point this to where you installed Tesseract executable for OCR
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
africa_cdc_path = "data/time_series/africa_cdc/"
timeseries_path = "data/time_series/"
# This is a hack for now, it will be replaced with similarity measures for more elegant accomodation of variants
# Variants happen due to people mistyping or OCR artifacts
freq_missed = {"Cape Verde":"Cabo Verde", "ORC" : "DRC", "Cdte d'ivoire": "Cote d'ivoire", "Cdte d'Ivoire": "Cote d'ivoire", 
                                "Cate d'ivoire" : "Cote d'ivoire", "Cote d'Ivoire" : "Cote d'ivoire", "Cate d'Ivoire" : "Cote d'ivoire", 
                                "Céte d'ivoire" : "Cote d'ivoire", "Céte d'Ivoire" : "Cote d'ivoire", "Cte d'Ivoire" : "Cote d'ivoire",
                                "S20 Tome & Principe" : "Sao Tome & Principe", "Sa0 Tome & Principe" : "Sao Tome & Principe"}

case_type = ["cases", "deaths", "recovered", "tests"]

def print_inter_diff(data_f, data):
    # matched text from images
    txt = list(data.keys())
    txt.sort()
    # country names from the CSV files
    x = data_f[0]
    country_names = x.index
    print(list(country_names))
    print(len(country_names))

    print(list(txt))
    print(len(txt))

    inxn = list(set(txt) & set(country_names))
    print(inxn)
    print(len(inxn))

    diff = set(txt).difference(set(country_names))
    print(diff)
    print(len(diff))

def parse_args():
    cmd_parser = argparse.ArgumentParser()
    cmd_parser.add_argument("-i", "--input_path", default="img/",
                            help="path to the image file to parse data from")
    cmd_parser.add_argument("-m", "--mode", default="batch",
                            help="Whether to batch process many days images or single day image")
    args = vars(cmd_parser.parse_args())

    return args

def get_filenames(files_path="", files_base=""):
    str_ = files_path + files_base + "_"
    str_ += '{0}.csv'
    files = list(map(str_.format, case_type))
    print(files)
    return files

def get_africa_cdc_filenames(files_path=africa_cdc_path, files_base="africa_cdc_daily_time_series"):
    return get_filenames(files_path, files_base)

def get_timeseries_filenames(files_path=timeseries_path, files_base="africa_daily_time_series"):
    return get_filenames(files_path, files_base)

def read_time_series():
    data_f, files = read_africa_cdc_time_series()
    return data_f[:3], files[:3]

def read_africa_cdc_time_series(use_country_asid=True):
    files = get_africa_cdc_filenames()
    # read the files
    if use_country_asid:
        data_f = [pd.read_csv(f, index_col='Country/Region', encoding = "ISO-8859-1", keep_default_na=False) for f in files]
    else:
        data_f = [pd.read_csv(f, encoding = "ISO-8859-1", keep_default_na=False) for f in files]
    # df = data_f[0]
    # print(df)
    # print(df.loc[df['Country/Region'] == "Namibia"])
    # print(data_f)
    return data_f, files

def update_time_series(data_f="", data="", date_txt=""):
    # data_f[0] is cases, data_f[1] is deaths, data_f[2] is recovered
    for df, i in zip(data_f, range(len(data_f))):
        # First, check to see if the date column exists
        # print("The existing data columns are: \n")
        # print(df.columns)
        if date_txt not in df.columns:
            print("The date doesn't exist!!! Creating a new column")
            # If date doesn't exist, create a new column with the current date
            df[date_txt] = 0
        else:
            print("The date exists! Updating existing column.")
        # update the column data
        keys = data.keys()
        for row in df.index:
            if row in keys:
                # print("Country: {}, Data: {}".format(row,i))
                if len(data[row]) == 3:
                    df.at[row, date_txt] = data[row][i] 
        # print(df)

    return data_f

def write_time_series(data="", files=""):
    for df, f in zip(data, files):
        df.to_csv(f, encoding = "utf-8")

def unpivot_timeseries():
    keys = ["Confirmed Cases", "Deaths", "Recovered Cases", "Tests"]
    df_unp = "unpivoted_dataframe"
    # First, get all the 4 files, unpivoted and sorted
    # filenames = get_mixed_timeseries_filenames()
    # filenames = get_africa_cdc_filenames()
    # print(filenames)
    dfs, filenames = read_africa_cdc_time_series(use_country_asid=False) #[pd.read_csv(filenames[i], keep_default_na=False) for i in range(len(keys))]

    df_cases_orig = dfs[0]
    rows = df_cases_orig.shape[0]
    
    for i in range(len(keys)):
        row, tw_sum, lw_sum, diff = [], [], [], []
        for j in range(rows):
            old_col = dfs[i].columns[-1] # Grab the last day (last column name)
            twfd_idx = dfs[i].columns[-7] # Grab the first day of this week (column name)
            lwld_idx = dfs[i].columns[-8] # Grab the last day of last week (column name)
            lwfd_idx = dfs[i].columns[-14] # Grab the first day of last week (column name)
            
            d = datetime.strptime(old_col, '%m/%d/%Y')
            one_day = timedelta(days=1)
            d -= one_day
            col = d.strftime('%#m/%#d/%Y')
            # Now grab the previous day value and subtract it from current day's value
            # to get the daily increase
            a = dfs[i].at[j, old_col]
            b = dfs[i].at[j, col]
            # Now grab this week values sum them up
            # to get the total for this week
            tw = sum(list(dfs[i].loc[j, twfd_idx:old_col]))
            # Now grab this week values sum them up
            # to get the total for this week
            lw = sum(list(dfs[i].loc[j, lwfd_idx:lwld_idx]))

            #print("Today value: {}".format(a))
            #print("Yesterday value: {}".format(b))
            #print("Difference between this week and last week: {}".format(tw-lw))
            #print("% Difference between this week and last week: {}".format(100*(tw-lw)/lw))
            
            row.append(int(a) - int(b))
            tw_sum.append(tw)
            lw_sum.append(lw)
            d = 0
            if not(lw == 0):
                d = 100*(tw-lw)/lw
            diff.append(d)

        dfs[i].insert(loc=dfs[i].columns.get_loc(old_col)+1, column="Daily Values", value=row)
        dfs[i].insert(loc=dfs[i].columns.get_loc(old_col)+2, column="Last Week Values", value=lw_sum)
        dfs[i].insert(loc=dfs[i].columns.get_loc(old_col)+3, column="This Week Values", value=tw_sum)
        dfs[i].insert(loc=dfs[i].columns.get_loc(old_col)+4, column="Diff Values", value=diff)
    
    data = {keys[i]:{"filename":filenames[i], \
                     "df": dfs[i], \
                      df_unp: dfs[i].melt(id_vars=["Country/Region", "iso2", "iso3", "Subregion", "Population-2020", "Lat", "Long", "Daily Values", "Last Week Values", "This Week Values", "Diff Values"], var_name="Date", value_name="Values"), \
                    } for i in range(len(keys))
            }
    #print(data)
    df_cases = data[keys[0]][df_unp]
    # print(df_cases.loc[df_cases['Country/Region'] == "Namibia"])
    print(df_cases.head())
    print(df_cases.shape)
    # Insert the extract columns, rename columns, etc
    rows = df_cases.shape[0]
    for key in keys:
        data[key][df_unp].insert(loc=0, column="Group", value=[key for i in range(rows)])
        data[key][df_unp].insert(loc=data[key][df_unp].columns.get_loc("Group")+1, column="Province State", value=["" for i in range(rows)])
        data[key][df_unp].insert(loc=data[key][df_unp].columns.get_loc("Long")+1, column="Location Geom", value=["POINT({} {})".format(data[key][df_unp].at[i, "Long"],data[key][df_unp].at[i, "Lat"]) for i in range(rows)])
        data[key][df_unp].insert(loc=data[key][df_unp].columns.get_loc("Location Geom")+1, column="Continent", value=["Africa" for i in range(rows)])
        data[key][df_unp].insert(loc=data[key][df_unp].columns.get_loc("Continent")+1, column="Continent Code", value=["AF" for i in range(rows)])
        data[key][df_unp].insert(loc=data[key][df_unp].columns.get_loc("Continent Code")+1, column="Region", value=[data[key][df_unp].at[i, "Subregion"] + " Africa" for i in range(rows)])
        data[key][df_unp].insert(loc=data[key][df_unp].columns.get_loc("Region")+1, column="Country", value=[data[key][df_unp].at[i, "Country/Region"] for i in range(rows)])
        row = []
        for j in range(rows):
            a = data[key][df_unp].at[j, "Values"]
            b = data[key][df_unp].at[j, "Population-2020"].replace(',', '')
            #print(a)
            #print(b)
            row.append(int(1000000*(int(a)/int(b))))
        
        data[key][df_unp].insert(loc=data[key][df_unp].columns.get_loc("Values")+1, column="Values per Mil", value=row)
        data[key][df_unp].rename({"Country/Region":"Country Region", "Lat":"Latitude", "Long":"Longitude"}, axis="columns", inplace=True)

    # Merge the data frames into single "Africa data Format from Mahlet for Tableau Dashboard"
    df_out = pd.concat([data[key][df_unp] for key in keys])
    # Finally sort them
    df_out.sort_values(by=["Country Region", "Date", "Group"], ascending=[True, False, True], inplace=True)
    print(df_out.head())
    print(df_out.shape)
    # write the combined data to file without index
    df_out.to_csv("data/time_series/africa_daily_time_series_unpivoted.csv", index=False)
    # write the individual unpivoted data to respective files
    for _key, _type in zip(keys, case_type):
        data[_key][df_unp].sort_values(by=["Country Region", "Date"], ascending=[True, False], inplace=True)
        data[_key][df_unp].to_csv("data/time_series/africa_daily_time_series_unpivoted_{}.csv".format(_type), index=False)

def preprocess(img_filename="", args=""):
    # load the example image and convert it to grayscale
    image = cv2.imread(img_filename)
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    # check to see if we should apply thresholding to preprocess the
    # image
    if args["preprocess"] == "thresh":
        gray = cv2.threshold(gray, 0, 255,
            cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]
    # make a check to see if median blurring should be done to remove
    # noise
    elif args["preprocess"] == "blur":
        # Median blur
        # gray = cv2.medianBlur(gray, 3)
        # Regular averaging
        # kernel = np.ones((3,3),np.float32)/9
        # gray = cv2.filter2D(gray,-1,kernel)
        # gray = cv2.blur(gray,(3,3))
        # Gaussian blur
        # gray = cv2.GaussianBlur(gray,(3,3),0)
        # Bilateral Filter
        gray = cv2.bilateralFilter(gray,9,75,75)
    # write the grayscale image to disk as a temporary file so we can
    # apply OCR to it later
    filename = "img/tmp_{}.png".format(os.getpid())
    # show the output images
    cv2.imshow("Original Image", image)
    cv2.imshow("Grayscale Image", gray)
    cv2.waitKey(0)
    cv2.imwrite(filename, gray)

    return filename

def process_single_image(image_file="", data_f="", files="", args=""):
    print("="*100)
    print("Processing File: {}".format(image_file))
    print("="*100)

    filename = preprocess(image_file, args)
    # Update the data from each image parsed
    data, date_txt = extract_africa_cdc_text(filename)
    print(data)
    print(date_txt)
    print_inter_diff(data_f, data)
    # Write the data to the time series data frames
    data_f = update_time_series(data_f, data, date_txt)

    # Finally, write the data frames for each update just in case
    write_time_series(data_f, files)
    
def extract_africa_cdc_text(file_name=""):
    # This expression matches the country cases, deaths, and recoveries as well as for the regions and totals at the beginning of the 
    # African CDC daily reporting images
    # cdr_exp = r"([*&\-'\w]+\s*[*&\-'\w]+\s*[*&\-'\w]+\s*[*&\-'\w]+)\s?(\([^)]+\))" # exp
    # cdr_exp = r"(([*&\-'\w]+\s?){1,4})\s?(\([^)]+\))" # exp
    # cdr_exp = r"((\s[*&\-'\w\,]+){1,4})\s*(\(?[\d\*\;\s\,\.]+\))" # exp
    # cdr_exp = r"((\s[*&\-'\w\,]+){1,4})\s*(\(?[\d\*\;\s\,\.\%\$]+\))" # exp
    cdr_exp = r"((\s[*&\-'\w\,]+){1,4})\s*([\(\{\]]?[\d\*\;\s\,\.\%\$\)\(\/]+[\)\}\]])" # exp
    
    re_ = re.compile(cdr_exp)

    #print( image_to_string(Image.open('April_15_6pm.jpg')))
    # 'img/April_15_6pm.jpg'
    txt = pytesseract.image_to_string(Image.open(file_name), lang='eng')
    # Remove the temporary file created by preprocessing
    os.remove(file_name)
    # Remove the newline characters
    txt = txt.replace("\n", ' ')
    print(txt)
    # get the date text
    date_txt = parse_date(txt)
    # get all matching data
    txt_ = re_.findall(txt)
    print(txt_)
    # remove trailing spaces and construct the data dict
    data = {x[0].rstrip().lstrip().replace("*",'').replace(",",''):parse_num(x[2]) for x in txt_}
    # Replace frequently missed country names because of data entry and OCR issues
    for key, val in freq_missed.items():
        if key in data.keys():
            data[val] = data.pop(key)

    keys = list(data.keys())
    # print(keys)
    keys.sort()
    print(keys)
    return data, date_txt

def parse_date(txt):
    exp_d = r'\d\d?\s[\w]+\s\d{4}'
    re_d = re.compile(exp_d)
    txt_ = re_d.findall(txt)
    # Grab the first matched date
    print(txt_)
    date_obj = datetime.strptime(txt_[0], '%d %B %Y').date()
    date_txt = date_obj.strftime('%#m/%#d/%Y')
    # date_txt = '5/2/2020'
    print(date_txt)

    return date_txt

def parse_num(x):
    exp_n = r'[\d\,\.\*\%\$]+' #r'[\d\,\.\*\%\$\)\(\/]+'
    re_n = re.compile(exp_n)
    print(x)
    txt_ = [int(re.sub('[\,\.\*\%\$\)\(\/]*', '', a)) for a in re_n.findall(x)]
    # print(txt_)
    return txt_

def update_africa_cdc_data(data="", files="", imgs_path="img/April_15_6pm.jpg"):
    # Open the files once
    data_f, files = read_time_series()
    # Update the data from each image parsed
    data, date_txt = extract_africa_cdc_text(imgs_path)
    print(data)
    print(date_txt)
    print_inter_diff(data_f, data)
    # Write the data to the time series data frames
    data_f = update_time_series(data_f, data, date_txt)

    # Finally, write the data frames once to the files
    write_time_series(data_f, files)
