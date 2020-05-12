# This script will be used to update cases, deaths, recovered from 
# https://twitter.com/AfricaCDC daily updates in the form of images

import sys
import os

import pandas as pd
import re
import argparse

from utils import *

def parse_args():
    cmd_parser = argparse.ArgumentParser()
    cmd_parser.add_argument("-i", "--input_path", default="img/",
                            help="path to the image file to parse data from")
    cmd_parser.add_argument("-m", "--mode", default="batch",
                            help="Whether to batch process many days images or single day image")
    cmd_parser.add_argument("-p", "--preprocess", type=str, default="blur",
	help="type of preprocessing to clean the image for OCR. If -p is set to None, OCR will be applied to the grayscale image.")
    args = vars(cmd_parser.parse_args())

    return args
    
def update_africa_cdc_data_batch(data="", files="", imgs_path="img/", args=""):
    # Open the files once
    data_f, files = read_time_series()
    # Create the filenames in the img/ folder
    images = [imgs_path]
    if imgs_path == "img/":
        images = [imgs_path + f for f in listdir(imgs_path) if isfile(join(imgs_path, f))]

    for image_file in images:
        process_single_image(image_file, data_f, files, args)

def read_config(yaml_path="data/config/africa_cdc_imgs.yml"):
    with open(yaml_path) as config_file:
        imgs_list = yaml.load(config_file, Loader=yaml.FullLoader)
        print(imgs_list)

def main():
    args = parse_args()
    images_path = args["input_path"]
    mode = args["mode"]
    print(images_path)
    print(mode)

    update_africa_cdc_data_batch(imgs_path=images_path, args=args)

if __name__ == "__main__":
    main()
    # read_config()
    # unpivot_timeseries()