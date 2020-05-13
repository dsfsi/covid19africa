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

def main():
    args = parse_args()
    unpivot_timeseries()

if __name__ == "__main__":
    main()