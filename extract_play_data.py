#!/usr/bin/env python

import requests
import os

'''
    Description:
        This script creates CSV files with play-by-play data for the NFL for the
        years specified in the constant variable portion of the code. At the moment,
        the data is extracted to a local /extract directory.
'''

PLAY_DATA_URL = 'http://www.nflsavant.com/pbp_data.php?year='
DESTINATION = './extract/'
FILE_NAME = 'play-by-play_data_'
EXTENSION = '.csv'
YEARS = ['2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']


# Checks if a file for the specified year already exists in the destination folder
def does_file_exist(year_: str) -> bool:
    full_file_name = FILE_NAME + year_ + EXTENSION

    if (os.path.isfile(DESTINATION + full_file_name)):
        print(f"{full_file_name} already exists.")
        return True
    else:
        print(f"{full_file_name} does not exist.")
        return False


# Download file to the ./extract folder
def download_csv(year_: str) -> None:
    try:
        print("Downloading file... ", end="", flush=True)
        response = requests.get(PLAY_DATA_URL + year_)
    except:
        print("ERROR: There is an issue with the URL.")
        return

    full_file_name = FILE_NAME + year_ + EXTENSION

    with open(os.path.join(DESTINATION, full_file_name), 'wb') as file:
        file.write(response.content)

    file.close()

    print(f"{full_file_name} successfully downloaded.")


if __name__ == "__main__":
    for year in YEARS:
        if does_file_exist(year):
            continue
        else:
            download_csv(year)
