from tqdm import tqdm
import requests
import pandas as pd
from zipfile import ZipFile
import os


def download(url, filename):

    r = requests.get(url, stream=True)
    total_size = int(r.headers["Content-Length"])
    downloaded = 0  # keep track of size downloaded so far
    chunkSize = 1024
    bars = int(total_size / chunkSize)

    with open(filename, "wb") as f:
        for chunk in tqdm(r.iter_content(chunk_size=chunkSize), total=bars, unit="KB",
                                      desc=filename, leave=True):
            f.write(chunk)
            downloaded += chunkSize  # increment the downloaded
            prog = ((downloaded * 100 / total_size))

base_url='https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{}_{}.zip'
airport="EWR"
output_dir="output"

def download_and_process(year, month):

    filename="{}-{}/file.zip".format(year, month)
    url=base_url.format(year, month)
    directory="{}-{}".format(year, month)

    os.mkdir(directory)
    download(url, filename)

    with ZipFile(filename, 'r') as zipObj:
        zipObj.extractall(directory)

    csv_name="{}-{}/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_{}_{}.csv".format(
        year, month, year, month)

    # cols = ["FL_DATE", "OP_UNIQUE_CARRIER", "TAIL_NUM", "OP_CARRIER_FL_NUM",
    #         "ORIGIN", "DEST", "CRS_DEP_TIME", "DEP_TIME", "DEP_DELAY",
    #         "TAXI_OUT", "TAXI_IN", "CRS_ARR_TIME", "ARR_TIME", "ARR_DELAY",
    #         "CANCELLED", "DIVERTED", "CRS_ELAPSED_TIME", "ACTUAL_ELAPSED_TIME",
    #         "AIR_TIME"]

    cols=["FlightDate", "Reporting_Airline", "Tail_Number", "Flight_Number_Reporting_Airline", "Origin",
            "Dest", "CRSDepTime", "DepTime", "DepDelay", "TaxiOut", "TaxiIn", "CRSArrTime", "ArrTime",
            "ArrDelay", "Cancelled", "Diverted", "CRSElapsedTime", "ActualElapsedTime", "AirTime"]

    df= pd.read_csv(csv_name, usecols=cols)
    df= df[(df["Origin"] == airport) | (df["Dest"] == airport)]
    df.head()
    df.to_csv("{}/{}-{}-{}.csv".format(output_dir, airport, year, month))
    del df

    os.remove(csv_name)

for year in range(2014, 2019):
    for month in range(1, 13):
        download_and_process(year, month)
        print("#### DONE {}{}".format(year, month))
