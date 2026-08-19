import argparse
import os
import shutil
import urllib.request

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
START_YEAR_MONTH = (2009, 1)
END_YEAR_MONTH = (2026, 5)


def _year_months(start, end):
    year, month = start
    while (year, month) <= end:
        yield year, month
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)


def _land_yellow_tripdata(volume_path):
    os.makedirs(volume_path, exist_ok=True)

    for year, month in _year_months(START_YEAR_MONTH, END_YEAR_MONTH):
        file_name = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
        destination = os.path.join(volume_path, file_name)
        if os.path.exists(destination):
            continue

        tmp_destination = destination + ".tmp"
        with urllib.request.urlopen(f"{BASE_URL}/{file_name}") as response, open(tmp_destination, "wb") as f:
            shutil.copyfileobj(response, f)
        os.rename(tmp_destination, destination)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    volume_path = f"/Volumes/{args.catalog}/{args.schema}/nyc_data_volume"
    _land_yellow_tripdata(volume_path)


if __name__ == "__main__":
    main()
