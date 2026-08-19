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


def _era_subfolder(year):
    if year == 2009:
        return "legacy_2009"
    if year == 2010:
        return "legacy_2010"
    return "modern"


def _land_yellow_tripdata(volume_path):
    downloaded, moved, skipped = 0, 0, 0
    for year, month in _year_months(START_YEAR_MONTH, END_YEAR_MONTH):
        era_dir = os.path.join(volume_path, _era_subfolder(year))
        os.makedirs(era_dir, exist_ok=True)

        file_name = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
        destination = os.path.join(era_dir, file_name)
        if os.path.exists(destination):
            skipped += 1
            continue

        # Files landed before the era-subfolder layout sit flat in volume_path;
        # move them into place instead of re-downloading from CloudFront.
        legacy_flat_path = os.path.join(volume_path, file_name)
        if os.path.exists(legacy_flat_path):
            os.rename(legacy_flat_path, destination)
            moved += 1
            continue

        tmp_destination = destination + ".tmp"
        with urllib.request.urlopen(f"{BASE_URL}/{file_name}") as response, open(tmp_destination, "wb") as f:
            shutil.copyfileobj(response, f)
        os.rename(tmp_destination, destination)
        downloaded += 1

    print(
        f"Landed {downloaded} new file(s), moved {moved} from the old flat layout, "
        f"skipped {skipped} already present ({downloaded + moved + skipped} total)."
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    volume_path = f"/Volumes/{args.catalog}/{args.schema}/nyc_data_volume"
    _land_yellow_tripdata(volume_path)


if __name__ == "__main__":
    main()
