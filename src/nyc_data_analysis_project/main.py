# Landing step for the yellow taxi pipeline. Runs as the job's python_wheel_task,
# before the Lakeflow pipeline refresh: lands every published NYC TLC Yellow Taxi
# monthly parquet file into the Unity Catalog volume the bronze layer reads from.
# Idempotent — re-runs only fetch months not already present.
import argparse
import os
import shutil
import urllib.request

# NYC TLC's public dataset. START/END is the month range currently published there;
# bump END_YEAR_MONTH as new months come out.
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
START_YEAR_MONTH = (2009, 1)
END_YEAR_MONTH = (2026, 5)


def _year_months(start, end):
    """Yield every (year, month) pair from start to end, inclusive.

    Args:
        start: (year, month) tuple to begin at.
        end: (year, month) tuple to stop at (inclusive).
    """
    year, month = start
    while (year, month) <= end:
        yield year, month
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)


# Routes each month into the same 3 subfolders bronze's Auto Loader streams read
# from (see ERA_2009_COLUMNS/ERA_2010_COLUMNS/MODERN_COLUMNS in bronze) — must stay
# in sync with that file, since each subfolder needs one stable schema per stream.
def _era_subfolder(year):
    """Return the landing subfolder for a given year's schema era.

    Args:
        year: The 4-digit year a file's data belongs to.
    """
    if year == 2009:
        return "legacy_2009"
    if year == 2010:
        return "legacy_2010"
    return "modern"


def _land_yellow_tripdata(volume_path):
    """Download every published Yellow Taxi month not already present in the volume.

    Idempotent: months already landed (or present in the old flat layout,
    which get moved into place instead) are skipped rather than re-downloaded.

    Args:
        volume_path: Root path of the Unity Catalog volume to land files into
            (era subfolders are created underneath it as needed).
    """
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


# Entry point wired up via pyproject.toml's [project.scripts] and invoked by the
# job's python_wheel_task (see resources/nyc_data_analysis_project_job.job.yml).
def main():
    """Entry point for the job's landing step: parses --catalog/--schema and lands files."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    volume_path = f"/Volumes/{args.catalog}/{args.schema}/nyc_data_volume"
    _land_yellow_tripdata(volume_path)


if __name__ == "__main__":
    main()
