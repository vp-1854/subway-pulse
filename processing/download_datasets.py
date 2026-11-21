from __future__ import annotations

import argparse
import csv
import http.client
import json
import os
import time
import urllib.parse
import urllib.error
import urllib.request
import zipfile
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

from processing.config import (
    DATA_WINDOW_END,
    DATA_WINDOW_START,
    MTA_HOURLY_RIDERSHIP_2020_2024_CSV_URL,
    MTA_HOURLY_RIDERSHIP_2020_2024_RESOURCE_ID,
    MTA_HOURLY_RIDERSHIP_2025_CSV_URL,
    MTA_HOURLY_RIDERSHIP_2025_RESOURCE_ID,
    MTA_STATIC_ZIP_URL,
    MTA_STATIONS_CSV_URL,
    NOAA_CDO_DATA_URL,
    RAW_RIDERSHIP_2020_2024,
    RAW_RIDERSHIP_2025,
    RAW_STATIONS,
    RAW_STOPS,
    RAW_WEATHER,
    ensure_dir,
    ensure_parent,
)


DEFAULT_NOAA_STATIONS = ["GHCND:USW00094728", "GHCND:USW00094789"]
NOAA_DATATYPES = ["PRCP", "SNOW", "TMAX", "TMIN"]
RIDERSHIP_SELECT_COLUMNS = [
    "transit_timestamp",
    "transit_mode",
    "station_complex_id",
    "station_complex",
    "borough",
    "payment_method",
    "ridership",
    "transfers",
    "latitude",
    "longitude",
]


def download_url(url: str, destination: Path, *, skip_existing: bool = True) -> None:
    if skip_existing and destination.exists() and destination.stat().st_size > 0:
        print(f"exists: {destination}")
        return
    ensure_parent(destination)
    print(f"downloading: {url} -> {destination}")
    part_path = destination.with_suffix(destination.suffix + ".part")
    bytes_written = 0
    try:
        with urllib.request.urlopen(url, timeout=120) as response, part_path.open("wb") as handle:
            while True:
                chunk = response.read(4 * 1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)
                bytes_written += len(chunk)
                print(f"  {bytes_written / (1024 * 1024):,.1f} MiB", end="\r", flush=True)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        if part_path.exists():
            part_path.unlink()
        raise RuntimeError(f"HTTP {exc.code} while downloading {url}\n{body[:2000]}") from exc
    part_path.replace(destination)
    print()
    print(f"wrote {destination.stat().st_size:,} bytes: {destination}")


def with_socrata_limit(url: str, limit: int | None) -> str:
    if not limit:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}$limit={int(limit)}"


def ridership_resource_url(
    resource_id: str, *, since: str | None = None, until: str | None = None, limit: int | None = None
) -> str:
    return ridership_resource_page_url(resource_id, since=since, until=until, limit=limit, offset=None)


def ridership_resource_page_url(
    resource_id: str,
    *,
    since: str | None = None,
    until: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> str:
    params: dict[str, object] = {
        "$select": ",".join(RIDERSHIP_SELECT_COLUMNS),
        # :id breaks ties between rows with the same timestamp so offset paging never skips or repeats rows.
        "$order": "transit_timestamp,:id",
    }
    conditions = []
    if since:
        conditions.append(f"transit_timestamp >= '{since}T00:00:00'")
    if until:
        # until is an inclusive date, so stop before the start of the next day.
        next_day = datetime.strptime(until, "%Y-%m-%d").date() + timedelta(days=1)
        conditions.append(f"transit_timestamp < '{next_day.isoformat()}T00:00:00'")
    if conditions:
        params["$where"] = " AND ".join(conditions)
    if limit:
        params["$limit"] = int(limit)
    if offset:
        params["$offset"] = int(offset)
    return f"https://data.ny.gov/resource/{resource_id}.csv?{urllib.parse.urlencode(params)}"


def _fetch_page(url: str, *, attempts: int = 6, timeout: int = 180) -> bytes:
    """Fetch one Socrata page, retrying timeouts, dropped connections and 429/5xx responses."""
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            if exc.code not in (429, 500, 502, 503, 504) or attempt == attempts:
                raise RuntimeError(f"HTTP {exc.code} while downloading {url}\n{body[:2000]}") from exc
            problem = f"HTTP {exc.code}"
        except (TimeoutError, urllib.error.URLError, ConnectionError, http.client.HTTPException) as exc:
            if attempt == attempts:
                raise RuntimeError(f"Giving up after {attempts} attempts on {url}: {exc!r}") from exc
            problem = repr(exc)
        wait = min(60, 5 * 2 ** (attempt - 1))
        print(f"  attempt {attempt}/{attempts} failed ({problem}); retrying in {wait}s")
        time.sleep(wait)
    raise RuntimeError("unreachable")


def _download_range_to_file(
    resource_id: str,
    out_path: Path,
    *,
    since: str,
    until: str | None,
    row_limit: int | None,
    page_size: int,
) -> int:
    """Download one date range (paged) into out_path, header included. Returns the data row count."""
    total_rows = 0
    offset = 0
    wrote_header = False
    with out_path.open("wb") as output:
        while True:
            remaining = None if row_limit is None else max(row_limit - total_rows, 0)
            if remaining == 0:
                break
            limit = min(page_size, remaining) if remaining is not None else page_size
            url = ridership_resource_page_url(resource_id, since=since, until=until, limit=limit, offset=offset)
            payload = _fetch_page(url)
            lines = payload.splitlines(keepends=True)
            if not lines:
                break
            output.writelines(lines[1:] if wrote_header else lines)
            wrote_header = True
            rows_this_page = max(len(lines) - 1, 0)
            total_rows += rows_this_page
            if rows_this_page < limit:
                break
            offset += rows_this_page
    return total_rows


def download_ridership_resource_pages(
    resource_id: str,
    destination: Path,
    *,
    since: str,
    until: str | None = None,
    row_limit: int | None = None,
    page_size: int = 50000,
    skip_existing: bool = True,
) -> None:
    """Download ridership one day at a time into <destination stem>_parts/, then join the days into destination.

    Day sized requests keep every offset small (deep offsets time out on Socrata), and finished days are kept,
    so an interrupted run resumes where it stopped.
    """
    if skip_existing and destination.exists() and destination.stat().st_size > 0:
        print(f"exists: {destination}")
        return

    ensure_parent(destination)
    part_path = destination.with_suffix(destination.suffix + ".part")

    if row_limit is not None:
        # Smoke test mode: one range, no day splitting.
        rows = _download_range_to_file(
            resource_id, part_path, since=since, until=until, row_limit=row_limit, page_size=page_size
        )
        part_path.replace(destination)
        print(f"wrote {rows:,} rows and {destination.stat().st_size:,} bytes: {destination}")
        return

    first_day = datetime.strptime(since, "%Y-%m-%d").date()
    last_day = datetime.strptime(until, "%Y-%m-%d").date() if until else date.today()
    parts_dir = ensure_dir(destination.parent / f"{destination.stem}_parts")
    days = [first_day + timedelta(days=i) for i in range((last_day - first_day).days + 1)]

    total_rows = 0
    for index, day in enumerate(days, 1):
        day_file = parts_dir / f"{day.isoformat()}.csv"
        if day_file.exists() and day_file.stat().st_size > 0:
            continue
        tmp_file = day_file.with_suffix(".csv.tmp")
        rows = _download_range_to_file(
            resource_id, tmp_file, since=day.isoformat(), until=day.isoformat(), row_limit=None, page_size=page_size
        )
        tmp_file.replace(day_file)
        total_rows += rows
        print(f"day {index}/{len(days)} {day.isoformat()}: {rows:,} rows (new rows this run: {total_rows:,})")

    if part_path.exists():
        part_path.unlink()
    wrote_header = False
    combined_rows = 0
    with part_path.open("wb") as output:
        for day in days:
            day_file = parts_dir / f"{day.isoformat()}.csv"
            if not day_file.exists():
                continue
            with day_file.open("rb") as handle:
                lines = handle.readlines()
            if not lines:
                continue
            output.writelines(lines[1:] if wrote_header else lines)
            wrote_header = True
            combined_rows += max(len(lines) - 1, 0)
    part_path.replace(destination)
    print(f"wrote {combined_rows:,} rows and {destination.stat().st_size:,} bytes: {destination}")
    print(f"you can delete the per day files in {parts_dir} once the pipeline has run")


def download_gtfs_static(destination: Path, *, skip_existing: bool = True) -> None:
    if skip_existing and destination.exists() and destination.stat().st_size > 0:
        print(f"exists: {destination}")
        return
    zip_path = destination.with_suffix(".zip")
    download_url(MTA_STATIC_ZIP_URL, zip_path, skip_existing=False)
    with zipfile.ZipFile(zip_path) as archive:
        with archive.open("stops.txt") as source:
            ensure_parent(destination)
            destination.write_bytes(source.read())
    print(f"extracted stops.txt -> {destination}")


def _noaa_get(token: str, params: dict[str, object]) -> dict:
    query = urllib.parse.urlencode(params, doseq=True)
    request = urllib.request.Request(
        f"{NOAA_CDO_DATA_URL}?{query}",
        headers={"token": token, "User-Agent": "subway-pulse/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"NOAA CDO HTTP {exc.code}: {body[:2000]}") from exc


def split_date_ranges(start_date: str, end_date: str, *, max_days: int = 365) -> list[tuple[str, str]]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    if end < start:
        raise ValueError("end_date must be on or after start_date")

    ranges = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=max_days - 1), end)
        ranges.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end + timedelta(days=1)
    return ranges


def download_noaa_weather(
    destination: Path,
    *,
    token: str,
    start_date: str,
    end_date: str,
    stations: list[str],
    skip_existing: bool = True,
) -> None:
    if skip_existing and destination.exists() and destination.stat().st_size > 0:
        print(f"exists: {destination}")
        return

    # CDO paginates responses. Store per date, then aggregate Central Park/JFK
    # into one NYC-wide daily row because the runtime model uses a city bucket.
    by_date: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for station in stations:
        for chunk_start, chunk_end in split_date_ranges(start_date, end_date):
            print(f"downloading NOAA station={station} start={chunk_start} end={chunk_end}")
            offset = 1
            while True:
                payload = _noaa_get(
                    token,
                    {
                        "datasetid": "GHCND",
                        "stationid": station,
                        "datatypeid": NOAA_DATATYPES,
                        "startdate": chunk_start,
                        "enddate": chunk_end,
                        "units": "standard",
                        "limit": 1000,
                        "offset": offset,
                    },
                )
                results = payload.get("results", [])
                for row in results:
                    day = row["date"][:10]
                    by_date[day][row["datatype"]].append(float(row["value"]))
                if len(results) < 1000:
                    break
                offset += 1000
                time.sleep(0.25)

    ensure_parent(destination)
    with destination.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["DATE", "PRCP", "SNOW", "TMAX", "TMIN"])
        writer.writeheader()
        for day in sorted(by_date):
            values = by_date[day]
            writer.writerow(
                {
                    "DATE": day,
                    "PRCP": max(values.get("PRCP", [0.0])),
                    "SNOW": max(values.get("SNOW", [0.0])),
                    "TMAX": _mean_or_blank(values.get("TMAX", [])),
                    "TMIN": _mean_or_blank(values.get("TMIN", [])),
                }
            )
    print(f"wrote NOAA daily weather rows: {destination}")


def _mean_or_blank(values: list[float]) -> float | str:
    return sum(values) / len(values) if values else ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Subway Pulse source datasets into data/raw.")
    parser.add_argument("--raw-dir", default=str(RAW_STOPS.parent))
    parser.add_argument("--start-date", default=DATA_WINDOW_START, help="NOAA weather start date (default: data window start).")
    parser.add_argument("--end-date", default=DATA_WINDOW_END, help="NOAA weather end date (default: data window end).")
    parser.add_argument("--noaa-station", action="append", dest="noaa_stations", default=[])
    parser.add_argument(
        "--ridership-limit",
        type=int,
        default=None,
        help="Optional Socrata row limit for quick smoke-test downloads. Omit for the full CSV.",
    )
    parser.add_argument(
        "--ridership-years",
        choices=["all", "2020-2024", "2025"],
        default="2025",
        help="Which MTA hourly ridership dataset to download. The default one year window only needs the Beginning 2025 dataset.",
    )
    parser.add_argument(
        "--ridership-since",
        default=DATA_WINDOW_START,
        help="YYYY-MM-DD start of the ridership window (inclusive). Uses the Socrata resource endpoint with projected columns.",
    )
    parser.add_argument(
        "--ridership-until",
        default=DATA_WINDOW_END,
        help="YYYY-MM-DD end of the ridership window (inclusive).",
    )
    parser.add_argument("--socrata-page-size", type=int, default=50000)
    parser.add_argument("--skip-ridership", action="store_true", help="Download only static GTFS/stations/weather inputs.")
    parser.add_argument("--skip-existing", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--skip-noaa", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_dir = ensure_dir(args.raw_dir)
    download_gtfs_static(raw_dir / RAW_STOPS.name, skip_existing=args.skip_existing)
    download_url(MTA_STATIONS_CSV_URL, raw_dir / RAW_STATIONS.name, skip_existing=args.skip_existing)
    if args.skip_ridership:
        print("skipping ridership download by request")
    else:
        if args.ridership_years in {"all", "2020-2024"}:
            if args.ridership_since:
                download_ridership_resource_pages(
                    MTA_HOURLY_RIDERSHIP_2020_2024_RESOURCE_ID,
                    since=args.ridership_since,
                    until=args.ridership_until,
                    row_limit=args.ridership_limit,
                    page_size=args.socrata_page_size,
                    destination=raw_dir / RAW_RIDERSHIP_2020_2024.name,
                    skip_existing=args.skip_existing,
                )
            else:
                download_url(
                    with_socrata_limit(MTA_HOURLY_RIDERSHIP_2020_2024_CSV_URL, args.ridership_limit),
                    raw_dir / RAW_RIDERSHIP_2020_2024.name,
                    skip_existing=args.skip_existing,
                )
        if args.ridership_years in {"all", "2025"}:
            if args.ridership_since:
                download_ridership_resource_pages(
                    MTA_HOURLY_RIDERSHIP_2025_RESOURCE_ID,
                    since=args.ridership_since,
                    until=args.ridership_until,
                    row_limit=args.ridership_limit,
                    page_size=args.socrata_page_size,
                    destination=raw_dir / RAW_RIDERSHIP_2025.name,
                    skip_existing=args.skip_existing,
                )
            else:
                download_url(
                    with_socrata_limit(MTA_HOURLY_RIDERSHIP_2025_CSV_URL, args.ridership_limit),
                    raw_dir / RAW_RIDERSHIP_2025.name,
                    skip_existing=args.skip_existing,
                )

    if args.skip_noaa:
        print("skipping NOAA download by request")
        return

    token = os.environ.get("NOAA_TOKEN")
    if not token:
        print("NOAA_TOKEN is not set; leave data/raw/noaa_weather.csv in place or rerun with a token.")
        return

    download_noaa_weather(
        raw_dir / RAW_WEATHER.name,
        token=token,
        start_date=args.start_date,
        end_date=args.end_date,
        stations=args.noaa_stations or DEFAULT_NOAA_STATIONS,
        skip_existing=args.skip_existing,
    )


if __name__ == "__main__":
    main()
