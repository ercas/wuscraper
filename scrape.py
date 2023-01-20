#!/usr/bin/env python3

import argparse
import csv
import enum
import gzip
import json
import logging
import multiprocessing
import os
import typing

import pandas
import requests
import tqdm
import tqdm.contrib.logging

import wuscraper

DEFAULT_API_KEY_PATH = "api_key.txt"


class Targets(enum.Enum):
    DAILY = enum.auto()
    HISTORICAL = enum.auto()
    FEATURES = enum.auto()
    EXPORT_DAILY = enum.auto()
    EXPORT_HISTORICAL = enum.auto()


def tqdm_if_verbose(iterable: typing.Iterable,
                    verbose: bool = True,
                    *tqdm_args, **tqdm_kwargs) -> typing.Iterable:
    if verbose:
        return tqdm.tqdm(iterable, *tqdm_args, **tqdm_kwargs)
    return iter(iterable)


def get_api_key(api_key_path: str = DEFAULT_API_KEY_PATH) -> str:
    if os.path.isfile(api_key_path):
        with open(api_key_path, "r") as input_fp:
            return input_fp.read().strip()
    with open(api_key_path, "w") as output_fp:
        output_fp.write("Paste API key here\n")
    print("Please paste your API key into {}".format(os.path.realpath(DEFAULT_API_KEY_PATH)))
    raise RuntimeError("Could not find API key")


def stream_file_paths(root_directory: str) -> typing.Iterable[str]:
    for root, directories, files in os.walk(root_directory):
        for filename in files:
            yield os.path.join(root, filename)


def build_parser():
    parser = argparse.ArgumentParser(
        description="Scraper and exporter for Weather Underground / weather.com"
                    " data. The `daily` and `historical` scrapers have separate"
                    " export functions because the normal scraping routine will"
                    " attempt to retrieve data that may return a 404 error"
                    " code, delaying the export process. In contrast, the"
                    " `features` endpoint will always return data, so a"
                    " complete scrape of features will export fairly quickly."
    )

    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "-a", "--api-key", type=str,
        help="The API key to use for scraping"
    )
    parent_parser.add_argument(
        "-d", "--scrape-directory", type=str, default=wuscraper.DEFAULT_OUTPUT_DIR,
        help="The directory that scraped data will be saved to"
    )
    parent_parser.add_argument(
        "-o", "--output-file", type=str,
        help="The file to export scraped results to, if any"
    )
    parent_parser.add_argument(
        "-p", "--progress", action="store_true", default=False,
        help="Show a progress bar"
    )
    parent_parser.add_argument(
        "-v", "--verbose", action="store_true", default=False,
        help="Change log level to logging.DEBUG"
    )

    subparsers = parser.add_subparsers(title="Target")

    daily_parser = subparsers.add_parser(
        "daily", parents=[parent_parser],
        help="Scrape daily observations by month for personal weather stations"
    )
    daily_parser.set_defaults(target=Targets.DAILY)
    daily_parser.add_argument(
        "stations", nargs="+",
        help="A list of NWS observed weather stations to scrape, separated by spaces"
    )
    daily_parser.add_argument(
        "-s", "--start-date", type=str, default="1980-01-01",
        help="The first month to scrape data for (YYYY-MM-DD)"
    )
    daily_parser.add_argument(
        "-e", "--end-date", type=str, default="2022-12-01",
        help="The first month to scrape data for (YYYY-MM-DD)"
    )

    historical_parser = subparsers.add_parser(
        "historical", parents=[parent_parser],
        help="Scrape hourly observations by day for NWS-operated weather stations"
    )
    historical_parser.set_defaults(target=Targets.HISTORICAL)
    historical_parser.add_argument(
        "stations", nargs="+",
        help="A list of personal weather stations to scrape, separated by spaces"
    )
    historical_parser.add_argument(
        "-s", "--start-date", type=str, default="1980-01-01",
        help="The first day to scrape data for (YYYY-MM-DD)"
    )
    historical_parser.add_argument(
        "-e", "--end-date", type=str, default="2022-12-01",
        help="The first day to scrape data for (YYYY-MM-DD)"
    )

    features_parser = subparsers.add_parser(
        "features", parents=[parent_parser],
        help="Scrape locations and other attributes for personal weather stations"
    )
    features_parser.set_defaults(target=Targets.FEATURES)
    features_parser.add_argument(
        "zoom_levels", nargs="*", type=int, default=list(range(1, 11 + 1))
    )

    export_daily_parser = subparsers.add_parser(
        "export-daily", parents=[parent_parser],
        help="Export all scraped observations from personal weather stations"
             " (bypasses the normal scraping routine)"
    )
    export_daily_parser.add_argument(
        "-j", "--jobs", type=int, default=1,
        help="The number of parallel workers to use to read and process the"
             " raw JSON data."
    )
    export_daily_parser.set_defaults(target=Targets.EXPORT_DAILY)

    export_historical_parser = subparsers.add_parser(
        "export-historical", parents=[parent_parser],
        help="Export all scraped observations from NWS-operated weather stations"
             " (bypasses the normal scraping routine)"
    )
    export_historical_parser.add_argument(
        "-j", "--jobs", type=int, default=1,
        help="The number of parallel workers to use to read and process the"
             " raw JSON data."
    )
    export_historical_parser.set_defaults(target=Targets.EXPORT_HISTORICAL)

    return parser

def observations_json_gz_to_df(path: str) -> typing.Optional[pandas.DataFrame]:
    if not path.endswith(".json.gz"):
        return
    try:
        with gzip.open(path, "rt") as input_fp:
            observations = json.load(input_fp)["observations"]
            if len(observations) == 0:
                return
            return pandas.json_normalize(observations)
    except Exception as error:
        logging.info("Caught exception {}: {}".format(error, path))

def stream_observations(paths: typing.Iterable[str],
                        output_path: str,
                        jobs: int = 1):
    first = True
    df_stream = (observations_json_gz_to_df(path) for path in paths)
    pool = None
    if jobs > 1:
        logging.info("Using {} parallel workers to process JSON data".format(jobs))
        pool = multiprocessing.Pool(jobs)
        df_stream = pool.imap(observations_json_gz_to_df, paths)
    for df in df_stream:
        if df is not None:
            df.to_csv(
                output_path,
                mode="w" if first else "a",
                header=first,
                index=False
            )
        first = False
    if pool:
        pool.close()


def main():
    parser = build_parser()

    args = parser.parse_args()

    if not hasattr(args, "target"):
        parser.print_help()
        return

    output_file = None
    if hasattr(args, "output_file"):
        output_file = args.output_file

    if args.verbose:
        logging.getLogger().setLevel(logging.INFO)
        logging.info(args)

    if args.target == Targets.EXPORT_DAILY:
        scrape_subdirectory = os.path.join(
            args.scrape_directory,
            wuscraper.WUScraper.Paths.DAILY.value.split("/")[1]
        )
        logging.info("Counting files in {}".format(scrape_subdirectory))
        total_files = 0
        if args.progress:
            total_files = sum(
                1
                for _ in tqdm.tqdm(
                    stream_file_paths(scrape_subdirectory),
                    desc="Counting files",
                    unit=" paths"
                )
            )
        stream_observations(
            paths=tqdm_if_verbose(
                stream_file_paths(scrape_subdirectory),
                verbose=args.progress,
                total=total_files,
                desc="Reading and converting observations"
            ),
            output_path=args.output_file,
            jobs=args.jobs
        )
        return

    elif args.target == Targets.EXPORT_HISTORICAL:
        scrape_subdirectory = os.path.join(
            args.scrape_directory,
            wuscraper.WUScraper.Paths.HISTORICAL.value.split("/")[1]
        )
        logging.info("Counting files in {}".format(scrape_subdirectory))
        total_files = 0
        if args.progress:
            total_files = sum(
                1
                for _ in tqdm.tqdm(
                    stream_file_paths(scrape_subdirectory),
                    desc="Counting files",
                    unit=" paths"
                )
            )
        stream_observations(
            paths=tqdm_if_verbose(
                stream_file_paths(scrape_subdirectory),
                verbose=args.progress,
                total=total_files,
                desc="Reading and converting observations"
            ),
            output_path=args.output_file,
            jobs=args.jobs
        )
        return

    # Scrape
    with (
        wuscraper.WUScraper(api_key=getattr(args, "api_key", None) or get_api_key(),
                            output_directory=args.scrape_directory) as scraper,
        tqdm.contrib.logging.logging_redirect_tqdm()
    ):

        # Scrape data from personal weather stations
        if args.target == Targets.DAILY:
            date_range = pandas.date_range(args.start_date, args.end_date, freq="MS").to_pydatetime()
            stations = sorted(set(args.stations))
            for station in tqdm_if_verbose(
                    stations,
                    verbose=args.progress and len(stations) > 1,
                    position=0,
                    desc="Stations"
            ):
                complete_marker = "output/daily/{}/complete".format(station)
                if os.path.isfile(complete_marker) and not output_file:
                    continue
                for dt in tqdm_if_verbose(
                        list(reversed(date_range)),
                        verbose=args.progress,
                        position=1,
                        miniters=1,
                        desc=station
                ):
                    try:
                        result = wuscraper.retry_x_times(
                            func=lambda: scraper.daily(
                                station=station,
                                month=dt,
                                as_df=output_file is not None
                            ),
                            x=5,
                            allowed_exceptions=(requests.exceptions.ConnectionError,)
                        )
                        if output_file:
                            do_append = os.path.isfile(output_file)
                            result.to_csv(
                                output_file,
                                mode="a" if do_append else "w",
                                header=not do_append,
                                index=False
                            )
                    # except (RuntimeError, requests.exceptions.HTTPError):
                    except RuntimeError:
                        pass
                with open(complete_marker, "w") as _:
                    pass

        # Scrape data from NWS-operated weather stations
        elif args.target == Targets.HISTORICAL:
            date_range = pandas.date_range(args.start_date, args.end_date).to_pydatetime()
            stations = sorted(set(args.stations))
            for station in tqdm_if_verbose(
                    stations,
                    verbose=args.progress and len(stations) > 1,
                    position=0,
                    desc="Stations"
            ):
                complete_marker = "output/daily/{}/complete".format(station)
                if os.path.isfile(complete_marker) and not output_file:
                    continue
                for dt in tqdm_if_verbose(
                        list(reversed(date_range)),
                        verbose=args.progress,
                        position=1,
                        miniters=1,
                        desc=station
                ):
                    try:
                        result = wuscraper.retry_x_times(
                            func=lambda: scraper.historical(
                                station=station,
                                start_date=dt,
                                as_df=output_file is not None
                            ),
                            x=5,
                            allowed_exceptions=(requests.exceptions.ConnectionError,)
                        )
                        if output_file:
                            do_append = os.path.isfile(output_file)
                            result.to_csv(
                                output_file,
                                mode="a" if do_append else "w",
                                header=not do_append,
                                index=False
                            )
                    except (RuntimeError, requests.exceptions.HTTPError):
                        pass
                if os.path.isdir(os.path.dirname(complete_marker)):
                    with open(complete_marker, "w") as _:
                        pass

        # Scrape the locations and attributes of personal weather stations
        elif args.target == Targets.FEATURES:
            all_features = []
            zoom_levels = sorted(set(args.zoom_levels))

            for zoom_level_ in tqdm_if_verbose(
                    zoom_levels,
                    verbose=args.progress and len(zoom_levels) > 1,
                    position=0,
                    desc="Zoom levels"
            ):
                zoom_level = str(zoom_level_)
                with open("generated/conus_tiles.csv", "r") as f:
                    reader = csv.reader(f)
                    tiles_xyz = [
                        tuple(map(int, xyz))
                        for xyz in reader
                        if xyz[-1] == zoom_level
                    ]

                for (x, y, z) in tqdm_if_verbose(
                        tiles_xyz,
                        verbose=args.progress,
                        miniters=1,
                        position=1,
                        desc="Finding stations (zoom={})".format(zoom_level)
                ):
                    station = scraper.features(
                        x=x,
                        y=y,
                        lod=z + 1,
                        as_df=output_file is not None
                    )
                    if output_file:
                        all_features.append(station)

            if output_file:
                logging.info("Concatenating stations and dropping duplicates")
                all_features = pandas.concat(all_features).drop_duplicates()
                logging.info("Writing stations to {}".format(output_file))
                all_features.to_file(output_file)


if __name__ == "__main__":
    main()
