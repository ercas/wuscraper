#!/usr/bin/env python3

import argparse
import csv
import enum
import logging
import os
import typing

import pandas
import requests
import tqdm

import wuscraper

API_KEY_PATH = "api_key.txt"

class Targets(enum.Enum):
    DAILY = enum.auto()
    FEATURES = enum.auto()
    HISTORICAL = enum.auto()
    EXPORT_DAILY = enum.auto()
    EXPORT_FEATURES = enum.auto()
    EXPORT_HISTORICAL = enum.auto()

def tqdm_if_verbose(iterable: typing.Iterable,
                    verbose: bool = True,
                    *tqdm_args, **tqdm_kwargs) -> typing.Iterable:
    if verbose:
        return tqdm.tqdm(iterable, *tqdm_args, **tqdm_kwargs)
    return iter(iterable)

def main(api_key):
    parser = argparse.ArgumentParser()

    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument(
        "-a", "--api-key", type=str, default=api_key,
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

    subparsers = parser.add_subparsers(title="Target")

    daily_parser = subparsers.add_parser("daily", parents=[parent_parser])
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

    historical_parser = subparsers.add_parser("historical", parents=[parent_parser])
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

    features_parser = subparsers.add_parser("features", parents=[parent_parser])
    features_parser.set_defaults(target=Targets.FEATURES)
    features_parser.add_argument(
        "zoom_levels", nargs="*", type=int, default=list(range(1, 11+1))
    )

    args = parser.parse_args()
    logging.info(args)

    if not hasattr(args, "api_key"):
        parser.print_help()
        return

    with wuscraper.WUScraper(api_key=args.api_key,
                             output_directory=args.scrape_directory) as scraper:

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
                if os.path.isfile(complete_marker):
                    continue
                for dt in tqdm_if_verbose(
                        list(reversed(date_range)),
                        verbose=args.progress,
                        position=1,
                        miniters=1,
                        desc=station
                ):
                    try:
                        wuscraper.retry_x_times(
                            func=lambda: scraper.daily(
                                station=station,
                                month=dt
                            ),
                            x=5,
                            allowed_exceptions=(requests.exceptions.ConnectionError,)
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
                if os.path.isfile(complete_marker):
                    continue
                for dt in tqdm_if_verbose(
                        list(reversed(date_range)),
                        verbose=args.progress,
                        position=1,
                        miniters=1,
                        desc=station
                ):
                    try:
                        wuscraper.retry_x_times(
                            func=lambda: scraper.historical(
                                station=station,
                                start_date=dt
                            ),
                            x=5,
                            allowed_exceptions=(requests.exceptions.ConnectionError,)
                        )
                    except (RuntimeError, requests.exceptions.HTTPError):
                        pass
                if os.path.isdir(os.path.dirname(complete_marker)):
                    with open(complete_marker, "w") as _:
                        pass

        # Scrape the locations and attributes of personal weather stations
        elif args.target == Targets.FEATURES:
            all_stations = []
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

                all_stations += [
                    station["id"]
                    for (x, y, z) in tqdm_if_verbose(
                        tiles_xyz,
                        verbose=args.progress,
                        miniters=1,
                        position=1,
                        desc="Finding stations (zoom={})".format(zoom_level)
                    )
                    for station in scraper.features(x=x, y=y, lod=z + 1)["features"]
                ]

            for station in sorted(set(all_stations)):
                print(station)

if __name__ == "__main__":
    if os.path.isfile(API_KEY_PATH):
        with open(API_KEY_PATH, "r") as input_fp:
            api_key = input_fp.read().strip()
            logging.info("API key: \"{}\"".format(api_key))
            main(api_key)
    else:
        with open(API_KEY_PATH, "w") as output_fp:
            output_fp.write("Paste API key here\n")
        print("Please paste your API key into {}".format(os.path.realpath(API_KEY_PATH)))