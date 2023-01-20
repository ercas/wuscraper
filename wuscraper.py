#!/usr/bin/env python3
#
# Scraper for Weather Underground data.
#
# Contact: Edgar Castro <edgar_castro@g.harvard.edu>

import datetime
import enum
import json
import gzip
import os
import logging
import typing

import geopandas
import mercantile
import pandas
import requests

EMPTY_DICT = dict()

DEFAULT_OUTPUT_DIR = "output/"


def load_json_gz(path: str) -> typing.Union[dict, list]:
    """ Read data from a GZIP-compressed JSON file.

    Args:
        path: The file containing data.

    Returns: The contents of the JSON file.
    """
    with gzip.open(path, "rt") as input_fp:
        return json.load(input_fp)


def save_json_gz(path: str,
                 data: typing.Union[dict, list]):
    """ Save data to a GZIP-compressed JSON file.

    Args:
        path: The path to save data to.
        data: Data to be written.
    """
    with gzip.open(path, "wt") as output_fp:
        json.dump(data, output_fp)


def cached_eval(path: str,
                func: callable,
                read_function: typing.Callable[[str], typing.Any] = load_json_gz,
                write_function: typing.Callable[[str, typing.Any], None] = save_json_gz
                ) -> typing.Any:
    """ Evaluate a function, caching its data to a predetermined file on the
    disk, or read data from that file if it exists.

    Args:
        path: The file to write the results of `func()` to.
        func: A function whose results will be cached.
        read_function: A function that takes a path as input and returns data.
        write_function: A function that takes a path and data as input and
            writes the data to the specified path.

    Returns: If `path` exists, the contents of `path`; otherwise, the result of
    `func()`.
    """
    parent_directory = os.path.dirname(path)
    if parent_directory != "" and not os.path.isdir(parent_directory):
        os.makedirs(parent_directory)
    if os.path.isfile(path):
        return read_function(path)
    result = func()
    if result:
        write_function(path, result)
        return result


def retry_x_times(func: callable,
                  x: int,
                  allowed_exceptions: tuple[Exception, ...] = (Exception,),
                  raise_on_fail: bool = False,
                  *args,
                  **kwargs
                  ) -> typing.Optional[typing.Any]:
    error = None
    for i in range(x):
        try:
            return func(*args, **kwargs)
        except allowed_exceptions as last_error:
            error = last_error
            logging.exception("Exception")
            logging.info("Retrying {} (#{}/{})".format(callable, i + 2, x))
    if raise_on_fail:
        raise error


class Units(enum.Enum):
    METRIC = "m"
    ENGLISH = "e"


class WUScraper:
    class Endpoints(enum.Enum):
        FEATURES = "https://api.weather.com/v2/vector-api/products/614/features"
        HISTORICAL = "https://api.weather.com/v1/location/{station}/observations/historical.json"
        DAILY = "https://api.weather.com/v2/pws/history/daily"

    class Paths(enum.Enum):
        FEATURES = "{output_directory}/features/{x}_{y}_{lod}.json.gz"
        HISTORICAL = "{output_directory}/historical/{station}/{start_date}_to_{end_date}.json.gz"
        DAILY = "{output_directory}/daily/{station}/{month}.json.gz"

    def __init__(self,
                 api_key: str,
                 output_directory: str = DEFAULT_OUTPUT_DIR):
        self.api_key = api_key
        self.session = requests.Session()
        self.output_directory = output_directory

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    def get(self, *args, **kwargs) -> requests.Response:
        response = self.session.get(*args, **kwargs)
        logging.info(response.url)
        response.raise_for_status()
        return response

    # # Format of the `time` parameter
    #
    # time=[1]-[2]:[3] where
    #
    # - [1] = POSIX timestamp (milliseconds)
    # - [2] = POSIX timestamp (milliseconds) = [1] + 900000
    # - [3] = unknown integer, can be left off with no apparent effect
    #
    # Example: time=1672942500000-1672943400000:11
    #
    # Seems valid to set [1] to the current time rounded to the nearest 15
    # minutes (0, 15, 30, 45), then calculate [1] as [2] + 900000 (i.5. 15
    # minutes)
    #
    # # Relationship between `x`, `y`, `lod`, and `tile-size`
    #
    # Table showing the range of LOD. `x`, `y`, and `lod` appear to be describe
    # Web Mercator tiles with zoom = LOD - 1; unsure what `tile-size` does.
    # Equivalent Web Mercator zoom range is 2-11.
    #
    # | Location                             | x   | y   | lod | tile-size |
    # |--------------------------------------+-----+-----+-----+-----------|
    # | CONUS                                | 0   | 1   | 3   | 512       |
    # | CONUS except West Coast              | 3   | 2   | 4   | 512       |
    # | New England + Midwest + Mid-Atlantic | 9   | 12  | 5   | 512       |
    # | New England + NY + PA + NJ           | 9   | 12  | 6   | 512       |
    # | MA, NY, CT, RI, some VT, NH          | 18  | 23  | 7   | 512       |
    # | Massachusetts                        | 37  | 47  | 8   | 512       |
    # | Greater Boston                       | 77  | 94  | 9   | 512       |
    # | Greater Boston (zoomed)              | 153 | 189 | 10  | 512       |
    # | Boston + Cambridge                   | 153 | 189 | 10  | 512       |
    # | Boston                               | 310 | 378 | 11  | 512       |
    # | Boston (zoomed)                      | 620 | 757 | 12  | 512       |
    #
    # Example: x=38, y=47, lod=8 contains KMALEXIN3 (-71.20918, 42.43205) - see
    # below that LOD 8 = zoom 7
    #
    #     >>> mercantile.tile(lng=-71.20918, lat=42.43205, zoom=8)
    #     Tile(x=77, y=94, z=8)
    #     >>> mercantile.tile(lng=-71.20918, lat=42.43205, zoom=7)
    #     Tile(x=38, y=47, z=7)
    #
    # Seems that the best approach for scraping is to reproduce the Web Mercator
    # tiles as a vector layer and then intersect it with the areas of interest
    def features(self,
                 x: int,
                 y: int,
                 lod: int = 8,
                 tile_size: int = 512,  # Doesn't seem to change
                 time: typing.Optional[datetime.datetime] = None,  # As in [2]
                 time_diff: datetime.timedelta = datetime.timedelta(minutes=15),
                 as_df: bool = False
                 ) -> typing.Union[dict, geopandas.GeoDataFrame]:
        """ Retrieve a list of stations and their attributes.

        Args:
            x: The coordinate of the Web Mercator map tile's western bound.
            y: The coordinate of the Web Mercator map tile's northern bound.
            lod: Presumably "Level of Detail"; equivalent to the Web Mercator
                map tile's zoom level **plus one**.
            tile_size: Unknown use.
            time: The beginning of the time window from which active stations
                will be queried. This can't be too far into the past.
            time_diff: The length of the time window.
            as_df: If True, return as a GeoDataFrame.

        Returns: A dict corresponding to a GeoJson FeatureCollection, or, if
        `as_df=True`, a geopandas.GeoDataFrame object corresponding to the
        FeatureCollection.
        """
        if not time:
            time = datetime.datetime.now()
        time -= datetime.timedelta(
            microseconds=time.microsecond,
            seconds=time.second,
            minutes=time.minute % 15  # Last 15 minute
        )
        output_path = self.Paths.FEATURES.value.format(
            output_directory=self.output_directory,
            x=x,
            y=y,
            lod=lod
        )
        result = cached_eval(
            path=output_path,
            func=lambda: self.get(
                url=self.Endpoints.FEATURES.value,
                params={
                    "apiKey": self.api_key,
                    "x": x,
                    "y": y,
                    "lod": lod,
                    "tile-size": tile_size,
                    "time": "{}-{}".format(
                        round(time.timestamp() * 1000),
                        round((time + time_diff).timestamp() * 1000)
                    )
                }
            ).json()
        )
        if as_df:
            return geopandas.GeoDataFrame.from_features(result)
        return result

    def features_nearby_wgs84(self,
                              longitude: float,
                              latitude: float,
                              zoom: int,
                              *args, **kwargs
                              ) -> dict:
        """ A wrapper around `self.features()` that handles conversion of WGS-84
        coordinates to the encompassing Web Mercator map tiles, using the
        `mercantile` library.

        Args:
            longitude: The WGS-84 longitude.
            latitude: The WGS-84 latitude.
            zoom: The Web Mercator zoom level. This will automatically be
                converted into LOD.
            *args, **kwargs: Additional arguments to be passed to
                `self.features()`.

        Returns: The results from `self.features()`, using the Web Mercator map
        tile that encompasses (`longitude`, `latitude`) as the input.
        """
        tile = mercantile.tile(lng=longitude, lat=latitude, zoom=zoom)
        logging.info("({}, {}), zoom = {} -> {} (WU lod = {})".format(
            longitude, latitude, zoom,
            tile,
            tile.z + 1
        ))
        return self.features(x=tile.x, y=tile.y, lod=tile.z + 1, *args, **kwargs)

    def historical(self,
                   station: str,
                   start_date: datetime.datetime,
                   end_date: typing.Optional[datetime.datetime] = None,
                   units: Units = Units.METRIC,
                   as_df: bool = False,
                   overwrite: bool = False,
                   no_net: bool = False
                   ) -> typing.Union[dict, pandas.DataFrame]:
        """ Return hourly observations from a given weather station.

        Args:
            station: The ID of the weather station. Must be an NWS-operated
                weather station.
            start_date: The start of the date range from which records will be
                queried.
            end_date: The end of the date range, or, if None, the same as
                `start_date`.
            units: The unit system that measurements will be reported in.
            as_df: If True, return as a DataFrame.
            overwrite: If True, overwrites the cached data, if any.
            no_net: If True, use only cached files and throw an exception if
                there is none.

        Returns: A dict containing metadata in the "metadata" index and a list
        of observations, each being a dict, in the "observations" index. If
        `as_df` is True, returns a DataFrame built from items in the
        "observations" list.
        """
        if end_date is None:
            end_date = start_date + datetime.timedelta(days=1)
        output_path = self.Paths.HISTORICAL.value.format(
            output_directory=self.output_directory,
            station=station,
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d")
        )
        if os.path.isfile(output_path):
            if overwrite:
                os.remove(output_path)
        elif no_net:
            raise RuntimeError("{} does not exist".format(output_path))
        result = cached_eval(
            path=output_path,
            func=lambda: self.get(
                url=self.Endpoints.HISTORICAL.value.format(station=station),
                params={
                    "apiKey": self.api_key,
                    "startDate": start_date.strftime("%Y%m%d"),
                    "endDate": end_date.strftime("%Y%m%d"),
                    "units": units.value
                }
            ).json()
        )
        if as_df:
            return pandas.json_normalize(result["observations"])
        return result

    def daily(self,
              station: str,
              month: datetime.datetime,
              units: Units = Units.METRIC,
              format: str = "json",
              as_df: bool = False,
              overwrite: bool = False,
              no_net: bool = False
              ) -> typing.Union[dict, pandas.DataFrame]:
        month_start = datetime.datetime(month.year, month.month, 1)
        month_end = month_start + datetime.timedelta(days=31)
        month_end = month_end - datetime.timedelta(days=month_end.day)
        output_path = self.Paths.DAILY.value.format(
            output_directory=self.output_directory,
            station=station,
            month=month.strftime("%Y%m")
        )
        if os.path.isfile(output_path):
            if overwrite:
                os.remove(output_path)
        elif no_net:
            raise RuntimeError("{} does not exist".format(output_path))
        result = cached_eval(
            path=output_path,
            func=lambda: self.get(
                url=self.Endpoints.DAILY.value,
                params={
                    "apiKey": self.api_key,
                    "format": format,
                    "stationId": station,
                    "startDate": month_start.strftime("%Y%m%d"),
                    "endDate": month_end.strftime("%Y%m%d"),
                    "units": units.value
                }
            ).json()
        )
        # The API will return empty observation lists so we have to clear these
        # out
        if len(result["observations"]) == 0:
            os.remove(output_path)
            raise RuntimeError("No observations")
        if as_df:
            return pandas.json_normalize(result["observations"])
        return result
