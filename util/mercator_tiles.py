#!/usr/bin/env python3
#
# Calculate the X, Y, and Z of all Web Mercator tiles up to a certain zoom,
# optionally subsetting them to only those intersecting with a given polygon or
# multipolygon.
#
# If run as-is, this script will calculate all Web Mercator tiles between zoom
# levels 2 and 11 that intersect a 1-kilometer buffer of the continental US
# (external/conus_1km_buffer_wgs84.geojson, produced using the U.S. Census
# Bureau's 2010 state cartographic boundary files) and write the resulting x, y,
# and zoom values for each tile to generated/conus_tiles.csv.

import collections
import itertools
import os
import typing

import fiona
import fiona.crs
import mercantile
import shapely.geometry
import tqdm

DEFAULT_BOUNDS_GEOJSON = "external/conus_1km_buffer_wgs84.geojson"

DEFAULT_OUTPUT = "generated/conus_tiles.csv"

ROOT_TILE = mercantile.Tile(x=0, y=0, z=0)

GPKG_SCHEMA = {
    "geometry": "Polygon",
    "properties": {
        "x": "int",
        "y": "int",
        "z": "int"
    }
}

T_AnyPolygon = typing.Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon]


def tile_to_feature(tile: mercantile.Tile) -> dict:
    return {
        "geometry": mercantile.feature(tile)["geometry"],
        "properties": {
            "x": tile.x,
            "y": tile.y,
            "z": tile.z
        }
    }


# Adapted from https://docs.python.org/3/library/itertools.html#itertools-recipes
def batched(iterable: typing.Iterable, n: int) -> typing.Iterable[typing.Iterable]:
    """ Batch data into tuples of length n. The last batch may be shorter.

    Args:
        iterable: An iterable to be batched.
        n: The size of each batch.

    Returns: An iterable yielding slices of `iterable` of size `n`.
    """
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    while batch := tuple(itertools.islice(iter(iterable), n)):
        yield batch


def calculate_tiles_xyz(max_zoom: int = 12,
                        polygon: typing.Optional[T_AnyPolygon] = None
                        ) -> dict[int, list[tuple[int, int, int]]]:
    """ Calculate all Web Mercator tiles that intersect a given area.

    By default, the "given area" is the entire world (i.e. all tiles will be
    calculated).

    Args:
        max_zoom: The maximum zoom level to calculate tiles for.
        polygon: If given, tiles will be subset to only those intersecting with
            this polygon or multipolygon.

    Returns: A dict containing lists of tuples containing tile x, y, and zoom
    values, separated out by zoom level.
    """
    all_tiles_xyz: dict[int, list[tuple[int, int, int]]] = collections.defaultdict(list)
    for zoom in range(1, max_zoom + 1):
        if zoom == 1:
            tiles_xyz = [(0, 0, 0)]
        else:
            tiles_xyz = all_tiles_xyz[zoom - 1]
        for tile_xyz in tqdm.tqdm(tiles_xyz, desc="Zoom level {}".format(zoom), unit=" tiles"):
            tile = mercantile.Tile(*tile_xyz)
            for child_tile in mercantile.children(tile):
                if polygon:
                    child_tile_polygon = shapely.geometry.shape(
                        mercantile.feature(child_tile)["geometry"]
                    )
                    if not child_tile_polygon.intersects(polygon):
                        continue
                all_tiles_xyz[zoom].append((
                    child_tile.x, child_tile.y, child_tile.z
                ))
    return all_tiles_xyz


def export_tiles_gpkg(max_zoom: int = 12,
                      polygon: typing.Optional[T_AnyPolygon] = None,
                      batch_size: int = int(1e6),
                      output_directory: str = ".",
                      filename_template: str = "tiles_z{zoom:02d}.gpkg"):
    """ Export all Web Mercator tiles that intersect a given area to a
    GeoPackage file.

    To avoid excessive memory usage, no more than `batch_size` + 1 tiles are
    kept in-memory at any given time - each tile from the previous zoom is
    read from the disk separately and then discarded before the next tile is
    processed. Because of this, ETA is not available.

    Args:
        max_zoom: The maximum zoom level to calculate tiles for.
        polygon: If given, tiles will be subset to only those intersecting with
            this polygon or multipolygon.
        batch_size: The maximum number of tiles (+ 1) to keep in memory at once.
        output_directory: Where tiles should be saved to.
        filename_template: A template for the filename of each zoom of tiles.
            This should contain a placeholder called "zoom" which will be
            replaced with the current zoom.
    """
    if not os.path.isdir(output_directory):
        os.makedirs(output_directory)
    for zoom in range(1, max_zoom + 1):
        input_path = os.path.join(
            output_directory,
            filename_template.format(zoom=zoom - 1)
        )
        output_path = os.path.join(
            output_directory,
            filename_template.format(zoom=zoom)
        )
        output_mode = "a" if os.path.isfile(output_path) else "w"
        if zoom == 1:
            input_fp = None
            tiles = iter([tile_to_feature(ROOT_TILE)])
        else:
            input_fp = fiona.open(input_path, "r")
            tiles = iter(input_fp)
        progress = tqdm.tqdm(desc="Zoom level {}".format(zoom), unit=" tiles")
        for batch in batched(tiles, batch_size):
            progress.refresh()
            with fiona.open(output_path,
                            output_mode,
                            crs=fiona.crs.from_epsg(4326),
                            driver="GPKG",
                            schema=GPKG_SCHEMA
                            ) as output_fp:
                for tile_feature in batch:
                    tile = mercantile.Tile(*tile_feature["properties"].values())
                    for child_tile in mercantile.children(tile):
                        if polygon:
                            child_tile_polygon = shapely.geometry.shape(
                                mercantile.feature(child_tile)["geometry"]
                            )
                            if not child_tile_polygon.intersects(polygon):
                                continue
                        output_fp.write(tile_to_feature(child_tile))
                        progress.update(len(batch))
        progress.close()
        if input_fp:
            input_fp.close()


if __name__ == "__main__":
    import argparse
    import csv

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", default=DEFAULT_BOUNDS_GEOJSON)
    parser.add_argument("-o", "--output", default=DEFAULT_OUTPUT)

    args = parser.parse_args()

    if not os.path.isdir(os.path.dirname(args.output)):
        os.makedirs(os.path.dirname(args.output))

    with fiona.open(args.input, "r") as input_fp:
        bounds = shapely.geometry.shape(next(iter(input_fp))["geometry"])

    all_tiles = calculate_tiles_xyz(max_zoom=11, polygon=bounds)

    with open(args.output, "w") as output_fp:
        writer = csv.writer(output_fp, lineterminator="\n")
        writer.writerow(("x", "y", "z"))
        for zoom, tiles_xyz in all_tiles.items():
            if zoom < 2:
                continue
            for tile_xyz in tiles_xyz:
                writer.writerow(tile_xyz)
