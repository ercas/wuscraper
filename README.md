# wuscraper - scraper for Weather Underground

This repository contains code to scrape data from the Weather Underground (WUnderground) API.

## Disclaimer

The API is no longer documented after acquisition of WUnderground by The Weather Channel and the code in this repository is the result of a reverse engineering effort based on analysis of captured network requests and responses. As such, although very effort has been made to ensure that the scraped data will be as complete as possible, no guarantees can be made that all available data is, indeed, scraped (or even scrapable).

## Usage

Normal usage of `wuscraper` is as follows (further details in subsequent sections):

1. Obtain an API key from WUnderground and paste it into `./api_key.txt`. This can be obtained by inspecting AJAX requests to `api.weather.com` endpoints from the [Wundermap](https://www.wunderground.com/wundermap) page for the `apiKey=` URL parameter.
2. (Optional) Use `mercator_tiles.py` to export a list of Web Mercator tiles that will be used for station discovery. An example file `generated/conus_tiles.csv` has been provided that contains all tiles for the continental U.S.; scrapes of the continental U.S. can skip this step.
3. Use `scrape.py` to discover weather stations.
4. Feed the discovered weather stations into `scrape.py` to scrape for weather observations.
5. (Optional) Export the scraped data to a CSV file for use in other analyses.

### Obtaining an API key

To obtain an API key, first open your browser's Network console (Ctrl+Shift+E in Firefox or Ctrl+Shift+I -> Network tab n Chrome), `mitmproxy`, or a similar network monitor and then navigate to the Underground [Wundermap](https://www.wunderground.com/wundermap). Filtering for `api.weather.com` should yield requests hat pass `apiKey=` as a URL parameter. Write this API to `./api_key.txt`, or save it for use in the `--api-key` parameter of `scrape.py`.

### Generating Web Mercator tiles

A list of Web Mercator tiles must be created for use in station discovery. The file `util/mercator_tiles.py` can generate this file for you if given a shapefile covering the area of interest (projected to WGS-84, in any format readable by OGR). See `--help` for more information:

    usage: mercator_tiles.py [-h] [-i INPUT] [-o OUTPUT] [-z MAX_ZOOM]

    optional arguments:
      -h, --help            show this help message and exit
      -i INPUT, --input INPUT
                            The path to a shapefile readable by OGR that contains
                            the scrape area extents, in WGS-84.
      -o OUTPUT, --output OUTPUT
                            The path of the CSV file to be written to.
      -z MAX_ZOOM, --max-zoom MAX_ZOOM
                            The maximum zoom level that will be generated.

Example:

    util/mercator_tiles.py --input external/conus_1km_buffer_wgs84.geojson -o conus_tiles.csv

### Discovering stations

The WUnderground API no longer contains an endpoint for station discovery, and we will have to do this ourselves using the `scrape.py` subparser `features`. See `--help` for more information:

    usage: scrape.py features [-h] [-a API_KEY] [-d SCRAPE_DIRECTORY]
                              [-o OUTPUT_FILE] [-p] [-v] [-t TILES]
                              [zoom_levels ...]

    positional arguments:
      zoom_levels           A list of zoom levels to scrape

    optional arguments:
      -h, --help            show this help message and exit
      -a API_KEY, --api-key API_KEY
                            The API key to use for scraping
      -d SCRAPE_DIRECTORY, --scrape-directory SCRAPE_DIRECTORY
                            The directory that scraped data will be saved to
      -o OUTPUT_FILE, --output-file OUTPUT_FILE
                            The file to export scraped results to, if any
      -p, --progress        Show a progress bar
      -v, --verbose         Change log level to logging.DEBUG
      -t TILES, --tiles TILES
                            The path to a file created by `util/mercator_tiles.py`

Example:

    ./scrape.py features \
        --scrape-directory output \
        --tiles conus_tiles.csv \
        --progress

This will have created files in `output/features`:

    $ ls output/features/ | shuf | head
    564_870_12.json.gz
    237_409_11.json.gz
    390_742_12.json.gz
    171_397_11.json.gz
    85_175_10.json.gz
    528_803_12.json.gz
    580_812_12.json.gz
    430_745_12.json.gz
    626_763_12.json.gz
    550_820_12.json.gz

Each one of these files is a valid GeoJSON file that can be opened in a program like [QGIS](https://www.qgis.org/) if unzipped. Many of these files will be empty, but we can see that, for example, the tile (273, 409, 11) has some data:

    $ zcat output/features/273_409_11.json.gz  | jq -c .features[] | wc -l
    62

The scraped features can be exported using by running the same command and additionally passing `--output-file` - the script will not re-retrieve data that has already been retrieved. The output file can be in any format supported by OGR.

### Scraping weather data

The primary scraping functionality is through the `daily` subparser of `scrape.py`, which retrieves hourly data from personal NWS-operated weather stations. There is also a `historical` subparser that can retrieve hourly data, though only from NWS-operated weather stations. See `--help` for more information:

    usage: scrape.py daily [-h] [-a API_KEY] [-d SCRAPE_DIRECTORY] [-o OUTPUT_FILE] [-p] [-v] [-s START_DATE] [-e END_DATE] stations [stations ...]

    positional arguments:
      stations              A list of NWS observed weather stations to scrape, separated by spaces

    optional arguments:
      -h, --help            show this help message and exit
      -a API_KEY, --api-key API_KEY
                            The API key to use for scraping
      -d SCRAPE_DIRECTORY, --scrape-directory SCRAPE_DIRECTORY
                            The directory that scraped data will be saved to
      -o OUTPUT_FILE, --output-file OUTPUT_FILE
                            The file to export scraped results to, if any
      -p, --progress        Show a progress bar
      -v, --verbose         Change log level to logging.DEBUG
      -s START_DATE, --start-date START_DATE
                            The first month to scrape data for (YYYY-MM-DD)
      -e END_DATE, --end-date END_DATE
                            The first month to scrape data for (YYYY-MM-DD)

Unlike the `features` subparser, which can take a file as input, this subparser only takes weather station IDs as input. This was done to facilitate parallel scraping and the building and modification of input lists via command-line tools.

The following code can be used to parse the scraped features and create an input list:

    find output/features/ -name '*.json.gz' |
        xargs -n 1 zcat |
        jq -r .features[].id |
        sort |
        uniq > features-list.txt

The input list can then be used as follows:

    cat features-list.txt |
        xargs ./scrape.py daily --scrape-directory output/ --progress

If you have [GNU Parallel](https://www.gnu.org/software/parallel/) installed, you can use multiple threads to scrape in parallel:

    cat features-list.txt |
        parallel --jobs 4 --bar ./scrape.py daily --scrape-directory output/ --progress

As a side note, the script will also create a text file called `complete` in the directory of each weather station after the requested date range has been scraped completely. This can be exploited to, for example, resume interrupted scrapes using something like `uniq`:

    (
        find output/daily/ -name complete |
            xargs -n 1 dirname |
            xargs -n 1 basename
        cat -features-list.txt
    ) |
        sort |
        uniq -u |
        xargs ./scrape.py daily --scrape-directory output/ --progress

### Exporting scraped data

The scraped data is available in the form of raw JSON responses. Additional code exists to facilitate the parsing and exporting of this JSON data into CSV files that can be read more easily by other programs.

Both the `daily` and `historical` subparsers have a `--output-file` argument that can be used to specify a CSV file to dump scraped data to after the scrape has completed. However, the scraper has been designed to run single-threaded; multithreaded parsing and exporting facilities can instead be accessed usinf the `export-daily` and `export-historical` subparsers. See `--help` for more information:

    usage: scrape.py export-daily [-h] [-a API_KEY] [-d SCRAPE_DIRECTORY] [-o OUTPUT_FILE] [-p] [-v] [-j JOBS]

    optional arguments:
      -h, --help            show this help message and exit
      -a API_KEY, --api-key API_KEY
                            The API key to use for scraping
      -d SCRAPE_DIRECTORY, --scrape-directory SCRAPE_DIRECTORY
                            The directory that scraped data will be saved to
      -o OUTPUT_FILE, --output-file OUTPUT_FILE
                            The file to export scraped results to, if any
      -p, --progress        Show a progress bar
      -v, --verbose         Change log level to logging.DEBUG
      -j JOBS, --jobs JOBS  The number of parallel workers to use to read and process the raw JSON data.

For example:

    ./scrape.py export-daily \
        --scrape-directory output/ \
        --output-file conus-observations.csv \
        --jobs 4 \
        --progress