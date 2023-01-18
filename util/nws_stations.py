#!/usr/bin/env python3
#
# Retrieve a list of the IDs of all stations observed by the National Weather
# Service.

import csv
import os
import xml.etree.ElementTree

import requests

STATIONS_XML = "https://w1.weather.gov/xml/current_obs/index.xml"

response = requests.get(STATIONS_XML)
response.raise_for_status()
station_index = xml.etree.ElementTree.fromstring(response.content)

if not os.path.isdir("generated"):
    os.makedirs("generated")

with open("generated/stations.csv", "w") as output_fp:
    writer = csv.writer(output_fp, lineterminator="\n")
    writer.writerow(["ID", "STATE", "NAME", "LONGITUDE", "LATITUDE"])
    for station_element in station_index.findall(".//station"):
        writer.writerow([
            station_element.find(attribute).text
            for attribute in [
                "station_id",
                "state",
                "station_name",
                "longitude",
                "latitude"
            ]
        ])
