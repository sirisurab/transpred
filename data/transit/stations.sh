#!/bin/bash
rm Stations.csv

echo "spawning the station data thread"
curl -O http://web.mta.info/developers/data/nyct/subway/Stations.csv
echo "spawned the station data thread"