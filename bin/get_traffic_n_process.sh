#!/bin/bash

cd ../data/traffic

echo "fetching traffic data"
/bin/bash traffic.sh && echo "processing traffic and link data" && python process_traffic_data.py