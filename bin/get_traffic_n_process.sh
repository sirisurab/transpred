#!/bin/bash

cd ../data/traffic

/bin/bash traffic.sh && python process_traffic_data.py

#/bin/bash hydra-curl.sh traffic.txt $ROOT/traffic_speed.csv && python process_traffic_data.py

