#!/bin/bash
cd /app/bin
/bin/bash get_cab_data.sh &
/bin/bash get_traffic_n_process.sh &
/bin/bash get_transit_data.sh &

#cd ../transpred
#python clean_and_wrangle_1.py