#!/bin/bash
cd bin
echo "fetching cab data"
/bin/bash get_cab_data.sh &
echo "fetching transit data"
#/bin/bash get_traffic_n_process.sh &
/bin/bash get_transit_data.sh &

wait