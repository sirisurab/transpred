#!/bin/bash
cd ../data/cabs
echo "fetching yellow cab data"
/bin/bash cabs_yellow.sh &
echo "fetching green cab data"
/bin/bash cabs_green.sh &