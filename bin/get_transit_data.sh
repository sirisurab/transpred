#!/bin/bash

cd ../data/transit
echo "fetching station data"
/bin/bash stations.sh &
echo "fetching turnstile data"
/bin/bash turnstile.sh &