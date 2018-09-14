# Use an official Python runtime as a parent image
FROM continuumio/miniconda3 AS base

RUN mkdir app
COPY ./requirements.txt /app/requirements.txt
# this is for geopandas
RUN apt-get update && \
apt-get install -y curl && \
apt-get install -y g++ && \
apt-get install -y make && \
apt-get install -y unzip && \
curl -L http://download.osgeo.org/libspatialindex/spatialindex-src-1.8.5.tar.gz | tar xz && \
cd spatialindex-src-1.8.5 && \
./configure && \
make && \
make install && \
ldconfig && \
# this is for fastparquet
pip install numpy && \
# Install any needed packages specified in requirements.txt
pip install --trusted-host pypi.python.org -r /app/requirements.txt



FROM base AS stage1

# Set the working directory to /app
WORKDIR /app
# Copy the current directory contents into the container at /app
#ADD ./src/clean_and_wrangle_1.py /app/src/clean_and_wrangle_1.py
#ADD ./data/cabs/cabs_green.sh /app/data/cabs/cabs_green.sh
#ADD ./data/cabs/cabs_yellow.sh /app/data/cabs/cabs_yellow.sh
#ADD ./data/gas/2018-2008_monthly_gas_NYC.csv /app/data/gas/2018-2008_monthly_gas_NYC.csv
#ADD ./data/traffic/traffic.sh /app/data/traffic/traffic.sh
#ADD ./data/traffic/process_traffic_data.py /app/data/traffic/process_traffic_data.py
#ADD ./data/transit/stations.sh /app/data/transit/stations.sh
#ADD ./data/transit/turnstile.sh /app/data/transit/turnstile.sh
#ADD ./data/weather/1409973.csv /app/data/weather/1409973.csv
#ADD ./bin/get_cab_data.sh /app/bin/get_cab_data.sh
#ADD ./bin/get_traffic_n_process.sh /app/bin/get_traffic_n_process.sh
#ADD ./bin/get_transit_data.sh /app/bin/get_transit_data.sh
#ADD ./bin/get_data.sh /app/bin/get_data.sh
#COPY . /app

RUN GIT_URL="https://github.com/sirisurab/transpred/archive/master.zip" && \
wget --no-check-certificate -O master.zip $GIT_URL && \
#git clone $GIT_URL && cd src && git checkout 8054d2db3cd16d6862bb140adda7060b4dbbc5cc
unzip master.zip && \
mv /app/transpred-master/* /app && \
chmod -R +x /app && \
rm -r /app/transpred-master && \
rm master.zip


#RUN chmod -R 777 /app
# Make port 80 available to the world outside this container
#EXPOSE 80

# Define environment variable
# ENV NAME World
#VOLUME /app
#RUN cd bin && \
#./get_data.sh
# Run src/clean_and_wrangle_1.py when the container launches
#CMD ["python", "src/stations.py"]
#CMD ["python", "src/traffic_links.py"]
#CMD ["python", "src/cabs.py"]
#CMD ["python", "src/clean_and_wrangle_1.py"]
#CMD ["cd", "bin"]
#CMD ["./bin/get_data.sh"]
ENTRYPOINT ["/bin/bash"]