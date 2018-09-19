# Use an official Python runtime as a parent image
FROM continuumio/miniconda3 AS base

RUN mkdir app
# this is for geopandas
RUN apt-get update && \
apt-get install -y curl && \
apt-get install -y g++ && \
apt-get install -y make && \
apt-get install -y redis-tools && \
apt-get install -y unzip && \
curl -L http://download.osgeo.org/libspatialindex/spatialindex-src-1.8.5.tar.gz | tar xz && \
cd spatialindex-src-1.8.5 && \
./configure && \
make && \
make install && \
ldconfig

RUN apt-get install -y libfreetype6-dev && \
#apt-get install -y pkg-config && \
# this is for fastparquet
pip install numpy


FROM base AS app-pkgs
# Install any needed packages specified in requirements_1.txt
COPY ./requirements_1.txt /app/requirements_1.txt
COPY ./requirements_2.txt /app/requirements_2.txt
RUN pip install --trusted-host pypi.python.org -r /app/requirements_1.txt
RUN pip install --trusted-host pypi.python.org -r /app/requirements_2.txt



FROM app-pkgs AS app

# Set the working directory to /app
WORKDIR /app

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
ENTRYPOINT ["/bin/bash"]