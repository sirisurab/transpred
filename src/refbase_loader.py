import sys
from error_handling import errors
from utils import persistence as ps
from utils import http
from urllib3.response import HTTPResponse
from typing import Tuple, List, Dict, Callable
from zipfile import ZipFile
from geopandas import GeoDataFrame, read_file
from shapely.geometry import Point
import pandas as pd
import os
from io import BytesIO
import glob
from fuzzywuzzy import process, fuzz
from numpy import nan

REFBASE_BUCKET: str = 'ref-base'
TRANSIT_BUCKET: str = 'transit'


def add_fuzzy_station(df: pd.DataFrame) -> pd.DataFrame:
    col_func: Callable = lambda x: x.strip().lower() in ['station']
    s3 = ps.get_s3fs_client()
    # get any one raw turnstile file from transit bucket
    file: str = ps.get_all_filenames(bucket=TRANSIT_BUCKET)[0]
    file_obj = s3.open('s3://' + TRANSIT_BUCKET + '/' + file, 'r')
    transit_df = pd.read_csv(file_obj, header=0, encoding='utf-8',
                             usecols=col_func, skipinitialspace=True,
                             low_memory=False, squeeze=True)
    transit_df.rename(columns=lambda x: x.strip().lower(), inplace=True)
    transit_df.drop_duplicates(inplace=True)
    transit_df.dropna(inplace=True)

    stations_fuzzy: List = []
    for station in transit_df:
        station_fuzz_1 = process.extractOne(station, df.stop_name, scorer=fuzz.ratio)
        station_fuzz_2 = process.extractOne(station, df.stop_name, scorer=fuzz.partial_ratio)
        station_fuzz_3 = process.extractOne(station, df.stop_name, scorer=fuzz.token_sort_ratio)
        stations = {station_fuzz_1[0]: station_fuzz_1[1], station_fuzz_2[0]: station_fuzz_2[1],
                    station_fuzz_3[0]: station_fuzz_3[1]}
        station_max = max(stations.keys(), key=lambda key: stations[key])
        if stations[station_max] > 88:
            stations_fuzzy.append(station_max)
        else:
            stations_fuzzy.append(nan)

    st_df: pd.DataFrame = pd.concat([transit_df.reset_index(drop=True),
                                    pd.DataFrame(stations_fuzzy)],
                                    axis=1, ignore_index=True).\
        rename(columns={0: 'tsstation', 1: 'fuzzy_ts_station'})
    df = df.merge(st_df.dropna(), how='left', left_on='stop_name', right_on='fuzzy_ts_station', copy=False)

    return df.drop(columns=['fuzzy_ts_station'])


def load_ref_files(*args) -> bool:
    for task in list(*args):
        print('loading ref files for %s' % task)

        if task in ['cabs', 'transit']:
            # create ref-base bucket
            ps.create_bucket(REFBASE_BUCKET)
            crs: Dict[str, str] = {'init': 'epsg:4326'}
            if task == 'cabs':
                # load taxi zone files
                taxi_zones_url: str = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi_zones.zip'
                taxi_zones_file: Tuple = http.get_stream_from_url(taxi_zones_url)
                print('zip file response status %s' % taxi_zones_file[1].status)
                # unzip
                zip_path: str = '/tmp/cabs-ref-in/'
                zipfile: ZipFile = ZipFile(BytesIO(taxi_zones_file[1].read()))
                zipfile.extractall(zip_path)
                zipfile.close()

                # process taxi shapefile
                cabs_out_path: str = '/tmp/cabs-ref-out/'
                cabs_filename: str = 'taxi_zones.shp'
                taxi_zone_df: GeoDataFrame = read_file(zip_path + cabs_filename).to_crs(crs)
                taxi_zone_df.drop(['Shape_Area', 'Shape_Leng', 'OBJECTID', 'borough', 'zone'],
                                  axis=1, inplace=True)
                taxi_zone_df.drop_duplicates(inplace=True)
                taxi_zone_df.dropna(inplace=True)
                os.makedirs(cabs_out_path, exist_ok=True)
                taxi_zone_df.to_file(cabs_out_path+cabs_filename)
                taxi_zone_files: List[str] = glob.glob(cabs_out_path+'*')
                os.chdir(cabs_out_path)
                with ZipFile('taxi_zones.zip', 'w') as zipfile:
                    for file in taxi_zone_files:
                        zipfile.write(file.rsplit('/', 1)[1])
                #ps.copy_files(dest_bucket=REFBASE_BUCKET, source_folder=cabs_out_path)
                ps.copy_file(dest_bucket=REFBASE_BUCKET, source=cabs_out_path+'taxi_zones.zip', file='taxi_zones.zip')

            elif task == 'transit':
                # load station file
                stations_url: str = 'http://web.mta.info/developers/data/nyct/subway/Stations.csv'
                usecols: List[str] = ['Station ID', 'GTFS Stop ID', 'Stop Name', 'Borough',
                                      'GTFS Latitude', 'GTFS Longitude']
                stations_df: pd.DataFrame = pd.read_csv(stations_url, header=0, usecols=usecols,
                                                        encoding='utf-8')
                stations_df.rename(columns={'Station ID': 'station_id', 'GTFS Stop ID': 'stop_id',
                                            'Stop Name': 'stop_name', 'Borough': 'borough',
                                            'GTFS Latitude': 'latitude', 'GTFS Longitude': 'longitude'},
                                   inplace=True)

                stations_df.drop_duplicates(inplace=True)
                stations_df.dropna(inplace=True)

                # add fuzzy station name from turnstile data
                stations_df = add_fuzzy_station(df=stations_df)

                geometry: List[Point] = [Point(xy) for xy in zip(stations_df.longitude, stations_df.latitude)]
                stations_df.drop(['latitude', 'longitude'], axis=1, inplace=True)
                stations_geodf: GeoDataFrame = GeoDataFrame(stations_df, crs=crs, geometry=geometry)
                stations_out_path: str = '/tmp/transit-ref-out/'
                os.makedirs(stations_out_path, exist_ok=True)
                stations_filename: str = 'stations.shp'
                stations_geodf.to_file(stations_out_path+stations_filename)
                station_files: List[str] = glob.glob(stations_out_path+'*')
                os.chdir(stations_out_path)
                with ZipFile('stations.zip', 'w') as zipfile:
                    for file in station_files:
                        zipfile.write(file.rsplit('/', 1)[1])
                #ps.copy_files(dest_bucket=REFBASE_BUCKET, source_folder=stations_out_path)
                ps.copy_file(dest_bucket=REFBASE_BUCKET, source=stations_out_path+'stations.zip', file='stations.zip')

        else:
            print('unrecognized ref-base load task %s' % task)
            raise errors.TaskTypeError('ref-base load '+task)
    return True


if __name__ == '__main__':
    print('loading ref files')
    status: bool = load_ref_files(sys.argv[1:])