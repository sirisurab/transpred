import sys
from error_handling import errors
from utils import persistence as ps
from utils import http
from urllib3.response import HTTPResponse
from typing import Tuple, List, Dict
from zipfile import ZipFile
from geopandas import GeoDataFrame, read_file
from shapely.geometry import Point
import pandas as pd

REFBASE_BUCKET: str = 'ref-base'


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
                taxi_zones_file: Tuple[str, HTTPResponse] = http.get_stream_from_url(taxi_zones_url)

                # unzip
                zip_path: str = '/tmp/cabs-ref-in/'
                zipfile: ZipFile = ZipFile(taxi_zones_file[1])
                zipfile.extractall(zip_path)
                zipfile.close()

                # process taxi shapefile
                cabs_out_path: str = '/tmp/cabs-ref-out/'
                cabs_filename: str = 'taxi_zones.shp'
                taxi_zone_df: GeoDataFrame = read_file(zip_path + cabs_filename).to_crs(crs)
                taxi_zone_df.drop(['Shape_Area', 'Shape_Leng', 'OBJECTID', 'borough', 'zone'],
                                  axis=1, inplace=True)
                taxi_zone_df.to_file(cabs_out_path+cabs_filename)

                ps.copy_files(dest_bucket=REFBASE_BUCKET, source_folder=cabs_out_path)

            elif task == 'transit':
                # load station file
                stations_url: str = 'http://web.mta.info/developers/data/nyct/subway/Stations.csv'
                #stations_file: Tuple[str, HTTPResponse] = http.get_stream_from_url(stations_url)
                usecols: List[str] = ['Station ID', 'GTFS Stop ID', 'Stop Name', 'Borough',
                                      'GTFS Latitude', 'GTFS Longitude']
                stations_df: pd.DataFrame = pd.read_csv(stations_url, header=0, usecols=usecols,
                                                        encoding='utf-8')
                stations_df.rename(columns=['STATION_ID', 'STOP_ID', 'STOP_NAME', 'BOROUGH',
                                            'LATITUDE', 'LONGITUDE'], inplace=True)

                geometry: List[Point] = [Point(xy) for xy in zip(stations_df.LONGITUDE, stations_df.LATITUDE)]
                stations_df.drop(['LATITUDE', 'LONGITUDE'], axis=1, inplace=True)
                stations_geodf: GeoDataFrame = GeoDataFrame(stations_df, crs=crs, geometry=geometry)
                stations_out_path: str = '/tmp/transit-ref-out/'
                stations_filename: str = 'stations.shp'
                stations_geodf.to_file(stations_out_path+stations_filename)

                ps.copy_files(dest_bucket=REFBASE_BUCKET, source_folder=stations_out_path)

        else:
            print('unrecognized ref-base load task %s' % task)
            raise errors.TaskTypeError('ref-base load '+task)
    return True


if __name__ == '__main__':
    print('loading ref files')
    status: bool = load_ref_files(sys.argv[1:])