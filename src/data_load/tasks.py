# prepare tasks for data loading(dl)
from typing import List, Tuple
from functools import reduce, partial
from utils import persistence as ps, http
import os
import urllib.error as u_err
import datetime as dt
from error_handling import errors
MIN_YEAR = 2010
MAX_YEAR = 2018

prefix_zero = lambda x: "0" + str(x) if x < 10 else str(x)


def make_transit(*args) -> List[str]:
    print('constructing transit tasks for years '+str(args))
    # each month is a task
    tasks_for_year = lambda tasks, year: tasks + [validate_transit_year(year)[2:]+"-"+str(month) for month in range(1, 13)]
    return reduce(tasks_for_year, list(*args), [])


def validate_transit_year(year: str):
    try:
        date = dt.date(int(year), 1, 1)
        if not (date.year <= MAX_YEAR and date.year >=MIN_YEAR) :
            raise Exception
    except:
        raise errors.InvalidYearError(year)
    else:
        return year

def make_traffic() -> List[str]:
    print('constructing traffic tasks')
    # create 20 tasks
    tasks: List[str] = [str(task_no) for task_no in range(1, 21)]
    return tasks



def make_cabs(*args) -> List[str]:
    print('constructing tasks for years '+str(args))
    tasks_for_year = lambda tasks, year: tasks + [year+"-"+str(quarter) for quarter in range(1, 5)]
    return reduce(tasks_for_year, list(*args), [])



def perform_transit(b_task: bytes) -> bool:
    task: str = str(b_task, 'utf-8')
    task_split: List[str] = task.split('-')
    year: str = task_split[0]
    month: int = int(task_split[1])
    url_part1: str = "http://web.mta.info/developers/data/nyct/turnstile/turnstile_"+year+prefix_zero(month)
    url_part2: str = ".txt"
    urls: List[str] = [url_part1+prefix_zero(day)+url_part2 for day in range(1, 32)]
    print('downloading from transit urls ')
    source_folder: str = os.path.dirname(__file__)+'/transit/'
    os.makedirs(source_folder, exist_ok=True)
    print('created source folder '+source_folder)
    status: bool = False
    try:
        for url in urls:
            print('downloading file from '+url)
            try:
                filename: str = http.download_from_url(url, source_folder)
            except u_err.HTTPError as err:
                # ignore bad urls
                if err.code == 404:
                    print('ignoring bad transit url ' + url)
                    # do not attempt to copy file to minio
                    continue
                else:
                    raise err

            except Exception as err:
                raise err

            print('copying file '+filename+' to bucket transit')
            status = ps.copy_file(dest_bucket='transit', file=filename, source=source_folder+filename)

    except Exception as err:
        raise err
    else:
        return status


def perform_traffic(b_task: bytes) -> bool:
    block_number: int = int(str(b_task, 'utf-8'))
    chunk_size = 60000000
    chunks_per_block = 5
    max_bl_num = 20
    url: str = "https://data.cityofnewyork.us/api/views/i4gi-tjb9/rows.csv?accessType=DOWNLOAD&bom=true&query=select+*"
    source_folder: str = os.path.dirname(__file__)+'/traffic/'
    os.makedirs(source_folder, exist_ok=True)
    print('created source folder '+source_folder)
    status: bool = False
    start_chunk: int = (block_number-1) * chunks_per_block + 1
    start_byte: int = (start_chunk-1) * chunk_size
    end_chunk: int = start_chunk+chunks_per_block+1
    last_chunk_in_file = max_bl_num * chunks_per_block
    try:
        for i in range(start_chunk, end_chunk):
            end_byte: int = start_byte+chunk_size-1
            byte_range: str = 'bytes='
            if not (end_chunk == last_chunk_in_file) :
                byte_range = byte_range+'%(start)i-%(end)i' % {'start': start_byte, 'end': end_byte}
            else:
                byte_range = byte_range+'%(start)i-' % {'start': start_byte}
            print('downloading file from '+url+' for byte range '+ byte_range)
            filename: str = http.download_chunk_from_url(url=url, folder=source_folder, byte_range=byte_range, filename='traffic_speed.part'+str(i))
            print('copying file '+filename+' to bucket traffic')
            status = ps.copy_file(dest_bucket='traffic', file=filename, source=source_folder+filename)
            start_byte = end_byte + 1

    except Exception as err:
        raise err
    else:
        return status


def perform_cabs(b_task: bytes) -> bool:
    task: str = str(b_task, 'utf-8')
    task_split: List[str] = task.split('-')
    year: str = task_split[0]
    quarter: int = int(task_split[1])
    prefix_zero = lambda x: "0"+str(x) if x < 10 else str(x)
    months = lambda quarter: range( (quarter-1)*3+1, (quarter-1)*3+4 )
    get_url = lambda month: "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_"+year+"-"+prefix_zero(month)+".csv"
    urls: List[str] = list(map(get_url, months(quarter)))
    print('downloading from urls '+str(urls))
    source_folder: str = os.path.dirname(__file__)+'/gcabs/'
    os.makedirs(source_folder, exist_ok=True)
    print('created source folder '+source_folder)
    status: bool = False
    try:
        for url in urls:
            print('downloading file from '+url)
            filename: str = http.download_from_url(url, source_folder)
            print('copying file '+filename+' to bucket gcabs')
            status = ps.copy_file(dest_bucket='gcabs', file=filename, source=source_folder+filename)

    except Exception as err:
        raise err
    else:
        return status


