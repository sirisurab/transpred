# prepare tasks for data loading(dl)
from typing import List, Tuple, Dict, Union, Optional, Callable
from functools import reduce, partial
from utils import persistence as ps, http
from utils import dask
import os
import urllib.error as u_err
import datetime as dt
from error_handling import errors
import dask.dataframe as dd
from data_tools import task_map
from dask.distributed import Client
from data_tools import row_operations as row_ops
import calendar as cal
from pandas import to_datetime, read_csv, Timedelta

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



def make_gcabs(*args) -> List[str]:
    print('constructing gcabs tasks for years '+str(args))
    tasks_for_year = lambda tasks, year: tasks + [year+"-"+str(quarter) for quarter in range(1, 5)]
    return reduce(tasks_for_year, list(*args), [])

def make_ycabs(*args) -> List[str]:
    print('constructing ycabs tasks for years '+str(args))
    tasks_for_year = lambda tasks, year: tasks + [year+"-"+str(month) for month in range(1, 13)]
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


def perform_tsfare(b_task: bytes) -> bool:
    task: str = str(b_task, 'utf-8')
    task_split: List[str] = task.split('-')
    year: str = task_split[0]
    month: int = int(task_split[1])
    url_part1: str = "http://web.mta.info/developers/data/nyct/fares/fares_"+year+prefix_zero(month)
    url_part2: str = ".csv"
    #urls: List[str] = [url_part1+prefix_zero(day)+url_part2 for day in range(1, 32)]
    print('downloading from transit fare urls ')
    source_folder: str = os.path.dirname(__file__)+'/tsfare/'
    os.makedirs(source_folder, exist_ok=True)
    print('created source folder '+source_folder)
    status: bool = False
    td : Timedelta = Timedelta(14, unit='d')
    try:
        for day in range(1, 32):
            url = url_part1 + prefix_zero(day) + url_part2
            print('downloading file from '+url)
            try:
                filename: str = http.download_from_url(url, source_folder)
            except u_err.HTTPError as err:
                # ignore bad urls
                if err.code == 404:
                    print('ignoring bad transit fare url ' + url)
                    # do not attempt to copy file to minio
                    continue
                else:
                    raise err

            except Exception as err:
                raise err
            df = read_csv(source_folder+filename, skiprows=2)
            date: str = prefix_zero(month)+'/'+prefix_zero(day)+'/20'+year
            df['date'] = to_datetime(date, format='%m/%d/%Y') - td
            df.to_csv(source_folder+filename)
            print('copying file '+filename+' to bucket tsfare')
            status = ps.copy_file(dest_bucket='tsfare', file=filename, source=source_folder+filename)

    except Exception as err:
        raise err
    else:
        return status


def perform_traffic(b_task: bytes) -> bool:
    block_number: int = int(str(b_task, 'utf-8'))
    #url: str = "https://data.cityofnewyork.us/api/views/i4gi-tjb9/rows.csv?accessType=DOWNLOAD&bom=true&query=select+*"
    url: str = "https://data.bloomington.in.gov/dataset/117733fb-31cb-480a-8b30-fbf425a690cd/resource/d5ba88f9-5798-46cd-888a-189eb59f7b46/download/traffic-counts2013-2015.csv"
    content_length: int = http.get_content_length(url)
    total_blocks: int = 20
    chunks_per_block = 5
    chunk_size = content_length // (total_blocks*chunks_per_block)
    print('content length is %(length)i and chunk size is %(cs)i' % {'length': content_length, 'cs': chunk_size})
    source_folder: str = os.path.dirname(__file__)+'/traffic/'
    os.makedirs(source_folder, exist_ok=True)
    print('created source folder '+source_folder)
    status: bool = False
    start_chunk: int = (block_number-1) * chunks_per_block + 1
    start_byte: int = (start_chunk-1) * chunk_size
    end_chunk: int = start_chunk+chunks_per_block+1
    last_chunk_in_file = total_blocks * chunks_per_block
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


def perform_cabs(cab_type: str, b_task: bytes) -> bool:
    bucket: str
    if cab_type == 'green':
        file_suffix = 'green'
        bucket = 'gcabs'
    elif cab_type == 'yellow':
        file_suffix = 'yellow'
        bucket = 'ycabs'
    task: str = str(b_task, 'utf-8')
    task_split: List[str] = task.split('-')
    year: str = task_split[0]
    urls: List[str]
    if cab_type == 'green':
        quarter: int = int(task_split[1])
        months = lambda quarter: range( (quarter-1)*3+1, (quarter-1)*3+4 )
        get_url = lambda month: 'https://s3.amazonaws.com/nyc-tlc/trip+data/'+file_suffix+'_tripdata_'+year+'-'+prefix_zero(month)+'.csv'
        urls = list(map(get_url, months(quarter)))
    elif cab_type == 'yellow':
        month: int = int(task_split[1])
        #months = lambda bimonth: range( (bimonth-1)*2+1, (bimonth-1)*2+3 )
        #get_url = lambda month: 'https://s3.amazonaws.com/nyc-tlc/trip+data/'+file_suffix+'_tripdata_'+year+'-'+prefix_zero(month)+'.csv'
        #urls = list(map(get_url, months(bimonth)))
        urls = ['https://s3.amazonaws.com/nyc-tlc/trip+data/'+file_suffix+'_tripdata_'+year+'-'+prefix_zero(month)+'.csv']


    print('downloading from urls '+str(urls))
    source_folder: str = os.path.dirname(__file__)+'/'+bucket+'/'
    os.makedirs(source_folder, exist_ok=True)
    print('created source folder '+source_folder)
    status: bool = False
    try:
        for url in urls:
            print('downloading file from '+url)
            filename: str = http.download_from_url(url, source_folder)
            print('copying file '+filename+' to bucket '+bucket)
            status = ps.copy_file(dest_bucket=bucket, file=filename, source=source_folder+filename)

    except Exception as err:
        raise err
    else:
        return status


def to_parquet(df: dd.DataFrame, out_bucket: str, folder: str, compute: bool = True) -> bool:
    try:
        s3_out_url: str = 's3://' + out_bucket + '/' + folder
        s3_options: Dict = ps.fetch_s3_options()
        dd.to_parquet(df=df,
                      path=s3_out_url,
                      engine='fastparquet',
                      compute=compute,
                      compression='lz4',
                      storage_options=s3_options)
    except Exception as err:
        print('error while saving to parquet to path %(path)s - %(error)s'
               % {'path': out_bucket + '/' + folder, 'error': str(err)})
        raise err
    else:
        return True


def perform_cabs_dask(task_type: str, years: List[str]) -> bool:
    file_prefix: str
    cab_type: str = task_type.split('-', 1)[1]
    if cab_type == 'gcabs':
        file_prefix = 'green'
    elif cab_type == 'ycabs':
        file_prefix = 'yellow'

    task_type_map: Dict = task_map.task_type_map[task_type]
    out_bucket: str = task_type_map['out']
    cols: Dict[str, str] = task_type_map['cols']
    parse_dates: bool = task_type_map['dates']['parse']
    dates: Union[bool, Dict[str, List[str]], List[str]]
    date_parser: Optional[Callable]
    if parse_dates:
        date_parser = task_type_map['dates']['parser']
    else:
        dates = False
        date_parser = None
    #dtypes: Dict[str, str] = task_type_map['dtypes']

    status: bool = False
    try:
        client: Client = dask.create_dask_client(num_workers=8)
        special_case: bool = False
        normal_case: bool = False
        month_st_sp: int
        month_end_sp: int
        month_st_norm: int
        month_end_norm: int
        for year in years:
            if int(year) == 2016:
                special_case = True
                normal_case = True
                month_st_sp = 7
                month_end_sp = 13
                month_st_norm = 1
                month_end_norm = 7
            elif int(year) > 2016:
                special_case = True
                normal_case = False
                month_st_sp = 1
                month_end_sp = 13
            elif int(year) < 2016:
                special_case = False
                normal_case = True
                month_st_norm = 1
                month_end_norm = 13

            if special_case:
                if task_type == 'dl-gcabs':
                    usecols = [1, 2, 5, 6, 7, 8]
                    names = ['pudatetime', 'dodatetime', 'pulocationid', 'dolocationid', 'passengers', 'distance']
                else:
                    usecols = [1, 2, 3, 4, 7, 8]
                    names = ['pudatetime', 'dodatetime', 'passengers', 'distance', 'pulocationid', 'dolocationid']

                urls = ['https://s3.amazonaws.com/nyc-tlc/trip+data/' +
                                   file_prefix + '_tripdata_' + year + '-' + prefix_zero(month) + '.csv'
                                   for month in range(month_st_sp, month_end_sp)]

                df = dd.read_csv(urlpath=urls,
                                 header=None,
                                 usecols=usecols,
                                 names=names,
                                 parse_dates=['pudatetime', 'dodatetime'],
                                 date_parser=date_parser,
                                 skipinitialspace=True,
                                 skip_blank_lines=True,
                                 converters={
                                     'passengers': row_ops.clean_num,
                                     'distance': row_ops.clean_num,
                                     'dolocationid': row_ops.clean_num,
                                     'pulocationid': row_ops.clean_num
                                 },
                                 encoding='utf-8'
                                 )
                to_parquet(df=df, out_bucket=out_bucket, folder=year+'/special/', compute=True)

            if normal_case:
                if task_type == 'dl-gcabs':
                    usecols = [1, 2, 5, 6, 7, 8, 9, 10]
                    names = ['pudatetime', 'dodatetime', 'pulongitude', 'pulatitude', 'dolongitude', 'dolatitude', 'passengers', 'distance']
                else:
                    usecols = [1, 2, 3, 4, 5, 6, 9, 10]
                    names = ['pudatetime', 'dodatetime', 'passengers', 'distance', 'pulongitude', 'pulatitude', 'dolongitude', 'dolatitude']
                urls = ['https://s3.amazonaws.com/nyc-tlc/trip+data/' +
                        file_prefix + '_tripdata_' + year + '-' + prefix_zero(month) + '.csv'
                        for month in range(month_st_norm, month_end_norm)]
                df = dd.read_csv(urlpath=urls,
                                 header=None,
                                 usecols=usecols,
                                 names=names,
                                 parse_dates=['pudatetime', 'dodatetime'],
                                 date_parser=date_parser,
                                 skipinitialspace=True,
                                 skip_blank_lines=True,
                                 converters={
                                     'passengers': row_ops.clean_num,
                                     'distance': row_ops.clean_num,
                                     'dolongitude': row_ops.clean_num,
                                     'dolatitude': row_ops.clean_num,
                                     'pulongitude': row_ops.clean_num,
                                     'pulatitude': row_ops.clean_num
                                 },
                                 encoding='utf-8'
                                 )

                to_parquet(df=df, out_bucket=out_bucket, folder=year+'/normal/', compute=True)

    except Exception as err:
        raise err

    else:
        return status


def perform_transit_dask(task_type: str, years: List[str]) -> bool:

    task_type_map: Dict = task_map.task_type_map[task_type]
    in_bucket: str = task_type_map['in']
    out_bucket: str = task_type_map['out']

    status: bool = False
    try:
        client: Client = dask.create_dask_client(num_workers=8)
        s3_options: Dict = ps.fetch_s3_options()
        month_st: int = 1
        month_end: int = 13
        calendar: cal.Calendar = cal.Calendar()
        for year in years:
            usecols = [3, 6, 7, 9, 10]
            names = ['station', 'date', 'time', 'entries', 'exits']
            url_part1: str = 's3://'+in_bucket+'/turnstile_'
            url_part2: str = ".txt"
            # urls for all saturdays in month range for year
            urls: List[str] = [url_part1 + year[2:] + prefix_zero(month) + prefix_zero(day_tuple[0]) + url_part2
                               for month in range(month_st, month_end)
                               for day_tuple in calendar.itermonthdays2(int(year), month)
                               if day_tuple[0] in range(1, 32) and day_tuple[1] == 5]

            #for url in urls:
            #    print(url)
            df = dd.read_csv(urlpath=urls,
                             storage_options=s3_options,
                             header=None,
                             usecols=usecols,
                             names=names,
                             parse_dates={'datetime': ['date', 'time']},
                             date_parser=row_ops.clean_transit_date,
                             skipinitialspace=True,
                             skip_blank_lines=True,
                             converters={
                                 'entries': row_ops.clean_num,
                                 'exits': row_ops.clean_num
                             },
                             encoding='utf-8'
                             )

            to_parquet(df=df, out_bucket=out_bucket, folder=year + '/', compute=True)

    except Exception as err:
        raise err

    else:
        return status


def perform_tsfare_dask(task_type: str, years: List[str]) -> bool:

    task_type_map: Dict = task_map.task_type_map[task_type]
    in_bucket: str = task_type_map['in']
    out_bucket: str = task_type_map['out']

    status: bool = False
    try:
        client: Client = dask.create_dask_client(num_workers=8)
        s3_options: Dict = ps.fetch_s3_options()
        month_st: int = 1
        month_end: int = 13
        calendar: cal.Calendar = cal.Calendar()
        for year in years:
            usecols =['date','station','FF','SEN/DIS', '7-D AFAS UNL','30-D AFAS/RMF UNL','JOINT RR TKT',
            '7-D UNL','30-D UNL','14-D RFM UNL','1-D UNL','14-D UNL','7D-XBUS PASS','TCMC',
            'RF 2 TRIP','RR UNL NO TRADE','TCMC ANNUAL MC','MR EZPAY EXP','MR EZPAY UNL',
            'PATH 2-T','AIRTRAIN FF','AIRTRAIN 30-D','AIRTRAIN 10-T','AIRTRAIN MTHLY',
            'STUDENTS']
            url_part1: str = 's3://'+in_bucket+'/fares_'
            url_part2: str = ".csv"
            # urls for all saturdays in month range for year
            urls: List[str] = [url_part1 + year[2:] + prefix_zero(month) + prefix_zero(day_tuple[0]) + url_part2
                               for month in range(month_st, month_end)
                               for day_tuple in calendar.itermonthdays2(int(year), month)
                               if day_tuple[0] in range(1, 32) and day_tuple[1] == 5]

            #for url in urls:
            #    print(url)
            df = dd.read_csv(urlpath=urls,
                             header=0,
                             usecols=usecols,
                             skipinitialspace=True,
                             skip_blank_lines=True,
                             parse_dates=['date'],
                             date_parser=row_ops.clean_tsfare_date,
                             converters={
                                 'FF': row_ops.clean_num,
                                 'SEN/DIS': row_ops.clean_num,
                                 '7-D AFAS UNL': row_ops.clean_num,
                                 '30-D AFAS/RMF UNL': row_ops.clean_num,
                                 'JOINT RR TKT': row_ops.clean_num,
                                 '7-D UNL': row_ops.clean_num,
                                 '30-D UNL': row_ops.clean_num,
                                 '14-D RFM UNL': row_ops.clean_num,
                                 '1-D UNL': row_ops.clean_num,
                                 '14-D UNL': row_ops.clean_num,
                                 '7D-XBUS PASS': row_ops.clean_num,
                                 'TCMC': row_ops.clean_num,
                                 'RF 2 TRIP': row_ops.clean_num,
                                 'RR UNL NO TRADE': row_ops.clean_num,
                                 'TCMC ANNUAL MC': row_ops.clean_num,
                                 'MR EZPAY EXP': row_ops.clean_num,
                                 'MR EZPAY UNL': row_ops.clean_num,
                                 'PATH 2-T': row_ops.clean_num,
                                 'AIRTRAIN FF': row_ops.clean_num,
                                 'AIRTRAIN 30-D': row_ops.clean_num,
                                 'AIRTRAIN 10-T': row_ops.clean_num,
                                 'AIRTRAIN MTHLY': row_ops.clean_num,
                                 'STUDENTS': row_ops.clean_num
                             },
                             encoding='utf-8'
                             )
            #to_parquet(df=df, out_bucket=out_bucket, folder=year + '/', compute=True)
            dd.to_csv(df=df,
                     filename='s3://'+out_bucket+'/'+year+'/',
                     #name_function=lambda i: out_file_prefix + '_' + str(i),
                     storage_options=s3_options)

    except Exception as err:
        raise err

    else:
        return status



def perform_traffic_dask(task_type: str, years: List[str]) -> bool:

    task_type_map: Dict = task_map.task_type_map[task_type]
    in_bucket: str = task_type_map['in']
    out_bucket: str = task_type_map['out']

    status: bool = False
    try:
        client: Client = dask.create_dask_client(num_workers=8)
        s3_options: Dict = ps.fetch_s3_options()
        month_st: int = 1
        month_end: int = 13
        calendar: cal.Calendar = cal.Calendar()
        for year in years:
            if year in ['2016', '2017']:
                month_st = 1
                month_end = 13
            elif year == '2015':
                month_st = 4
                month_end = 13
            elif year == '2018':
                month_st = 1
                month_end = 10
            usecols = [1, 2, 4, 5]
            names = ['speed', 'traveltime', 'datetime', 'linkid']
            url_part1: str = 's3://'+in_bucket+'/'
            url_part2: str = ".csv"
            # urls for all saturdays in month range for year
            urls: List[str] = [url_part1 + prefix_zero(month) + year + url_part2
                               for month in range(month_st, month_end)]

            #for url in urls:
            #    print(url)
            df = dd.read_csv(urlpath=urls,
                             storage_options=s3_options,
                             header=None,
                             usecols=usecols,
                             names=names,
                             parse_dates=['datetime'],
                             date_parser=row_ops.clean_traffic_date,
                             skipinitialspace=True,
                             skip_blank_lines=True,
                             converters={
                                 'speed': row_ops.clean_num,
                                 'traveltime': row_ops.clean_num,
                                 'linkid': row_ops.clean_num
                             },
                             encoding='utf-8'
                             )

            to_parquet(df=df, out_bucket=out_bucket, folder=year + '/', compute=True)

            #dd.to_csv(df=df,
            #          filename='s3://'+out_bucket+'/'+year+'/',
            #          #name_function=lambda i: out_file_prefix + '_' + str(i),
            #          storage_options=s3_options)

    except Exception as err:
        raise err

    else:
        return status


if __name__=='__main__':
    perform_transit_dask('dl-transit', ['2018','2017'])



