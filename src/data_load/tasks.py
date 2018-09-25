#!/usr/bin/env python
# prepare tasks for data loading(dl)
from typing import List
from functools import reduce, partial
import wget
from utils import persistence as ps
import os

#TODO
def make_transit(years: List[str]) -> List[str]:
    return []

#TODO
def make_traffic(years: List[str]) -> List[str]:
    return []

#TODO
def make_cabs(years: List[str]) -> List[str]:
    print('constructing tasks for years '+str(years))
    tasks_for_year = lambda tasks, year: tasks + [year+"-"+str(quarter) for quarter in range(1, 4)]
    return reduce(tasks_for_year, years, [])

#TODO
def perform_transit(task: str) -> bool:
    return False

#TODO
def perform_traffic(task: str) -> bool:
    return False


def perform_cabs(task: str) -> bool:
    task_split: List[str] = task.split('-')
    year: str = task_split[0]
    quarter: int = int(task_split[1])
    prefix_zero = lambda x: "0"+str(x) if x < 10 else str(x)
    months = lambda quarter: range( (quarter-1)*3+1, (quarter-1)*3+4 )
    get_url = lambda month: "https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_"+year+"-"+str(month)+".csv"
    urls: List[str] = list(map(get_url, months(quarter)))
    print('downloading from urls '+str(urls))
    source_folder: str = './gcabs'+task+'/'
    os.makedirs(source_folder, exist_ok=True)
    print('created source folder '+source_folder)
    try:
        download_from_urls(urls, source_folder)
    except Exception as err:
        raise err
    else:
        try:
            print('copying from '+source_folder+' to gcabs')
            ps.copy_files(source_folder,'gcabs')
        except Exception as err:
            raise err
        else:
            return True


def download_from_urls(urls: List[str], folder) -> bool:
    try:
        download=partial(wget.download, out=folder)
        map(download, urls)
    except Exception as err:
        raise err
    else:
        return True



