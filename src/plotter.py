import sys
from typing import List, Dict
from data_tools import task_map
from utils import persistence as ps
from urllib3.response import HTTPResponse
from pandas import DataFrame, read_csv, concat
from bokeh.plotting import figure, output_file, show
from numpy import mean

RGTRANSIT_BUCKET: str = 'rg-transit'
RGGCABS_BUCKET: str = 'rg-gcabs'
RGYCABS_BUCKET: str = 'rg-ycabs'
RGTRAFFIC_BUCKET: str = 'rg-traffic'
REFBASE_BUCKET: str = 'ref-base'
GEOMERGED_PATH: str = 'geo-merged/'
PLOTS_BUCKET: str = 'plots'

def plot(*args) -> bool:
    inputs: List[str] = list(*args)
    task: str = inputs[0]
    buffer: float = float(inputs[1])
    print('plotting task %(task)s and buffer-radius %(buffer)s for stations %(stations)s'
          % {'task': task, 'buffer': buffer, 'stations': inputs[2:]})
    # read in and out buckets, freq and range for task from task_map
    freq: str = task_map.task_type_map[task]['freq']
    range: List[str] = task_map.task_type_map[task]['range']
    geomerged_cabs: str = GEOMERGED_PATH+str(buffer)+'/cabs.csv'
    geomerged_traffic: str = GEOMERGED_PATH+str(buffer)+'/traffic.csv'

    # load ref-base geomerged files
    filestream: HTTPResponse = ps.get_file_stream(bucket=REFBASE_BUCKET, filename=geomerged_cabs)
    dtypes: Dict[str, str] = {
        'stop_name': 'object',
        'tsstation': 'object',
        'locationid': 'int64'
    }
    geomerged_cabs_df: DataFrame = read_csv(filestream, usecols=dtypes.keys(), encoding='utf-8', dtype=dtypes)
    filestream = ps.get_file_stream(bucket=REFBASE_BUCKET, filename=geomerged_traffic)
    dtypes = {
        'stop_name': 'object',
        'tsstation': 'object',
        'linkid': 'int64'
    }
    geomerged_traffic_df: DataFrame = read_csv(filestream, usecols=dtypes.keys(), encoding='utf-8', dtype=dtypes)

    # for plotting
    plot_filepath: str = task + '/' + str(buffer) + '/'
    plot_filename: str = 'EDA.html'
    tmp_filepath: str = '/tmp/' + plot_filename
    output_file(tmp_filepath)

    for station in inputs[2:]:
        try:
            # determine filename of transit data for
            # the current station in the rg-transit bucket
            # replace '/' in station with ' '
            ts_filename: str = station.replace('/', ' ').upper()

            # read transit data for station (rg-transit bucket)
            filestream = ps.get_file_stream(bucket=RGTRANSIT_BUCKET, filename=ts_filename)
            ts_datecols = ['datetime']
            dtypes = {
                     'delex': 'int64',
                     'delent': 'int64'
                    }
            transit_df: DataFrame = read_csv(filestream, usecols=ts_datecols + list(dtypes.keys()),
                                             parse_dates=ts_datecols,
                                             encoding='utf-8', dtype=dtypes)

            #transit_df = transit_df.set_index('datetime').sort_index().reset_index()

            # read data from other in buckets
            gcabs_df: DataFrame
            ycabs_df: DataFrame
            cabs_datecols = ['dodatetime']
            traffic_df: DataFrame
            traffic_datecols = ['datetime']

            # determine relevant cabs files
            # by finding dolocationids corresponding
            # to current station from ref-base geomerged df
            dolocationids = geomerged_cabs_df.loc[geomerged_cabs_df.tsstation == station]['dolocationid']

            cabs_dtypes = {
                'passengers': 'int64',
                'distance': 'float64'
            }
            gcabs_df = concat([read_csv(ps.get_file_stream(bucket=RGGCABS_BUCKET, filename=str(locationid)),
                                                   usecols=cabs_datecols + list(cabs_dtypes.keys()),
                                                   parse_dates=cabs_datecols,
                                                   encoding='utf-8', dtype=cabs_dtypes)
                                          for locationid in dolocationids],
                                         ignore_index=True)

            gcabs_df = gcabs_df.groupby(cabs_datecols).apply(sum).\
                sort_index().reset_index()

            ycabs_df = concat([read_csv(ps.get_file_stream(bucket=RGYCABS_BUCKET, filename=str(locationid)),
                                        usecols=cabs_datecols + list(cabs_dtypes.keys()),
                                        parse_dates=cabs_datecols,
                                        encoding='utf-8', dtype=cabs_dtypes)
                               for locationid in dolocationids],
                              ignore_index=True)

            ycabs_df = ycabs_df.groupby(cabs_datecols).apply(sum). \
                sort_index().reset_index()

            # determine relevant traffic files
            # by finding linkids corresponding
            # to current station from ref-base geomerged traffic df
            linkids = geomerged_traffic_df.loc[geomerged_traffic_df.tsstation == station]['linkid']

            traffic_dtypes = {
                'speed': 'float64',
                'traveltime': 'float64'
            }
            traffic_df = concat([read_csv(ps.get_file_stream(bucket=RGTRAFFIC_BUCKET, filename=str(linkid)),
                                        usecols=traffic_datecols + list(traffic_dtypes.keys()),
                                        parse_dates=traffic_datecols,
                                        encoding='utf-8', dtype=traffic_dtypes)
                               for linkid in linkids],
                              ignore_index=True)

            traffic_df = traffic_df.groupby(traffic_datecols).apply(mean). \
                sort_index().reset_index()

            # create plots

            p = figure(title='plot for station '+station,
                       x_axis_label='datetime', x_axis_type='datetime',
                       y_axis_label='')

            p.line(transit_df[ts_datecols[0]], transit_df['delex'],
                   legend='transit exits', line_width=2, line_color='blue')
            p.line(transit_df[ts_datecols[0]], transit_df['delent'],
                   legend='transit entries', line_width=2, line_color='red')
            p.line(gcabs_df[cabs_datecols[0]], gcabs_df['passengers'],
                   legend='green cab passengers', line_width=2, line_color='green')
            p.line(gcabs_df[cabs_datecols[0]], gcabs_df['distance'],
                   legend='green cab trip length', line_width=1, line_color='green')
            p.line(ycabs_df[cabs_datecols[0]], ycabs_df['passengers'],
                   legend='yellow cab passengers', line_width=2, line_color='yellow')
            p.line(ycabs_df[cabs_datecols[0]], ycabs_df['distance'],
                   legend='yellow cab trip length', line_width=1, line_color='yellow')
            p.line(traffic_df[traffic_datecols[0]], traffic_df['speed'],
                   legend='traffic speed', line_width=2, line_color='magenta')
            p.line(traffic_df[traffic_datecols[0]], traffic_df['traveltime'],
                   legend='traffic travel time', line_width=1, line_color='magenta')

            show(p)

        except Exception as err:
            print('Error in plotting task %(task)s for station %(station)s'
                  % {'task': task, 'station': station})
            raise err

    # save plots in out bucket
    ps.copy_file(dest_bucket=PLOTS_BUCKET, file=plot_filepath+plot_filename, source=tmp_filepath)

    return True


if __name__ == '__main__':
    status: bool = plot(sys.argv[1:])
