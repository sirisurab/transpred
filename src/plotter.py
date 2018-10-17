import sys
from typing import List, Dict, Tuple
from data_tools import task_map
from utils import persistence as ps
from urllib3.response import HTTPResponse
from pandas import DataFrame, read_csv, concat
from bokeh.plotting import figure, output_file, show
from bokeh.models import Range1d, LinearAxis
from numpy import mean

RGTRANSIT_BUCKET: str = 'rg-transit'
RGGCABS_BUCKET: str = 'rg-gcabs'
RGYCABS_BUCKET: str = 'rg-ycabs'
RGTRAFFIC_BUCKET: str = 'rg-traffic'
REFBASE_BUCKET: str = 'ref-base'
GEOMERGED_PATH: str = 'geo-merged/'
PLOTS_BUCKET: str = 'plots'


def get_axis_range(df: DataFrame, cols: List[str]) -> Tuple:
    l_ext = [df[col].max() for col in cols]
    h_ext = [df[col].min() for col in cols]
    return min(l_ext), max(h_ext)


def create_transit_plot(station: str, transit_df: DataFrame, datecols: List[str]) -> figure:
    p = figure(title='plot for station ' + station,
                x_axis_label='datetime', x_axis_type='datetime',
                y_axis_label='')

    axis_range: Tuple = get_axis_range(df=transit_df, cols=['delent', 'delex'])
    p.y_range = Range1d(start=axis_range[0], end=axis_range[1])
    p.line(transit_df[datecols[0]], transit_df['delex'],
            legend='transit exits', line_width=2, line_color='blue')
    p.line(transit_df[datecols[0]], transit_df['delent'],
            legend='transit entries', line_width=2, line_color='red')
    return p


def plot(*args) -> bool:
    inputs: List[str] = list(*args)
    task: str = inputs[0]
    buffer: float = float(inputs[1])
    print('plotting task %(task)s and buffer-radius %(buffer)s for stations %(stations)s'
          % {'task': task, 'buffer': buffer, 'stations': inputs[2:]})
    # read in and out buckets, freq and range for task from task_map
    freq: str = task_map.task_type_map[task]['freq']
    range: List[str] = task_map.task_type_map[task]['range']
    start_date = range[0]
    end_date = range[1]
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
    geomerged_cabs_df = geomerged_cabs_df[~geomerged_cabs_df['locationid'].isna()]
    filestream = ps.get_file_stream(bucket=REFBASE_BUCKET, filename=geomerged_traffic)
    dtypes = {
        'stop_name': 'object',
        'tsstation': 'object',
        'linkid': 'float64'
    }
    geomerged_traffic_df: DataFrame = read_csv(filestream, usecols=dtypes.keys(), encoding='utf-8', dtype=dtypes)
    geomerged_traffic_df = geomerged_traffic_df[~geomerged_traffic_df['linkid'].isna()]

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
            transit_df = transit_df.set_index('datetime')[start_date: end_date].reset_index()

            # read data from other in buckets
            gcabs_df: DataFrame
            ycabs_df: DataFrame
            cabs_datecols = ['dodatetime']
            traffic_df: DataFrame
            traffic_datecols = ['datetime']

            # determine relevant cabs files
            # by finding dolocationids corresponding
            # to current station from ref-base geomerged df
            dolocationids = geomerged_cabs_df.loc[geomerged_cabs_df.tsstation == station]['locationid']

            if len(dolocationids) > 0:
                cabs_dtypes = {
                    'passengers': 'int64',
                    'distance': 'float64'
                }
                cabs_cols = list(cabs_dtypes.keys())
                gcabs_df = concat([read_csv(ps.get_file_stream(bucket=RGGCABS_BUCKET, filename=str(locationid)),
                                            header=0,
                                            usecols=cabs_datecols + list(cabs_dtypes.keys()),
                                            parse_dates=cabs_datecols,
                                            encoding='utf-8', dtype=cabs_dtypes)
                                   for locationid in dolocationids],
                                   ignore_index=True)
                print(gcabs_df.head())
                ycabs_df = concat([read_csv(ps.get_file_stream(bucket=RGYCABS_BUCKET, filename=str(locationid)),
                                            header=0,
                                            usecols=cabs_datecols + cabs_cols,
                                            parse_dates=cabs_datecols,
                                            encoding='utf-8', dtype=cabs_dtypes)
                                   for locationid in dolocationids],
                                  ignore_index=True)
                print(ycabs_df.head())

                gcabs_df = gcabs_df.set_index(cabs_datecols).sort_index().resample('1D')[cabs_cols].apply(sum)
                gcabs_df = gcabs_df.loc[start_date: end_date].reset_index()
                ycabs_df = ycabs_df.set_index(cabs_datecols).sort_index().resample('1D')[cabs_cols].apply(sum)
                ycabs_df = ycabs_df.loc[start_date: end_date].reset_index()

            # determine relevant traffic files
            # by finding linkids corresponding
            # to current station from ref-base geomerged traffic df
            linkids = geomerged_traffic_df.loc[geomerged_traffic_df.tsstation == station]['linkid']

            if len(linkids) > 0:
                traffic_dtypes = {
                    'speed': 'float64',
                    'traveltime': 'float64'
                }
                traffic_cols = list(traffic_dtypes.keys())
                traffic_df = concat([read_csv(ps.get_file_stream(bucket=RGTRAFFIC_BUCKET, filename=str(int(linkid))),
                                              header=0,
                                              usecols=traffic_datecols + traffic_cols,
                                              parse_dates=traffic_datecols,
                                              encoding='utf-8', dtype=traffic_dtypes)
                                    for linkid in linkids],
                                  ignore_index=True)

                print(traffic_df.head())
                traffic_df = traffic_df.set_index(traffic_datecols).sort_index().resample('1D')[traffic_cols].apply(mean)
                traffic_df = traffic_df.loc[start_date: end_date].reset_index()

            # create plots

            p1 = create_transit_plot(station=station, transit_df=transit_df, datecols=ts_datecols)
            if len(dolocationids) == 0 or gcabs_df.size == 0:
                show(p1)

            if len(dolocationids) > 0:
                if gcabs_df.size > 0:
                    axis_range = get_axis_range(df=gcabs_df, cols=['passengers', 'distance'])
                    p1.extra_y_ranges = {'gcabs': Range1d(start=axis_range[0], end=axis_range[1])}
                    p1.add_layout(LinearAxis(y_range_name='gcabs'), 'right')
                    p1.line(gcabs_df[cabs_datecols[0]], gcabs_df['passengers'],
                           legend='green cab passengers', line_width=2, line_color='green', y_range_name='gcabs')
                    p1.line(gcabs_df[cabs_datecols[0]], gcabs_df['distance'],
                           legend='green cab trip length', line_width=1, line_color='green', y_range_name='gcabs')
                    show(p1)

                if ycabs_df.size > 0:
                    p2 = create_transit_plot(station=station, transit_df=transit_df, datecols=ts_datecols)
                    axis_range = get_axis_range(df=ycabs_df, cols=['passengers', 'distance'])
                    p2.extra_y_ranges = {'ycabs': Range1d(start=axis_range[0], end=axis_range[1])}
                    p2.add_layout(LinearAxis(y_range_name='ycabs'), 'right')
                    p2.line(ycabs_df[cabs_datecols[0]], ycabs_df['passengers'],
                           legend='yellow cab passengers', line_width=2, line_color='yellow', y_range_name='ycabs')
                    p2.line(ycabs_df[cabs_datecols[0]], ycabs_df['distance'],
                           legend='yellow cab trip length', line_width=1, line_color='yellow', y_range_name='ycabs')
                    show(p2)

            if len(linkids) > 0 and transit_df.size > 0:
                p3 = create_transit_plot(station=station, transit_df=transit_df, datecols=ts_datecols)
                axis_range = get_axis_range(df=traffic_df, cols=['speed', 'traveltime'])
                p3.extra_y_ranges = {'traffic': Range1d(start=axis_range[0], end=axis_range[1])}
                p3.add_layout(LinearAxis(y_range_name='traffic'), 'right')
                p3.line(traffic_df[traffic_datecols[0]], traffic_df['speed'],
                       legend='traffic speed', line_width=2, line_color='magenta', y_range_name='traffic')
                p3.line(traffic_df[traffic_datecols[0]], traffic_df['traveltime'],
                       legend='traffic travel time', line_width=1, line_color='magenta', y_range_name='traffic')
                show(p3)

        except Exception as err:
            print('Error in plotting task %(task)s for station %(station)s'
                  % {'task': task, 'station': station})
            raise err

    # save plots in out bucket
    ps.copy_file(dest_bucket=PLOTS_BUCKET, file=plot_filepath+plot_filename, source=tmp_filepath)

    return True


if __name__ == '__main__':
    status: bool = plot(sys.argv[1:])
