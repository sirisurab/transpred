import sys
from typing import List, Dict, Tuple
from data_tools import task_map
from utils import persistence as ps
from urllib3.response import HTTPResponse
from pandas import DataFrame, read_csv, concat
from bokeh.plotting import figure, output_file, show
from bokeh.models import Range1d, LinearAxis
from numpy import mean
#import matplotlib.pyplot as plt
#import seaborn as sns

RGTRANSIT_BUCKET: str = 'rg-transit'
RGGCABS_BUCKET: str = 'rg-gcabs'
RGYCABS_BUCKET: str = 'rg-ycabs'
RGTRAFFIC_BUCKET: str = 'rg-traffic'
REFBASE_BUCKET: str = 'ref-base'
GEOMERGED_PATH: str = 'geo-merged/'
PLOTS_BUCKET: str = 'plots'


def get_axis_range(df: DataFrame, col: str) -> Tuple:
    return df[col].min(), df[col].max()


def create_base_plot(station: str, base_df: DataFrame, datecol: str, varcol: str, varname: str, color: str ='red') -> figure:
    p = figure(title='plot for station ' + station,
                x_axis_label='datetime', x_axis_type='datetime',
                y_axis_label=varname)

    axis_range: Tuple = get_axis_range(df=base_df, col=varcol)
    p.y_range = Range1d(start=axis_range[0], end=axis_range[1])
    p.line(base_df[datecol], base_df[varcol],
            legend=varname, line_width=2, line_color=color)
    return p


def add_variable_to_plot(p: figure, var_df: DataFrame, datecol: str, varcol: str, varname: str, color: str='green') -> figure:
    axis_range = get_axis_range(df=var_df, col=varcol)
    p.extra_y_ranges = {'extra': Range1d(start=axis_range[0], end=axis_range[1])}
    p.add_layout(LinearAxis(y_range_name='extra', axis_label=varname), 'right')
    p.line(var_df[datecol], var_df[varcol],
            legend=varname, line_width=2, line_color=color, y_range_name='extra')
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

            if len(dolocationids) > 0:
                if gcabs_df.size > 0:
                    p1 = create_base_plot(station=station,
                                          base_df=transit_df,
                                          datecol=ts_datecols[0],
                                          varcol='delex',
                                          varname='transit exits',
                                          color='red')
                    p1 = add_variable_to_plot(p=p1,
                                              var_df=gcabs_df,
                                              datecol=cabs_datecols[0],
                                              varcol='passengers',
                                              varname='green cab passengers',
                                              color='green')
                    show(p1)

                    p2 = create_base_plot(station=station,
                                          base_df=transit_df,
                                          datecol=ts_datecols[0],
                                          varcol='delent',
                                          varname='transit entries',
                                          color='blue')
                    p2 = add_variable_to_plot(p=p2,
                                              var_df=gcabs_df,
                                              datecol=cabs_datecols[0],
                                              varcol='passengers',
                                              varname='green cab passengers',
                                              color='green')
                    show(p2)

                if ycabs_df.size > 0:
                    p3 = create_base_plot(station=station,
                                          base_df=transit_df,
                                          datecol=ts_datecols[0],
                                          varcol='delex',
                                          varname='transit exits',
                                          color='red')
                    p3 = add_variable_to_plot(p=p3,
                                              var_df=ycabs_df,
                                              datecol=cabs_datecols[0],
                                              varcol='passengers',
                                              varname='yellow cab passengers',
                                              color='orange')
                    show(p3)

                    p4 = create_base_plot(station=station,
                                          base_df=transit_df,
                                          datecol=ts_datecols[0],
                                          varcol='delent',
                                          varname='transit entries',
                                          color='blue')
                    p4 = add_variable_to_plot(p=p4,
                                              var_df=ycabs_df,
                                              datecol=cabs_datecols[0],
                                              varcol='passengers',
                                              varname='yellow cab passengers',
                                              color='orange')
                    show(p4)

            if len(linkids) > 0 and transit_df.size > 0:
                p5 = create_base_plot(station=station,
                                      base_df=transit_df,
                                      datecol=ts_datecols[0],
                                      varcol='delex',
                                      varname='transit exits',
                                      color='red')
                p5 = add_variable_to_plot(p=p5,
                                          var_df=traffic_df,
                                          datecol=traffic_datecols[0],
                                          varcol='speed',
                                          varname='traffic speed',
                                          color='magenta')
                show(p5)

                p6 = create_base_plot(station=station,
                                      base_df=transit_df,
                                      datecol=ts_datecols[0],
                                      varcol='delent',
                                      varname='transit entries',
                                      color='blue')
                p6 = add_variable_to_plot(p=p6,
                                          var_df=traffic_df,
                                          datecol=traffic_datecols[0],
                                          varcol='speed',
                                          varname='traffic speed',
                                          color='magenta')
                show(p6)

        except Exception as err:
            print('Error in plotting task %(task)s for station %(station)s'
                  % {'task': task, 'station': station})
            raise err

    # save plots in out bucket
    ps.copy_file(dest_bucket=PLOTS_BUCKET, file=plot_filepath+plot_filename, source=tmp_filepath)

    return True


if __name__ == '__main__':
    status: bool = plot(sys.argv[1:])
