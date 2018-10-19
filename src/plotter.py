import sys
from typing import List, Dict, Tuple
from data_tools import task_map, row_operations
from utils import persistence as ps
from urllib3.response import HTTPResponse
from pandas import DataFrame, read_csv, concat
from bokeh.plotting import figure, output_file, show
from bokeh.models import Range1d, LinearAxis
from numpy import NaN, mean
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
import seaborn as sns

RGTRANSIT_BUCKET: str = 'rg-transit'
RGGCABS_BUCKET: str = 'rg-gcabs'
RGYCABS_BUCKET: str = 'rg-ycabs'
RGTRAFFIC_BUCKET: str = 'rg-traffic'
REFBASE_BUCKET: str = 'ref-base'
GEOMERGED_PATH: str = 'geo-merged/'
PLOTS_BUCKET: str = 'plots'


def get_axis_range(df: DataFrame, col: str) -> Tuple:
    return df[col].min(), df[col].max()


def create_plot(df1: DataFrame, varcol1: str, label1: str, df2: DataFrame, varcol2: str, label2: str, ax: plt.Axes.axis, weighted: bool=False, weight_col: str=None, datecol: str=None):
    sns.lineplot(data=df1[varcol1], ax=ax, color='blue', label=label1, legend='brief')
    ax1 = ax.twinx()
    if weighted:
        for name, group in df2.reset_index().groupby(weight_col):
            weight = float(name)
            df = group.set_index(datecol)
            size = 1 / weight
            sns.lineplot(data=df[varcol2], ax=ax1, color='coral',
                         ci=None, linewidth=size)
    else:
        sns.lineplot(data=df2[varcol2], ax=ax1, color='coral', label=label2, legend='brief')

    #ax.title(station+' '+start_date+' to '+end_date)
    ax.set_title(label1 + ' vs ' + label2)
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=.0)
    return


def create_base_plot_bokeh(station: str, base_df: DataFrame, datecol: str, varcol: str, varname: str, color: str ='red') -> figure:
    p = figure(title='plot for station ' + station,
                x_axis_label='datetime', x_axis_type='datetime',
                y_axis_label=varname)

    axis_range: Tuple = get_axis_range(df=base_df, col=varcol)
    p.y_range = Range1d(start=axis_range[0], end=axis_range[1])
    p.line(base_df[datecol], base_df[varcol],
            legend=varname, line_width=2, line_color=color)
    return p


def add_variable_to_plot_bokeh(p: figure, var_df: DataFrame, datecol: str, varcol: str, varname: str, color: str='green') -> figure:
    axis_range = get_axis_range(df=var_df, col=varcol)
    p.extra_y_ranges = {'extra': Range1d(start=axis_range[0], end=axis_range[1])}
    p.add_layout(LinearAxis(y_range_name='extra', axis_label=varname), 'right')
    p.line(var_df[datecol], var_df[varcol],
            legend=varname, line_width=2, line_color=color, y_range_name='extra')
    return p


def plot(*args) -> bool:
    inputs: List[str] = list(*args)
    task: str = inputs[0]
    print('plotting task %(task)s for stations %(stations)s'
          % {'task': task, 'stations': inputs[1:]})
    # read in and out buckets, freq and range for task from task_map
    freq: str = task_map.task_type_map[task]['freq']
    range: List[str] = task_map.task_type_map[task]['range']
    start_date: str = range[0]
    end_date: str = range[1]
    geomerged_cabs: str = GEOMERGED_PATH+'/cabs.csv'
    geomerged_traffic: str = GEOMERGED_PATH+'/traffic.csv'
    gas_file: str ='gas.csv'
    weather_file: str ='weather.csv'

    # load ref-base geomerged files
    filestream: HTTPResponse = ps.get_file_stream(bucket=REFBASE_BUCKET, filename=geomerged_cabs)
    dtypes: Dict[str, str] = {
        'stop_name': 'object',
        'tsstation': 'object',
        'locationid': 'int64',
        'weight': 'float64'
    }
    geomerged_cabs_df: DataFrame = read_csv(filestream, usecols=dtypes.keys(), encoding='utf-8', dtype=dtypes)
    geomerged_cabs_df = geomerged_cabs_df[~geomerged_cabs_df['locationid'].isna()]
    filestream = ps.get_file_stream(bucket=REFBASE_BUCKET, filename=geomerged_traffic)
    dtypes = {
        'stop_name': 'object',
        'tsstation': 'object',
        'linkid': 'float64',
        'weight': 'float64'
    }
    geomerged_traffic_df: DataFrame = read_csv(filestream, usecols=dtypes.keys(), encoding='utf-8', dtype=dtypes)
    geomerged_traffic_df = geomerged_traffic_df[~geomerged_traffic_df['linkid'].isna()]

    filestream = ps.get_file_stream(bucket=REFBASE_BUCKET, filename=gas_file)
    dtypes = {
        'price': 'float64'
    }
    gas_datecols = ['date']
    gas_df: DataFrame = read_csv(filestream, usecols=list(dtypes.keys())+gas_datecols, parse_dates=gas_datecols, encoding='utf-8', dtype=dtypes)
    gas_df = gas_df.set_index(gas_datecols).loc[start_date: end_date]
    print(gas_df.head())

    filestream = ps.get_file_stream(bucket=REFBASE_BUCKET, filename=weather_file)
    dtypes = {
            'prcp': 'float64',
            'snow': 'float64',
            'temp': 'float64'
    }
    weather_datecols = ['date']
    weather_df: DataFrame = read_csv(filestream, usecols=list(dtypes.keys())+weather_datecols, parse_dates=weather_datecols, encoding='utf-8', dtype=dtypes)
    weather_df = weather_df.set_index(weather_datecols).loc[start_date: end_date]
    print(weather_df.head())

    # for plotting
    plot_filepath: str = task + '/'
    tmp_filepath: str = '/tmp/'
    #output_file(tmp_filepath)
    sns.set()
    sns.set_style('dark')

    for station in inputs[1:]:
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
            transit_df = read_csv(filestream, usecols=ts_datecols + list(dtypes.keys()),
                                             parse_dates=ts_datecols,
                                             date_parser=row_operations.parse_rg_dt,
                                             encoding='utf-8', dtype=dtypes)
            transit_df = transit_df.set_index('datetime').loc[start_date: end_date]
            print(transit_df.head())

            # read data from other in buckets
            gcabs_df: DataFrame
            ycabs_df: DataFrame
            cabs_datecols = ['dodatetime']
            traffic_df: DataFrame
            traffic_datecols = ['datetime']

            # determine relevant cabs files
            # by finding dolocationids corresponding
            # to current station from ref-base geomerged df
            dolocationids = geomerged_cabs_df.loc[geomerged_cabs_df.tsstation == station][['locationid', 'weight']]

            if dolocationids.size > 0:
                cabs_dtypes = {
                    'dolocationid': 'int64',
                    'passengers': 'int64',
                    'distance': 'float64'
                }
                cabs_cols = list(cabs_dtypes.keys())
                gcabs_df = concat([read_csv(ps.get_file_stream(bucket=RGGCABS_BUCKET, filename=str(locationid)),
                                            header=0,
                                            usecols=cabs_datecols + list(cabs_dtypes.keys()),
                                            parse_dates=cabs_datecols,
                                            encoding='utf-8', dtype=cabs_dtypes)
                                   for locationid in dolocationids['locationid']
                                   if str(locationid) in ps.get_all_filenames(bucket=RGGCABS_BUCKET, path='/')],
                                   ignore_index=True)
                gcabs_df = gcabs_df.merge(dolocationids, left_on='dolocationid', right_on='locationid', how='left', copy=False).\
                    drop(columns=['dolocationid', 'locationid']).drop_duplicates()
                ycabs_df = concat([read_csv(ps.get_file_stream(bucket=RGYCABS_BUCKET, filename=str(locationid)),
                                            header=0,
                                            usecols=cabs_datecols + cabs_cols,
                                            parse_dates=cabs_datecols,
                                            encoding='utf-8', dtype=cabs_dtypes)
                                   for locationid in dolocationids['locationid']
                                   if str(locationid) in ps.get_all_filenames(bucket=RGYCABS_BUCKET, path='/')],
                                  ignore_index=True)
                ycabs_df = ycabs_df.merge(dolocationids, left_on='dolocationid', right_on='locationid', how='left', copy=False).\
                    drop(columns=['dolocationid', 'locationid']).drop_duplicates()
                gcabs_df = gcabs_df.set_index(cabs_datecols).loc[start_date: end_date]
                ycabs_df = ycabs_df.set_index(cabs_datecols).loc[start_date: end_date]
                print(gcabs_df.head())
                print(ycabs_df.head())

            # determine relevant traffic files
            # by finding linkids corresponding
            # to current station from ref-base geomerged traffic df
            linkids = geomerged_traffic_df.loc[geomerged_traffic_df.tsstation == station][['linkid', 'weight']]

            if linkids.size > 0:
                traffic_dtypes = {
                    'linkid': 'int64',
                    'speed': 'float64',
                    'traveltime': 'float64'
                }
                traffic_cols = list(traffic_dtypes.keys())
                traffic_df = concat([read_csv(ps.get_file_stream(bucket=RGTRAFFIC_BUCKET, filename=str(int(linkid))),
                                              header=0,
                                              usecols=traffic_datecols + traffic_cols,
                                              parse_dates=traffic_datecols,
                                              encoding='utf-8', dtype=traffic_dtypes)
                                    for linkid in linkids['linkid']
                                   if str(int(linkid)) in ps.get_all_filenames(bucket=RGTRAFFIC_BUCKET, path='/')],
                                  ignore_index=True)
                traffic_df = traffic_df.merge(linkids, on='linkid', how='left', copy=False).drop(columns=['linkid']).drop_duplicates()
                traffic_df = traffic_df.set_index(traffic_datecols).loc[start_date: end_date]
                print(traffic_df.head())

            # create plots
            plt.close('all')
            fig, axes = plt.subplots(nrows=5, ncols=2, clear=True, figsize=(18, 15))

            if dolocationids.size > 0:
                if gcabs_df.size > 0:
                    varcol1 = 'delex'
                    var1 = 'transit '
                    var2 = 'gcabs '
                    varcol2 = 'passengers'
                    create_plot(df1=transit_df,
                                varcol1=varcol1,
                                label1=var1+varcol1,
                                df2=gcabs_df,
                                varcol2=varcol2,
                                label2=var2+varcol2,
                                ax=axes[0, 0],
                                weighted=True,
                                weight_col='weight',
                                datecol=cabs_datecols[0])

                    varcol1 = 'delent'
                    create_plot(df1=transit_df,
                                varcol1=varcol1,
                                label1=var1+varcol1,
                                df2=gcabs_df,
                                varcol2=varcol2,
                                label2=var2+varcol2,
                                ax=axes[0, 1],
                                weighted=True,
                                weight_col='weight',
                                datecol=cabs_datecols[0])

                if ycabs_df.size > 0:
                    varcol1 = 'delex'
                    var1 = 'transit '
                    var2 = 'ycabs '
                    varcol2 = 'passengers'
                    create_plot(df1=transit_df,
                                varcol1=varcol1,
                                label1=var1+varcol1,
                                df2=ycabs_df,
                                varcol2=varcol2,
                                label2=var2+varcol2,
                                ax=axes[1, 0],
                                weighted=True,
                                weight_col='weight',
                                datecol=cabs_datecols[0])

                    varcol1 = 'delent'
                    create_plot(df1=transit_df,
                                varcol1=varcol1,
                                label1=var1+varcol1,
                                df2=ycabs_df,
                                varcol2=varcol2,
                                label2=var2+varcol2,
                                ax=axes[1, 1],
                                weighted=True,
                                weight_col='weight',
                                datecol=cabs_datecols[0])

            if linkids.size > 0 and transit_df.size > 0:
                varcol1 = 'delex'
                var1 = 'transit '
                var2 = 'traffic '
                varcol2 = 'speed'
                create_plot(df1=transit_df,
                            varcol1=varcol1,
                            label1=var1+varcol1,
                            df2=traffic_df,
                            varcol2=varcol2,
                            label2=var2+varcol2,
                            ax=axes[2, 0],
                            weighted=True,
                            weight_col='weight',
                            datecol=traffic_datecols[0])

                varcol1 = 'delent'
                create_plot(df1=transit_df,
                            varcol1=varcol1,
                            label1=var1+varcol1,
                            df2=traffic_df,
                            varcol2=varcol2,
                            label2=var2+varcol2,
                            ax=axes[2, 1],
                            weighted=True,
                            weight_col='weight',
                            datecol=traffic_datecols[0])

            # gas
            varcol1 = 'delex'
            var1 = 'transit '
            var2 = 'gas '
            varcol2 = 'price'
            create_plot(df1=transit_df,
                        varcol1=varcol1,
                        label1=var1 + varcol1,
                        df2=gas_df,
                        varcol2=varcol2,
                        label2=var2 + varcol2,
                        ax=axes[3, 0])

            varcol1 = 'delent'
            create_plot(df1=transit_df,
                        varcol1=varcol1,
                        label1=var1 + varcol1,
                        df2=gas_df,
                        varcol2=varcol2,
                        label2=var2 + varcol2,
                        ax=axes[3, 1])

            # weather
            varcol1 = 'delex'
            var1 = 'transit '
            var2 = 'weather '
            varcol2 = 'temp'
            create_plot(df1=transit_df,
                        varcol1=varcol1,
                        label1=var1 + varcol1,
                        df2=weather_df,
                        varcol2=varcol2,
                        label2=var2 + varcol2,
                        ax=axes[4, 0])

            varcol1 = 'delent'
            create_plot(df1=transit_df,
                        varcol1=varcol1,
                        label1=var1 + varcol1,
                        df2=weather_df,
                        varcol2=varcol2,
                        label2=var2 + varcol2,
                        ax=axes[4, 1])

            plot_filename = station + '.png'
            outfile = tmp_filepath + plot_filename
            fig.tight_layout()
            fig.savefig(outfile)
            # save plots in out bucket
            ps.copy_file(dest_bucket=PLOTS_BUCKET, file=plot_filepath+plot_filename, source=outfile)

        except Exception as err:
            print('Error in plotting task %(task)s for station %(station)s'
                  % {'task': task, 'station': station})
            raise err



    return True


if __name__ == '__main__':
    status: bool = plot(sys.argv[1:])
