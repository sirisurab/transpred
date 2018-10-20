import sys
from typing import List, Dict, Tuple
from data_tools import task_map, row_operations
from utils import persistence as ps
from urllib3.response import HTTPResponse
from pandas import DataFrame, read_csv, concat, Grouper
import matplotlib.pyplot as plt
import seaborn as sns
from multiprocessing import Process, cpu_count
from error_handling import errors

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
        for name, group in df2.groupby(level=1):
            weight = float(name)
            #df = group.set_index(datecol)
            size = 1 / weight
            sns.lineplot(data=group[varcol2], ax=ax1, color='coral',
                         ci=None, linewidth=size)
    else:
        sns.lineplot(data=df2[varcol2], ax=ax1, color='coral', label=label2)

    #ax.title(station+' '+start_date+' to '+end_date)
    ax.set_title(label1 + ' vs ' + label2)
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=.0)
    return


def create_rel_plot(df: DataFrame, varcol1: str, label1: str, varcol2: str, label2: str, ax: plt.Axes.axis, weighted: bool=False, weight_col: str=None, datecol: str=None):
    #index_col = df.index.names
    if weighted:
        for name, group in df.groupby(level=1):
            weight = float(name)
            #df = group.set_index(datecol).resample('D')[varcol2].mean()
            #df = group.set_index(datecol)
            alpha = 1 / 2 * weight
            sns.relplot(x=varcol1, y=varcol2, data=df, ax=ax, color='coral',
                         ci=None, alpha=alpha)
    else:
        sns.relplot(x=varcol1, y=varcol2, data=df, ax=ax, color='coral', label=label2)

    #ax.title(station+' '+start_date+' to '+end_date)
    ax.set_title(label1 + ' vs ' + label2)
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=.0)
    return


def plot_for_station(task: str, station: str, sub_task: str, geomerged_cabs_df: DataFrame=None, geomerged_traffic_df: DataFrame=None, gas_df: DataFrame=None, weather_df: DataFrame=None):
    try:
        freq: str = task_map.task_type_map[task]['freq']
        range: List[str] = task_map.task_type_map[task]['range']
        start_date: str = range[0]
        end_date: str = range[1]
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
        transit_df = transit_df.set_index('datetime').resample('D').sum().loc[start_date: end_date]
        print(transit_df.head())

        # create plots
        plot_filepath: str = task + '/'
        tmp_filepath: str = '/tmp/'
        sns.set()
        sns.set_style('dark')
        plt.close('all')
        fig, axes = plt.subplots(nrows=10, ncols=2, clear=True, figsize=(25, 15))
        ts_col1 = 'delex'
        ts_col2 = 'delent'
        ts_label = 'transit '

        if sub_task in ['gcabs', 'ycabs']:
            # read data from other in buckets
            cabs_datecols = ['dodatetime']

            # determine relevant cabs files
            # by finding dolocationids corresponding
            # to current station from ref-base geomerged df
            dolocationids = geomerged_cabs_df.loc[geomerged_cabs_df.tsstation == station][['locationid', 'weight']]

            cabs_dtypes = {
                    'dolocationid': 'int64',
                    'passengers': 'int64',
                    'distance': 'float64'
                }

            if sub_task == 'gcabs':
                gcabs_df: DataFrame
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

                gcabs_df = gcabs_df.set_index(cabs_datecols, 'weight').groupby(Grouper(freq='D', level=0)).sum().loc[start_date: end_date]
                print(gcabs_df.head())

                # plots for cabs
                if dolocationids.size > 0 and gcabs_df.size > 0:
                    gcabs_label = 'gcabs '
                    gcabs_col = 'passengers'
                    create_plot(df1=transit_df,
                                varcol1=ts_col1,
                                label1=ts_label + 'exits',
                                df2=gcabs_df,
                                varcol2=gcabs_col,
                                label2=gcabs_label + gcabs_col,
                                ax=axes[0, 0],
                                weighted=True,
                                weight_col='weight',
                                datecol=cabs_datecols[0])

                    df = transit_df.join(gcabs_df, how='outer') \
                        [[ts_col1, ts_col2, gcabs_col, 'weight']]
                    create_rel_plot(df=df,
                                    varcol1=ts_col1,
                                    label1=ts_label + 'exits',
                                    varcol2=gcabs_col,
                                    label2=gcabs_label + gcabs_col,
                                    ax=axes[0, 1],
                                    weighted=True,
                                    weight_col='weight',
                                    datecol=ts_datecols[0])

                    create_plot(df1=transit_df,
                                varcol1=ts_col2,
                                label1=ts_label + 'entries',
                                df2=gcabs_df,
                                varcol2=gcabs_col,
                                label2=gcabs_label + gcabs_col,
                                ax=axes[1, 0],
                                weighted=True,
                                weight_col='weight',
                                datecol=cabs_datecols[0])
                    create_rel_plot(df=df,
                                    varcol1=ts_col2,
                                    label1=ts_label + 'entries',
                                    varcol2=gcabs_col,
                                    label2=gcabs_label + gcabs_col,
                                    ax=axes[1, 1],
                                    weighted=True,
                                    weight_col='weight',
                                    datecol=ts_datecols[0])

            elif sub_task == 'ycabs':
                ycabs_df: DataFrame
                ycabs_df = concat([read_csv(ps.get_file_stream(bucket=RGYCABS_BUCKET, filename=str(locationid)),
                                            header=0,
                                            usecols=cabs_datecols + list(cabs_dtypes.keys()),
                                            parse_dates=cabs_datecols,
                                            encoding='utf-8', dtype=cabs_dtypes)
                                   for locationid in dolocationids['locationid']
                                   if str(locationid) in ps.get_all_filenames(bucket=RGYCABS_BUCKET, path='/')],
                                  ignore_index=True)
                ycabs_df = ycabs_df.merge(dolocationids, left_on='dolocationid', right_on='locationid', how='left',
                                          copy=False). \
                    drop(columns=['dolocationid', 'locationid']).drop_duplicates()
                ycabs_df = ycabs_df.set_index(cabs_datecols, 'weight').groupby(Grouper(freq='D', level=0)).sum().loc[
                           start_date: end_date]

                print(ycabs_df.head())

                # plots for cabs
                if dolocationids.size > 0 and ycabs_df.size > 0:
                    ycabs_label = 'ycabs '
                    ycabs_col = 'passengers'
                    create_plot(df1=transit_df,
                                varcol1=ts_col1,
                                label1=ts_label + 'exits',
                                df2=ycabs_df,
                                varcol2=ycabs_col,
                                label2=ycabs_label + ycabs_col,
                                ax=axes[2, 0],
                                weighted=True,
                                weight_col='weight',
                                datecol=cabs_datecols[0])

                    df = transit_df.join(ycabs_df, how='outer') \
                        [[ts_col1, ts_col2, ycabs_col, 'weight']]
                    create_rel_plot(df=df,
                                    varcol1=ts_col1,
                                    label1=ts_label + 'exits',
                                    varcol2=ycabs_col,
                                    label2=ycabs_label + ycabs_col,
                                    ax=axes[2, 1],
                                    weighted=True,
                                    weight_col='weight',
                                    datecol=ts_datecols[0])

                    create_plot(df1=transit_df,
                                varcol1=ts_col2,
                                label1=ts_label + 'entries',
                                df2=ycabs_df,
                                varcol2=ycabs_col,
                                label2=ycabs_label + ycabs_col,
                                ax=axes[3, 0],
                                weighted=True,
                                weight_col='weight',
                                datecol=cabs_datecols[0])
                    create_rel_plot(df=df,
                                    varcol1=ts_col2,
                                    label1=ts_label + 'entries',
                                    varcol2=ycabs_col,
                                    label2=ycabs_label + ycabs_col,
                                    ax=axes[3, 1],
                                    weighted=True,
                                    weight_col='weight',
                                    datecol=ts_datecols[0])

        elif sub_task == 'traffic':
            # determine relevant traffic files
            # by finding linkids corresponding
            # to current station from ref-base geomerged traffic df
            traffic_df: DataFrame
            traffic_datecols = ['datetime']
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
                traffic_df = traffic_df.set_index(traffic_datecols, 'weight').groupby(Grouper(freq='D', level=0)).sum().loc[start_date: end_date]
                print(traffic_df.head())

            if linkids.size > 0 and transit_df.size > 0:
                tr_label = 'traffic '
                tr_col = 'speed'
                create_plot(df1=transit_df,
                            varcol1=ts_col1,
                            label1=ts_label+'exits',
                            df2=traffic_df,
                            varcol2=tr_col,
                            label2=tr_label+tr_col,
                            ax=axes[4, 0],
                            weighted=True,
                            weight_col='weight',
                            datecol=traffic_datecols[0])

                df = transit_df.join(traffic_df, how='outer') \
                    [[ts_col1, ts_col2, tr_col, 'weight']]
                create_rel_plot(df=df,
                            varcol1=ts_col1,
                            label1=ts_label+'exits',
                            varcol2=tr_col,
                            label2=tr_label+tr_col,
                            ax=axes[4, 1],
                            weighted=True,
                            weight_col='weight',
                            datecol=ts_datecols[0])

                create_plot(df1=transit_df,
                            varcol1=ts_col2,
                            label1=ts_label+'entries',
                            df2=traffic_df,
                            varcol2=tr_col,
                            label2=tr_label+tr_col,
                            ax=axes[5, 0],
                            weighted=True,
                            weight_col='weight',
                            datecol=traffic_datecols[0])
                create_rel_plot(df=df,
                            varcol1=ts_col2,
                            label1=ts_label+'entries',
                            varcol2=tr_col,
                            label2=tr_label+tr_col,
                            ax=axes[5, 1],
                            weighted=True,
                            weight_col='weight',
                            datecol=ts_datecols[0])

        elif sub_task == 'gas':

            # gas
            gas_label = 'gas '
            gas_col = 'price'
            create_plot(df1=transit_df,
                        varcol1=ts_col1,
                        label1=ts_label + 'exits',
                        df2=gas_df,
                        varcol2=gas_col,
                        label2=gas_label + gas_col,
                        ax=axes[6, 0])

            df = transit_df.join(gas_df, how='outer') \
                [[ts_col1, ts_col2, gas_col]]
            create_rel_plot(df=df,
                        varcol1=ts_col1,
                        label1=ts_label + 'exits',
                        varcol2=gas_col,
                        label2=gas_label + gas_col,
                        ax=axes[6, 1])

            create_plot(df1=transit_df,
                        varcol1=ts_col2,
                        label1=ts_label + 'entries',
                        df2=gas_df,
                        varcol2=gas_col,
                        label2=gas_label + gas_col,
                        ax=axes[7, 0])
            create_rel_plot(df=df,
                        varcol1=ts_col2,
                        label1=ts_label + 'entries',
                        varcol2=gas_col,
                        label2=gas_label + gas_col,
                        ax=axes[7, 1])

        elif sub_task == 'weather':
            # weather
            wr_label = 'weather '
            wr_col = 'temp'
            create_plot(df1=transit_df,
                        varcol1=ts_col1,
                        label1=ts_label + 'exits',
                        df2=weather_df,
                        varcol2=wr_col,
                        label2=wr_label + wr_col,
                        ax=axes[8, 0])

            df = transit_df.join(weather_df, how='outer') \
                [[ts_col1, ts_col2, wr_col]]
            create_rel_plot(df=df,
                        varcol1=ts_col1,
                        label1=ts_label + 'exits',
                        varcol2=wr_col,
                        label2=wr_label + wr_col,
                        ax=axes[8, 1])

            create_plot(df1=transit_df,
                        varcol1=ts_col2,
                        label1=ts_label + 'entries',
                        df2=weather_df,
                        varcol2=wr_col,
                        label2=wr_label + wr_col,
                        ax=axes[9, 0])
            create_rel_plot(df=df,
                        varcol1=ts_col2,
                        label1=ts_label + 'entries',
                        varcol2=wr_col,
                        label2=wr_label + wr_col,
                        ax=axes[9, 1])

        else:
            raise errors.TaskTypeError(sub_task)

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

    return


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

    # spawn plot process for each station
    processes = []
    print(cpu_count())
    init_plot_kwargs = lambda station : {'task': task,
                                       'station': station,
                                       'sub_task': None,
                                       'geomerged_cabs_df': None,
                                       'geomerged_traffic_df': None,
                                       'gas_df': None,
                                       'weather_df': None}

    for station in inputs[1:]:
        for sub_task in ['gcabs', 'ycabs', 'traffic', 'gas', 'weather']:
            plot_kwargs: Dict = init_plot_kwargs(station)
            plot_kwargs['sub_task'] = sub_task
            if sub_task in ['gcabs', 'ycabs']:
                plot_kwargs['geomerged_cabs_df'] = geomerged_cabs_df
            elif sub_task == 'traffic':
                plot_kwargs['geomerged_traffic_df'] = geomerged_traffic_df
            elif sub_task == 'gas':
                plot_kwargs['gas_df'] = gas_df
            elif sub_task == 'weather':
                plot_kwargs['weather_df'] = weather_df

            p = Process(target=plot_for_station, kwargs=plot_kwargs)
            p.start()
            print('started process %(pid)f for %(station)s %(sub_task)s' % {'pid': p.pid,
                                                                            'station': station,
                                                                            'sub_task': sub_task})
            processes.append(p)

    for p in processes:
        p.join()

    return True


if __name__ == '__main__':
    status: bool = plot(sys.argv[1:])
