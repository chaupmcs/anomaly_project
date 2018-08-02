import numpy as np
import pandas as pd
from ano_points import ano_points
from ano_trend import trend_outliers_detect, trend_calculate
from calculate_trend import calc_trend
import multiprocessing
from multiprocessing import Process
import logging




def run_pool(args):
    ID, data_process, period, trend_type, num_points, length = args
    print "start process {0} ".format(ID)

    ano_point = [None] * (length)
    trend = [None] *length
    ano_trend = [None] * length
    ano_point_names = [None] * length
    trend_names = [None] * length
    ano_trend_names = [None] * length


    for i in range(0, length):
        print "..........start process {0}, at i={1} ".format(ID, i)

        # take one column (timeseries col)
        data_col = data_process[data_process.columns[i]]

        # convert serie type to array
        data_col = np.asarray(data_col)

        # 1. ano_point
        ano_point[i] = ano_points(data_col.tolist(), period, trend_type)

        # 2. trend
        ## calc_trend
        trend[i] = calc_trend(data_col, period)

        # 3. ano_trend
        ### calculate adjusted trend (It means, we cal the trend from start
        # point to the current point)
        adjusted_trend = trend_calculate(period, data_col)

        ### detect outliers trend
        ano_trend[i] = trend_outliers_detect(adjusted_trend,num_points, period)

        ano_point_names[i] = (data_process.columns[i] + '_anopoint')
        trend_names[i] = data_process.columns[i] + '_trend'
        ano_trend_names[i] = data_process.columns[i] + '_anotrend'

    q =[]
    q.append(ano_point)
    q.append(ano_trend)
    q.append(ano_point_names)
    q.append(trend_names)
    q.append(ano_trend_names)
    q.append(trend)
    if q==[[], [], [], [], [], []]:
        return None
    else:
        return q

def anomaly_detection(link_data, period_=7, num_points_=4, trend_type_="median"):
        # help function: detect outliers when the trend is given.
        #multiprocessing.log_to_stderr(logging.DEBUG)
        NUMBER_PROCESSES = 3
        # read the data
        data = pd.read_csv(link_data, na_values='NA')


        # find the number of cols
        ncols = len(data.columns)

        ncols_each_process = ncols // NUMBER_PROCESSES

        # init the result table
        result_table = data


        q_sum = multiprocessing.Queue()
        data_process_1 = data.ix[:, 1: (1 + ncols_each_process)]
        data_process_2 = data.ix[:, (1 + ncols_each_process): (1 + 2 * ncols_each_process)]
        data_process_3 = data.ix[:, (1 + 2 * ncols_each_process): ncols]


        ##########################  pool
        pool = multiprocessing.Pool(NUMBER_PROCESSES)

        name__ = (1, 2, 3)
        data__ = (data_process_1, data_process_2, data_process_3)
        trend__ = (trend_type_, trend_type_, trend_type_)
        period__ = (period_, period_, period_)
        num_point__ = (num_points_, num_points_, num_points_)
        ncols_each_process__ = (ncols_each_process, ncols_each_process, (ncols - (1 + 2 * ncols_each_process)))

        args_12 = zip(name__, data__, period__, trend__, num_point__, ncols_each_process__)
        print "Running..."
        result_pool = pool.map(run_pool, args_12)
        # result_pool = pool.map_async(run_pool, args_12)
        # print type(result_pool)
        result_pool = [x for x in result_pool if x is not None]

        print "joining..."
        ano_point = []
        trend = []
        ano_trend = []
        ano_point_names = []
        trend_names = []
        ano_trend_names = []

        n_loop = min(3, (ncols-1))
        for i in range(n_loop):
            ano_point = ano_point + result_pool[i][0]
            trend = trend +  result_pool[i][5]
            ano_trend = ano_trend + result_pool[i][1]
            ano_point_names = ano_point_names + result_pool[i][2]
            ano_trend_names = ano_trend_names+ result_pool[i][4]
            trend_names =  trend_names + result_pool[i][3]

        print "starting to get the result..."

        # append to the result table
        ano_point = pd.DataFrame(ano_point).transpose()
        ano_point.columns = ano_point_names

        ano_trend = pd.DataFrame(ano_trend).transpose()
        ano_trend.columns = ano_trend_names

        trend = pd.DataFrame(trend).transpose()
        trend.columns = trend_names

        result_table = pd.concat([result_table.reset_index(drop=True), ano_point, trend, ano_trend], axis=1)

        return result_table
