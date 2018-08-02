# -*- coding: utf-8 -*-
import time
import numpy as np
import pandas as pd
from ano_points import ano_points
from ano_trend import trend_outliers_detect, trend_calculate
from calculate_trend import calc_trend
import multiprocessing
from multiprocessing import Process
import logging


class myProcess(Process):
    def __init__(self, args ):
        self.ID, self.data_process, self.period, self.trend_type, self.num_points, self.length = args
        super(myProcess, self).__init__()

        self.q = multiprocessing.Queue()

    def run(self):

        print "start process {0} ".format(self.ID)


        ano_point = [None] * (self.length)
        trend = [None] * self.length
        ano_trend = [None] * self.length
        ano_point_names = [None] * self.length
        trend_names = [None] *self.length
        ano_trend_names = [None] * self.length

        for i in range(0, self.length):


            print "..........start process {0}, at i={1} ".format(self.ID, i)

            # take one column (timeseries col)
            data_col = self.data_process[self.data_process.columns[i]]

            # convert serie type to array
            data_col = np.asarray(data_col)

            # 1. ano_point
            ano_point[i] = ano_points(data_col.tolist(),  self.period,  self.trend_type)

            # 2. trend
            ## calc_trend
            trend[i] = calc_trend(data_col,  self.period)

            # 3. ano_trend
            ### calculate adjusted trend (It means, we cal the trend from start
            # point to the current point)
            adjusted_trend = trend_calculate( self.period, data_col)

            ### detect outliers trend
            ano_trend[i] = trend_outliers_detect(adjusted_trend,  self.num_points,  self.period)

            ano_point_names[i] = (self.data_process.columns[i] + '_anopoint')
            trend_names[i] = self.data_process.columns[i] + '_trend'
            ano_trend_names[i] = self.data_process.columns[i] + '_anotrend'
        print 'before: self.q.full()', self.q.full()
        self.q.put((ano_point, trend, ano_trend, ano_point_names, trend_names, ano_trend_names))
        print 'after: self.q.full()', self.q.full()

        print "exist process {0} ".format(self.ID)


def anomaly_detection(link_data, period_ = 7, num_points_ = 4, trend_type_ = "median"):
    #help function: detect outliers when the trend is given.
    multiprocessing.log_to_stderr(logging.INFO)
    NUMBER_PROCESSES = 3
    #read the data
    data = pd.read_csv(link_data, na_values='NA')

    #find the number of cols
    ncols = len(data.columns)

    ncols_each_process = ncols // NUMBER_PROCESSES

    #init the result table
    result_table = data

    # ano_point = [None] * (ncols-1)
    # trend = [None] * (ncols-1)
    # ano_trend = [None] * (ncols-1)
    #
    # ano_point_names = [None] * (ncols-1)
    # trend_names = [None] * (ncols-1)
    # ano_trend_names = [None] * (ncols-1)



    data_process_1 = data.ix[:,1: (1 + ncols_each_process)]
    data_process_2 = data.ix[:,(1 + ncols_each_process): (1 + 2*ncols_each_process)]
    data_process_3 = data.ix[:, (1 + 2*ncols_each_process): ncols]


    args = [(1, data_process_1, period_, trend_type_, num_points_, ncols_each_process),
            (2, data_process_2, period_, trend_type_, num_points_, (ncols_each_process)),
            (3, data_process_3, period_, trend_type_, num_points_, (ncols - (1 + 2 * ncols_each_process)))]

    jobs = []
    for i in range(3):
        p = myProcess(args[i])
        jobs.append(p)
        p.start()

    (ano_point1, trend1, ano_trend1, ano_point_names1, trend_names1, ano_trend_names1) = jobs[0].q.get()
    (ano_point2, trend2, ano_trend2, ano_point_names2, trend_names2, ano_trend_names2) =jobs[1].q.get()
    (ano_point3, trend3, ano_trend3, ano_point_names3, trend_names3, ano_trend_names3) = jobs[2].q.get()


    for j in jobs:
        j.join()

    print "done getting"

    print "starting to merge the table"
    ano_point = ano_point1 + ano_point2 + ano_point3
    trend = trend1+trend2+trend3
    ano_trend = ano_trend1 + ano_trend2 + ano_trend3
    ano_point_names = ano_point_names1 +ano_point_names2+ano_point_names3
    ano_trend_names = ano_trend_names1 + ano_trend_names2 + ano_trend_names3
    trend_names = trend_names1 + trend_names2 + trend_names3

    #append to the result table
    ano_point = pd.DataFrame(ano_point).transpose()
    ano_point.columns = ano_point_names

    ano_trend = pd.DataFrame(ano_trend).transpose()
    ano_trend.columns = ano_trend_names

    trend = pd.DataFrame(trend).transpose()
    trend.columns = trend_names

    result_table = pd.concat([result_table.reset_index(drop=True), ano_point, trend, ano_trend], axis=1)


    return result_table

##########################  pool
# pool = multiprocessing.Pool(2)
# arg_1 = (1, data_process_1, period_, trend_type_, num_points_, ncols_each_process)
# arg_2 = (2, data_process_2, period_, trend_type_, num_points_, (ncols-1-ncols_each_process))
#
# name__ = (1,2)
# data__ = (data_process_1, data_process_2)
# trend__ = (trend_type_, trend_type_)
# period__ = (period_, period_)
# num_point__ = (num_points_, num_points_)
# ncols_each_process__ = (ncols_each_process, (ncols-1-ncols_each_process))
#
# args_12 = zip(name__, data__, period__, trend__, num_point__, ncols_each_process__)
# result_pool = pool.map(run_pool, args_12)
# result_pool = [x for x in result_pool if x is not None]
# print result_pool
# print 'len result= ', len(result_pool)
