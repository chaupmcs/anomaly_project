# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from ano_points import ano_points
from ano_trend import trend_outliers_detect, trend_calculate
from calculate_trend import calc_trend


def anomaly_detection_old(link_data, period=7, num_points=4, trend_type="median"):
    # help function: detect outliers when the trend is given.

    # read the data
    data = pd.read_csv(link_data, na_values='NA')

    # find the number of cols
    ncols = len(data.columns)

    # init the result table
    result_table = data

    print ## calcualte_ano_points
    for i in range(1, ncols):
        # take one column (timeseries col)
        data_col = data[data.columns[i]]

        # convert serie type to list
        data_col = data_col.tolist()

        ano_point = ano_points(data_col, period, trend_type)

        # we bind it into the result_table
        result_table = pd.concat([result_table.reset_index(drop=True), pd.DataFrame(ano_point)], axis=1)

        result_table.rename(columns={0: (data.columns[i] + '_anopoint')}, inplace=True)

    print  ## calc_trend
    for i in range(1, ncols):
        # take one column (timeseries col)
        data_col = data[data.columns[i]]

        # convert serie type to array
        data_col = np.asarray(data_col)

        ## calc_trend
        trend = calc_trend(data_col, period)

        # we bind it into the result_table
        result_table = pd.concat([result_table.reset_index(drop=True), pd.DataFrame(trend)], axis=1)
        result_table.rename(columns={0: (data.columns[i] + '_trend')}, inplace=True)

    print   ### detect outliers trend
    for i in range(1, ncols):
        # take one column (timeseries col)
        data_col = data[data.columns[i]]

        # convert serie type to array
        data_col = np.asarray(data_col)

        ### calculate adjusted trend (It means, we cal the trend from start
        # point to the current point)
        adjusted_trend = trend_calculate(period, data_col)

        ### detect outliers trend
        ano_trend = trend_outliers_detect(adjusted_trend, num_points, period)

        # we bind it into the result_table
        result_table = pd.concat([result_table.reset_index(drop=True), pd.DataFrame(ano_trend)], axis=1)

        result_table.rename(columns={0: (data.columns[i] + '_anotrend')}, inplace=True)

    return result_table
