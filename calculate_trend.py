# -*- coding: utf-8 -*-

import statsmodels.api as sm

import numpy as np
def calc_trend(data_col, period = 7): #data_col is a array
    res = sm.tsa.seasonal_decompose(data_col,  freq=period) # Moving Average 
    trend = res.trend # decompose trend into 'trend' variable

    seasonal = res
    #fill nan values in the MA result by the average or period neibough obs
    amount_nan = period//2

    # fill at the ending points
    for i in range((len(trend) - amount_nan), len(trend)):
        trend[i]= np.mean(trend[(i-amount_nan-period):(i-amount_nan)])

    # in the start as well:
    for i in range((period - amount_nan)):
        trend[i] = np.mean(trend[(i + amount_nan):(i + amount_nan + period)])

    return trend
    
