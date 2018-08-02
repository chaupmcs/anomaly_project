# -*- coding: utf-8 -*-
"""
Created on Wed Sep 21 10:10:32 2016
Edited on Thu Sep 23 16:42:22 2016

@author: CPU10902-local
"""

# use package season to calculate the remainder, then we get IQR to detect outliers
from seasonal import fit_seasons, adjust_seasons
import numpy as np


# recive a list
def ano_points(data_col, period=7, trend_type="median"):

    remainder = []
    indice = []
    NUM_DAYS =period * 30

    # detrend and deseasonalize
    print "len(data_col) = ", len(data_col)
    for i in range((period*30), len(data_col)):
        if (i>=NUM_DAYS):
            d=data_col[(i-NUM_DAYS+1):(i + 1)]
        else:
            d = data_col[:(i + 1)]
        # d = data_col[:(i + 1)]
        seasons, trend = fit_seasons(d, trend=trend_type, period=period)

        if (seasons is None):
            #print "none at ", i
            seasons = [0L] *period





        adjusted = adjust_seasons(d, seasons=seasons)
        # print "seasons", seasons
        # print "adjusted", adjusted
        # print "trend", trend
        residual = adjusted - trend
        # flag=True
        # previous_season = seasons
        # season_.append(np.mean(seasons.tolist()))

        remainder.append(residual[(len(residual)-1)])
        indice.append(i)
        print "i =", i

    remainder = [round(elem, 1) for elem in remainder]

    q75, q25 = np.percentile(remainder, [75, 25])
    IQR = q75 - q25

    low_threshold = q25 - IQR * 1.5
    high_threshold = q75 + IQR * 1.5

    outliers = [0L] * len(data_col)

    for i in range(len(remainder)):
        if (remainder[i] > high_threshold or remainder[i] < low_threshold):
            outliers[indice[i]] = 1L
            #print indice[i]


    # season_ = list(np.array(season_).flat)
    # print season_
    # print len(season_)
    return outliers

# test
# a=ano_points(d2)

