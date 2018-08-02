import statsmodels.api as sm
import numpy as np

def trend_outliers_detect(trend, num_points, period):


        t = np.diff(trend) #calculate diff 1-lag
                
        res = [0L]* (num_points) #shift the result to the right a mount of num_points 
        for i in range((num_points-1),len(t)): #go through the data (the trend)
            if(max(t[(i-num_points+1):(i+1)]) < 0):
                res.append(1L) #anomaly point
            else:
                res.append(0L) #normal point

        return(res) #return a list
        
    #calculate the trend of the data
def trend_calculate(period, data):
    NumsDay = period * 30
      ### init 'trend_result' variable to store the trend
    trend_result = []

    # detrend and deseasonalize
    for i in range(period, len(data)):
        if (i > NumsDay):
            d_new = data[(i - NumsDay):(i + 1)]
        else:
            d_new = data[:(i+1)] # just take the data from start to current point

        res = sm.tsa.seasonal_decompose(d_new,  freq=period) # Moving Average
        trend = res.trend # decompose trend into 'trend' variable
        trend_result.append(trend[(len(trend)-1-(period//2))])


    # fill nan values in the MA result by the average or 7 neib obs
    amount_nan = period // 2
    trend_temp1 = []

    trend_temp2 = []


    # fill at the starting points


    # in the end as well:
    for i in range((len(trend_result) - amount_nan), len(trend_result)):
        trend_temp2.append(np.mean(trend_result[(i - amount_nan - period):(i - amount_nan)]))

    for i in range((period-amount_nan)):
        trend_temp1.append(np.mean(trend_result[(i + amount_nan):(i + amount_nan + period)]))

    trend_result = trend_temp1 + trend_result + trend_temp2

    trend_result_2 = [ round(elem, 1) for elem in trend_result ] # round the result

    return trend_result_2
        
