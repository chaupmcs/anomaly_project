import numpy as np
import pandas as pd
import time
from main_pool import anomaly_detection
from main_old___one_thread import anomaly_detection_old
def test():
    link_data_one_value_col = "C:\\Users\\CPU10902-local\\Desktop\\each_date.csv"
    link_data_a_lot_of_cols = "C:\\Users\\CPU10902-local\\Desktop\\op_msg.csv"
    link_medium_cols = "C:\\Users\\CPU10902-local\\Desktop\\data_test\\data_5_cols.csv"
    link_medium_2 = "C:\\Users\\CPU10902-local\\Desktop\\data_test\\data_3parts_comment.csv"

    link_hourly = "C:\\Users\\CPU10902-local\\Desktop\\data_test\\data_hourly_17h30_10th_Oct.csv"
    link_hourly_60dates = "C:\\Users\\CPU10902-local\\Desktop\\data_test\\data_hourly_2cols.csv"


    link_test = link_hourly_60dates

    # print("--- running...\n")
    # start_time = time.time()
    # table1 = anomaly_detection(link_test, period_=24)
    # time_mutil = time.time() - start_time
    # print("--- total run time: %s seconds ---" %time_mutil )
    #
    # print("--- writting to file...")
    # table1.to_csv("C:\\Users\\CPU10902-local\\Desktop\\output_hourly_24h_py.csv", sep=',', index=False)
    #
    # print("---done mutil function ! ")



    print("--- running\n")

    start_time2 = time.time()
    table2 = anomaly_detection_old(link_test, period=24)
    time_one = time.time() - start_time2
    print("--- total run time: %s seconds ---" % (time_one))

    print("--- writting to file...")

    table2.to_csv("C:\\Users\\CPU10902-local\\Desktop\\output_hourly_24h_py_1_process.csv", sep=',', index=False)
    print("---done one thread function ! ")

    print("---  asserting the results is the same")
    # assert pd.DataFrame.equals(table2, table1), "It's different !!!!! "
    # print("################# 2 files is the same.DONE !!!!!!!!! ")

if __name__ == '__main__':
    test()
