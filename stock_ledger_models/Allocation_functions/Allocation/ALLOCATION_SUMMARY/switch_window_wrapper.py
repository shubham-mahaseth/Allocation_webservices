
from .populate_srch_results import refresh_search_results
from datetime import timedelta, date
import pandas as pd
import numpy as np

def switch_window(conn):
    L_func_name = "switch_window"
    O_status =list()
    try:
        L_func_call, err_msg = refresh_search_results(conn,O_status)
        return L_func_call, err_msg

    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured in "+ str(argument)
        conn.rollback()
        return [],err_return
