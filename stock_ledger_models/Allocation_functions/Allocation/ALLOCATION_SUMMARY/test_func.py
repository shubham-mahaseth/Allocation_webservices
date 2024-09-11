import mysql.connector
from get_connection import get_mysql_conn
from populate_srch_results import populate_search_results
from datetime import timedelta, date
import pandas as pd
import numpy as np

def to_call_fun(I_alloc_no):
    L_func_name = "to_call_fun"
    O_status =list()
    try:
        I_get_mysql_conn = list()
        I_get_mysql_conn.append(0)
        with get_mysql_conn (I_get_mysql_conn) as conn:
            L_func_call = populate_search_results (conn,I_alloc_no,O_status)
            print("refresh_search_results: ",L_func_call)
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        conn.rollback()
        return False

#to_call_fun()

#to_call_fun(1111)

I_search = {"I_alloc_no"          : 11111,
			"I_alloc_desc"        : None,
			"I_from_release_date" : None,
			"I_to_release_date"   : None,
			"I_from_create_date"  : None,
			"I_to_create_date"    : None,
			"I_create_user"       : None,
			"I_alloc_status"      : None,
			"I_PO"                : None,
			"I_Tst_no"            : None,
			"I_context"           : None,
			"I_promotion"         : None,
			"I_dept"              : None,
			"I_class"             : None,
			"I_subclass"          : None,
			"I_wh"                : None,
			"I_item_parent"       : None,
			"I_diff_id"           : None,
			"I_item_sku"          : None,
			"I_vpn"               : None,
			"I_alloc_type"        : None,
			"I_source"            : None,
			"I_asn"               : None,
			"I_pack_id"           : None,
			"I_batch_calc_ind"    : None}
to_call_fun(I_search)

