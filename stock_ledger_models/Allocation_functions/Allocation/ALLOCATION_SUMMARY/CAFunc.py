#import mysql.connector
#from get_connection import get_mysql_conn
from .copy_alloc import copy_alloc_data
#from module2 import load_change_weight_date
#from module2 import pop_ly_eow
#from datetime import timedelta, date

def to_call_ca_fun(conn,I_copy_alloc_no):
    L_func_name = "to_call_ca_fun"
    O_status =list()
    I_new_alloc_no = list()
    try:
        #I_get_mysql_conn = list()
        #I_get_mysql_conn.append(0)
        #with get_mysql_conn (I_get_mysql_conn) as conn:
        L_func_call = copy_alloc_data(conn,I_copy_alloc_no,I_new_alloc_no,O_status)
        return L_func_call
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        conn.rollback()
        return False

#to_call_ca_fun('12345678') 





