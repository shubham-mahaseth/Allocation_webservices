import pandas as pd
#############################################################
# Created By - Priyanshu Pandey                             #
# File Name - get_vdate.py                                  #
# Purpose - fetching vdate                                  #
#############################################################
def get_vdate(conn):
    L_func_name ="get_vdate"
    O_status = 0
    emp_list = list()
    print("EXECUTING: ",L_func_name)
    try:
        Q_get_vdate = "SELECT SYSTEM_DATE FROM calendar_variables;"
        df_vdate = pd.read_sql(Q_get_vdate,conn)
        df_vdate = df_vdate["SYSTEM_DATE"][0]
        return df_vdate
    except Exception as error:
        print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
        return emp_list

#------------------------------------
def get_sys_option(conn):
    L_func_name ="get_sys_option"
    O_status = 0
    emp_list = list()
    print("EXECUTING: ",L_func_name)
    try:
        Q_get_sys_opt = "SELECT * FROM system_options;"
        df_sys_opt = pd.read_sql(Q_get_sys_opt,conn)
        return df_sys_opt
    except Exception as error:
        print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
        return emp_list