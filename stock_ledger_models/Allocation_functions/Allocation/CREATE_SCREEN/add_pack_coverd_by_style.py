#from pickle import TRUE
#from GLOBAL_FILES.get_connection import get_mysql_conn
#import mysql.connector
#import pandas as pd
#import yaml

#def pack_coverd_by_style(conn,O_status):
#    L_func_name = "pack_coverd_by_style"
#    try:
#        with open('GLOBAL_FILES\\pack_coverd_by_style.yaml') as fh:
#            queries      = yaml.load(fh, Loader=yaml.SafeLoader)
#            Q_merge_gtt = queries['add_pack_coverd_by_style']['Q_merge_gtt']

#            mycursor = conn.cursor()

#            O_status = 1
#            mycursor.execute(Q_merge_gtt)
#            print("excuted successfully")
#            conn.cursor().close()
#            return True #create wrapper for functions and close the cursor conn.cursor,connection as input

#    except Exception as error:
#        if O_status==1:
#            print(L_func_name,":",O_status,":","Exception occured while executing Q_merge_gtt query  : ", error) 
#        else: 
#            print("Exception occured in: ",L_func_name.format(error),error)
#        conn.rollback()
#        return False

