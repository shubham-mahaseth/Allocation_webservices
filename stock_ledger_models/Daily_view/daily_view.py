
import pandas as pd
import mysql.connector
#import pymysql
#from sqlalchemy import create_engine
from .mysql_conn import get_mysql_conn
# import sys        
# sys.path.append("E:\stock_ledger_webs\Stock_ledger\stock_ledger_web_services\STOCK_LEDGER_LISTENER\STOCK_LEDGER\GLOBAL_FILES") 
# from mysql_conn import get_mysql_conn

def get_daily_view(daily_date=None):
    O_status=[0]
    try:
        O_status[0]=100
        Q_select_trn=f'SELECT trn_name from trn_type_dtl;'
        if daily_date:
            Q_select_ds="SELECT ds.*, CASE WHEN ds.trn_type = 'OSK' THEN 'Opening Stock' WHEN ds.trn_type = 'CSK' THEN 'Closing Stock' ELSE TTD.TRN_NAME END AS TRN_NAME FROM DAILY_SKU ds LEFT JOIN TRN_TYPE_DTL ttd ON ds.trn_type=ttd.trn_type and ds.aref=ttd.aref WHERE ds.daily_date='{daily_date}' order by item,location,daily_date asc;"
        else:
            Q_select_ds="SELECT ds.*, CASE WHEN ds.trn_type = 'OSK' THEN 'Opening Stock' WHEN ds.trn_type = 'CSK' THEN 'Closing Stock' ELSE TTD.TRN_NAME END AS TRN_NAME FROM DAILY_SKU ds  LEFT JOIN TRN_TYPE_DTL ttd ON ds.trn_type=ttd.trn_type and ds.aref=ttd.aref order by item,location,daily_date asc;"
        I_db_connect_status = list()
        I_db_connect_status.append(0)
        O_status[0]=200
        with get_mysql_conn(I_db_connect_status) as conn:
            conn.autocommit=False
            O_status[0]=300
            mycursor = conn.cursor()     
            mycursor.execute(Q_select_trn)   
            avail_trn_name = list(mycursor.fetchall())
            avail_trn_name.extend([('Opening Stock',),('Closing Stock',)])  
            O_status[0]=400
            mycursor.execute(Q_select_ds)
            myresult = mycursor.fetchall()
            df_mysql = pd.DataFrame(myresult)
            df_mysql.columns=mycursor.column_names
            print(df_mysql)
            #convert to dict
            trn_name_dict = {trn_name[0]:index for index, trn_name in enumerate(avail_trn_name)}
            value_array = []
            newtable = dict()
            newtablecol_arr = ["ITEM","LOCATION","DAILY_DATE"]
            for i in range(0,len(avail_trn_name)):
                newtablecol_arr.append(avail_trn_name[i][0]+"_QTY")
                newtablecol_arr.append(avail_trn_name[i][0]+"_TOTAL_COST")
                newtablecol_arr.append(avail_trn_name[i][0]+"_TOTAL_RETAIL")
            O_status[0]=500
            for i in range(0,len(df_mysql)):
                key = f'{df_mysql.loc[i,"ITEM"]}/{df_mysql.loc[i,"LOCATION"]}/{df_mysql.loc[i,"DAILY_DATE"]}'
                if key in newtable.keys():
                    value_array = newtable[key][3:]
                else:
                    value_array = []
                    for h in range(0,len(avail_trn_name)*3):
                        value_array.append('NULL')
                trn_name_indx = trn_name_dict[df_mysql.loc[i, "TRN_NAME"]]
                value_array[trn_name_indx*3:(trn_name_indx*3)+3] = [df_mysql.loc[i,"QTY"],df_mysql.loc[i,"TOTAL_COST"],df_mysql.loc[i,"TOTAL_RETAIL"]]
                newtable[key] = [df_mysql.loc[i,"ITEM"], df_mysql.loc[i,"LOCATION"], df_mysql.loc[i,"DAILY_DATE"]] + value_array
            print('after for loop')
            df = pd.DataFrame(newtable, index = newtablecol_arr)
            df1 = df.transpose()
            print(df1)
            return df1             

    except Exception as e:
        if O_status[0]==100:
            print("daily_view: Exception raised during selecting the data from daily_sku or trn_type tables: ",daily_date , e)
        elif O_status[0]==200:
            print("daily_view: Exception raised during connecting to database. ", e)
        elif O_status[0]==300:
            print("daily_view: Exception raised during execution of trn_type select statement. ",e)
        elif O_status[0]==400:
            print("daily_view: Exception raised during execution of daily_sku select statement. ",e)
        elif O_status[0]==500:
            print("daily_view: Exception raised during processing data of daily_sku table into view. ", e)
        else:
            print( 'daily_view: Exception Occured', e)
        return []

if __name__ == "__main__":
    daily_view = get_daily_view()    
    print(daily_view)
    #print(daily_view.loc['12345/100/2022-07-14'])
    #print(daily_view.loc['12345/100/2022-05-20'])