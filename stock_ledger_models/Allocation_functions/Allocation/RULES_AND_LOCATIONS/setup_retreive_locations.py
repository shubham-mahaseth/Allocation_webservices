from GLOBAL_FILES.get_connection import get_mysql_conn
from GLOBAL_FILES.null_handler import null_py
import yaml
import pandas as pd

##################################################################################
#Created By - Naveen Ramanathan                                                  #
#File Name  - setup_rules_locations.py                                           #
#Purpose    - For retreiving locations in Rules & Locations screen               #
##################################################################################

#REFERENCE: [GROUP_TYPE = 1-LOC LIST,5-LOC TRAIT,6-ALL STORE,7-STORE] [LOC_TYPE = M-MULTI LOCATION,T-TRAITS,A-ALL STORE,L-LOCATION]
def RETREIVE_LOCATIONS(conn,I_alloc_no):
    O_status = 0
    L_fun = "RETREIVE_LOCATIONS"
    try:
        with open(r"./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_rules_locations_queries.yaml") as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            C_temp_tbl = queries['retreive_locations']['C_temp_tbl']
            L_ins = queries['retreive_locations']['L_ins']
            L_sel = queries['retreive_locations']['L_sel']

            mycursor = conn.cursor()
            #status
            O_status = 1
            mycursor.execute(C_temp_tbl)
            #status
            O_status = 2
            mycursor.execute(L_ins,(I_alloc_no,I_alloc_no,I_alloc_no,I_alloc_no))
            print(O_status,"-","rows_affected: ",mycursor.rowcount) 
            #status
            O_status = 3
            df_out = pd.read_sql(L_sel,conn,params=(I_alloc_no,))
            conn.commit()
            return df_out
            
    except Exception as error:
        if O_status==1:
            print(L_fun,":",O_status,":","Exception raised during temporary table creation:", error)
        elif O_status==2:
            print(L_fun,":",O_status,":","Exception raised during data insertion into alloc_location_temp table:", error)
        elif O_status==3:
            print(L_fun,":",O_status,":","Exception raised during data retreival process:", error)
        else:
            print(L_fun,":",O_status,":","Exception Occured: ", error)
        return False        

##################################################################################

def INSERT_LOCATIONS(conn,I_alloc_no):
    O_status = 0
    L_fun = "INSERT_LOCATIONS"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_rules_locations_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)

            #status
            O_status = 1
            return True
            
    except Exception as error:
        if O_status==1:
            print(L_fun,":",O_status,":","Exception raised during data retreival process:", error)
        else:
            print(L_fun,":",O_status,":","Exception Occured: ", error)
        return False  