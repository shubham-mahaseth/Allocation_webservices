
from ..INVENTORY_SETUP.update_alloc_ext import * 
from ..INVENTORY_SETUP.inventory_setup import *
import pandas as pd
import yaml


#--------------------------------------------------------------
# Function to load_item_source 
#--------------------------------------------------------------
def load_item(conn,L_alloc_no,O_status):
    L_func_name = "load_item"
    O_status=0
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/load_item_source_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_sel_record=queries['load_item_source']['Q_sel_record']
            Q_alloc_level=queries['load_item_source']['Q_alloc_level']
            Q_del_alloc_item_src_tmp=queries['load_item_source']['Q_del_alloc_item_src_tmp']
            Q_del_item_source=queries['load_item_source']['Q_del_item_source']
            Q_merge=queries['load_item_source']['Q_merge']
            Q_ins_item_source=queries['load_item_source']['Q_ins_item_source']
            Q_upd_pack_ind=queries['load_item_source']['Q_upd_pack_ind']
            Q_del_qty=queries['load_item_source']['Q_del_qty']

            I_input_data = list()
            mycursor = conn.cursor()
            mycursor.execute("SET sql_mode = ''; ")
            O_status=10
            L_item_found=0
            df_alloc_lvl = pd.read_sql(Q_alloc_level,conn,params=(L_alloc_no,))
            df_alloc_lvl=df_alloc_lvl["alloc_level"][0]
            print("FROM LOAD ITEM S:\n ",df_alloc_lvl)
            O_status=20
            mycursor.execute(Q_del_alloc_item_src_tmp,(L_alloc_no,L_alloc_no,))

            mycursor.execute(Q_del_item_source,(L_alloc_no,L_alloc_no,))
            if mycursor.rowcount > 0:
                L_item_found=1

            O_status=30
            mycursor.execute("SET sql_mode = ''; ")
            mycursor.execute(Q_merge,(L_alloc_no,))
            if mycursor.rowcount > 0:
                L_item_found=1
                print("updated the data")

            O_status=40
            mycursor.execute("SET sql_mode = ''; ")
            mycursor.execute(Q_ins_item_source,(L_alloc_no,df_alloc_lvl,L_alloc_no,))
            if mycursor.rowcount > 0:
                L_item_found=1
                print("inserted the data")

            O_status=50
            if df_alloc_lvl=='T':
                mycursor.execute(Q_upd_pack_ind,(L_alloc_no,))
            O_status=60
            if L_item_found==1:
                L_fun_upd_alloc = update_alloc(conn,O_status,L_alloc_no,None,None,'Y',I_input_data)
                if L_fun_upd_alloc == False:
                    conn.rollback()
                    conn.cursor().close()
                    return False

                O_status=70
                L_fun_calc_source,err_msg2 = setup_item_location(conn,L_alloc_no,O_status)
                if L_fun_calc_source==False:
                    conn.rollback()
                    conn.cursor().close()
                    return False,err_msg2

                O_status=80
                mycursor.execute(Q_sel_record,(L_alloc_no,))
                L_rule = mycursor.fetchall()
                if len(L_rule) > 0:
                    L_fun_setup_loc,err_msg3 = setup_location(conn,L_alloc_no,O_status)
                    if L_fun_setup_loc == False:
                        conn.rollback()
                        conn.cursor().close()
                        return False,err_msg3
                O_status=90
                mycursor.execute(Q_del_qty,(L_alloc_no,L_alloc_no,L_alloc_no,L_alloc_no,L_alloc_no))  #Changes by Shubham for Pack#
            conn.commit()
            conn.cursor().close()
            return True, ""

    except Exception as argument:
        err_return = ""
        if O_status==10:
            print("load_item: Exception occured in selecting the alloc level: ",L_func_name,argument)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured in selecting the alloc level :"+ str(argument)
        elif O_status==20:
            print("load_item: Exception occured in deleting the data: ",L_func_name,argument)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured in deleting the data :"+ str(argument)
        elif O_status==30:
            print("load_item:Exception occured while merging the data in alloc_item_source_dtl : ",L_func_name,argument)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while merging the data in alloc_item_source_dtl :"+ str(argument)
        elif O_status==40:
            print("load_item:Exception occured while inserting the data in alloc_item_source_dtl ",L_func_name,argument)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting the data in alloc_item_source_dtl :"+ str(argument)
        elif O_status==50:
            print("load_item:Exception occured while updating the pack indicator in alloc_item_source_dtl: ",L_func_name,argument)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while updating the pack indicator in alloc_item_source_dtl :"+ str(argument)
        elif O_status==60:
            print("load_item:Exception occured while calling the update alloc ext function: ",L_func_name,argument)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while calling the update alloc ext function :"+ str(argument)
        elif O_status==70:
            print("load_item:Exception occured while calling the setup_item_location function: ",L_func_name,argument)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while calling the setup_item_location function :"+ str(argument)
        elif O_status==80:
            print("load_item:Exception occured while calling the setup_location function: ",L_func_name,argument)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while calling the setup_location function :"+ str(argument)
        elif O_status==90:
            print("load_item:Exception occured while deleting the records from quantity limits: ",L_func_name,argument)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting the records from quantity limits :"+ str(argument)
        else:
            print("load_item:",L_func_name,argument)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(argument)
        conn.rollback() #handle run time exception completed
        conn.cursor().close()
        return False,err_return