
import pandas as pd
from datetime import date
from ..CREATE_ALLOC_FUNCTIONS.insert_alloc_sku_head_alloc_dtl import insert_tables
from ..CREATE_ALLOC_FUNCTIONS.complete_create import complete_create
import yaml


def create_alloc(conn,O_status,I_alloc_no,I_status):
    L_func_name = "create_alloc"
    O_status = 0
    try:
        I_get_mysql_conn = list()
        I_get_mysql_conn.append(0)
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/create_alloc_queries.yaml') as fh:
            queries               = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_sel_proces_id_head  = queries['create_alloc']['Q_sel_proces_id_head']
            Q_sel_process_id_dtl  = queries['create_alloc']['Q_sel_process_id_dtl']
            Q_del_sync_header     = queries['create_alloc']['Q_del_sync_header']
            Q_del_sync_dtl        = queries['create_alloc']['Q_del_sync_dtl']

            mycursor=conn.cursor()

            O_status = 1 
            if I_status == None:
                err_message = "Invalid parameters: I_status in create_alloc function is null"
                conn.cursor().close()
                return False,err_message

            if I_status not in ['APV','RSV']:
                err_message = "Invalid parameters: I_status in create_alloc function is not in approve or reserve"
                conn.cursor().close()
                return False,err_message

            O_status = 2
            mycursor.execute("unlock tables;")
            L_fun,err_msg1 = insert_tables(conn,O_status,I_alloc_no)
            if L_fun == False:
                conn.cursor().close()
                return False ,err_msg1  
            
            O_status = 3
            L_fun_1,err_msg2 = complete_create(conn,O_status,I_alloc_no,'Y')
            if L_fun_1 == False:
                conn.cursor().close()
                return False,err_msg2


            df_prcs_id_head = pd.read_sql(Q_sel_proces_id_head,conn,params=(I_alloc_no,))
            L_process_id_head   = df_prcs_id_head.alloc_sync_process_id[0]

            df_prcs_id_dtl = pd.read_sql(Q_sel_process_id_dtl,conn,params=(I_alloc_no,))
            L_process_id_dtl   = df_prcs_id_dtl.alloc_sync_process_id[0]

            mycursor.execute(Q_del_sync_header,(L_process_id_head,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            mycursor.execute(Q_del_sync_dtl,(L_process_id_dtl,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            conn.commit()
            conn.cursor().close()
            return True,""
            
    except Exception as error:
        err_return = ""
        if O_status == 1:
            print(L_func_name,":",O_status,":","Exception occured while checking the I_status  ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while checking the I_status :"+ str(error)
        elif O_status == 2:
            print(L_func_name,":",O_status,":","Exception occured before calling insert_table function ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured before calling insert_table function :"+ str(error)
        elif O_status == 3:
            print(L_func_name,":",O_status,":","Exception occured before calling complete_create function ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured before calling complete_create function :"+ str(error)
        else:
            print(L_func_name,":",O_status,":","Exception occured: ", error)
            err_return = L_func_name+": "+"Exception occured:"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False,err_return


