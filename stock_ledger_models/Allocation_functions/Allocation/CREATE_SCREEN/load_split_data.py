import mysql.connector
import pandas as pd
import yaml

def load_split(conn,
               I_alloc_no,
               O_status):
    L_func_name = "load_split"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/load_split_data_queries.yaml') as fls:
            queries      = yaml.load(fls,Loader=yaml.SafeLoader)
            Q_chck_split = queries['func_load_split_data']['Q_chck_split']
            Q_create_tbl = queries['func_load_split_data']['Q_create_tbl']
            Q_chck_tbl   = queries['func_load_split_data']['Q_chck_tbl']
            #Q_del_split  = queries['func_load_split_data']['Q_del_split']
            Q_ins_split  = queries['func_load_split_data']['Q_ins_split']
            Q_update_dtl = queries['func_load_split_data']['Q_update_dtl']

            mycursor = conn.cursor()
            df_chck = pd.read_sql(Q_chck_split,conn,params=(I_alloc_no,))
            if len(df_chck) > 0:
                L_chck  = df_chck.chck[0]
                if L_chck == 1:
                    print("Allocation cannot be split since the PO is not in approved status.")
            else:
                df_chck_tbl = pd.read_sql(Q_chck_tbl,conn)
                L_chck_tbl  = df_chck_tbl.chk[0]
                if L_chck_tbl == 1:
                    print("alloc_item_loc_split_gtt already exists ")
                else:
                    mycursor.execute(Q_create_tbl)
                    print("table created")
                    
                    '''mycursor.execute(Q_del_split,(I_alloc_no,))
                    O_status = 1
                    print(O_status,"-","rows_affected: ",mycursor.rowcount)'''
                    O_status = 1
                    mycursor.execute(Q_ins_split,(I_alloc_no,))
                    O_status = 2
                    print(O_status,"-","rows_affected: ",mycursor.rowcount)
                    
                    mycursor.execute(Q_update_dtl,(I_alloc_no,))
                    O_status = 3
                    print(O_status,"-","rows_affected: ",mycursor.rowcount)

                    conn.commit()
                    return True,""

    except mysql.connector.Error as error:
        print("Failed to update table record: {}".format(error)) 
        err_return = ""
        if O_status == 1:
            print(L_func_name,":",O_status,":","Exception raised during deleting records from alloc_item_loc_split")
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised during deleting records from alloc_item_loc_split :"+ str(error)
        elif O_status == 2:
            print(L_func_name,":",O_status,":","Exception raised during insering records into alloc_item_loc_split")
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised during insering records into alloc_item_loc_split :"+ str(error)
        elif O_status == 3:
            print(L_func_name,":",O_status,":","Exception raised during updating split_ind in alloc_itm_search_dtl")
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised during updating split_ind in alloc_itm_search_dtl :"+ str(error)
        else:
            print(L_func_name,":",O_status,":","Exception occured: ",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured :"+ str(error)
        conn.rollback()
        return False,err_return

