from pickle import TRUE
import pandas as pd
import yaml
from datetime import date
from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
from ..CREATE_ALLOC_FUNCTIONS.upd_alloc_item_resv_exp import upd_alloc_resv_exp




#-------------------------------------------#
# function COMPLETE_CREATE
# ------------------------------------------#
def complete_create(conn,O_status,I_alloc_no,I_resv_exp_ind):
    L_func_name ="complete_create"
    O_status = 0
    I_resv_exp_ind = 'Y'
    try:
        I_get_mysql_conn = list()
        I_get_mysql_conn.append(0)
           
        O_status = 1
        I_resv_exp_ind = 'Y'
        L_func,err = upd_resv_exp(conn,O_status,I_alloc_no,'A') #hard coded as 'A' because I_add_delete_ind = 'A' in rms
        
        return L_func,err


    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured before calling upd_resv_exp function: "+ str(error)
            print(L_func_name,":",O_status,":","Exception occured before calling upd_resv_exp function ", error)
        else:
            err_return = L_func_name+":"+"Exception occured: "+ str(error)
            print(L_func_name,":",O_status,":","Exception occured: ", error)
        conn.rollback()
        return False,err_return



#----------------------------------------------------#
# complete_create calling function upd_resv_exp 
# ---------------------------------------------------#

def upd_resv_exp(conn,O_status,I_alloc_no,I_add_delete_ind): #I_resv_exp_ind = I_add_delete_ind := 'A'
    L_func_name = "upd_resv_exp"
    try:
        print(1)
        I_get_mysql_conn = list()
        I_get_mysql_conn.append(0)
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/upd_item_resv_exp_queries.yaml') as fh:
            queries         = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_sel_rec     = queries['upd_resv_exp']['Q_sel_rec'] 

            O_status = 1
            df_sel_rec = pd.read_sql(Q_sel_rec,conn,params=(I_alloc_no,))
            L_recds = df_sel_rec

            O_status = 2
            if len(df_sel_rec)>0:
                for val in df_sel_rec.values:

                    L_rec_no=int(val)

                    L_func,err = upd_alloc_resv_exp(conn,O_status,L_rec_no,I_add_delete_ind)
                    if L_func == False:
                        return False,err
        return True,""

    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while selecting the records from alloc_sku_head table "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while selecting the records from alloc_sku_head table ", error)
        elif O_status == 2:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured before calling upd_alloc_resv_exp function "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured before calling upd_alloc_resv_exp function ", error)
        else:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured: ", error)
        conn.rollback()
        print(err_return)
        return False,err_return
#    I_alloc_no=332
#    I_resv_exp_ind='Y'
#    O_status=None
#    conn=None
#    daily_view = upd_resv_exp(conn,O_status,I_alloc_no,I_resv_exp_ind)  
#    print(daily_view);










