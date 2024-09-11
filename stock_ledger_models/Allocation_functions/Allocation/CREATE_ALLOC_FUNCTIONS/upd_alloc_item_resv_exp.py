
import pandas as pd
import yaml
from datetime import date
from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy



#----------------------------#
#function UPD_ALLOC_RESV_EXP
#----------------------------#

def upd_alloc_resv_exp(conn,O_status,I_alloc_no,I_add_delete_ind):
    L_func_name ="upd_alloc_resv_exp"
    O_status = 0
    try:

        I_get_mysql_conn = list()
        I_get_mysql_conn.append(0)
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/upd_item_resv_exp_queries.yaml') as fh:
            queries         = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_alloc_detail     = queries['upd_alloc_resv_exp']['Q_alloc_detail']

        L_from_loc_type = 'W'

        O_status = 1
        df_sel_rec = pd.read_sql(Q_alloc_detail,conn,params=(I_alloc_no,))


        for index, row in df_sel_rec.iterrows():
            #print("checking inside loop 1:::",row)
            O_status = 2
            L_item                 = f"{row['item']}"
            L_alloc_qty            = f"{row['alloc_qty']}"
            L_from_loc             = f"{row['from_loc']}"
            L_to_loc               = f"{row['to_loc']}"
            L_to_loc_type          = f"{row['to_loc_type']}"
            if I_add_delete_ind ==  'A':
                L_alloc_qty          = f"{row['alloc_qty']}"
            if I_add_delete_ind ==  'D':
                L_alloc_qty          = float(f"{row['alloc_qty']}") * (-1)
            
            O_status = 3
            L_fun_upd_item_resv_exp,err_msg = upd_item_resv_exp(conn,O_status,L_item,L_alloc_qty,L_from_loc,L_from_loc_type,L_to_loc,L_to_loc_type)
            if L_fun_upd_item_resv_exp == False:
                conn.cursor().close()
                return False,err_msg
        conn.cursor().close()
        return True,""

    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while selecting the records from alloc_dtl: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while selecting the records from alloc_dtl: ", error)
        elif O_status == 2:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while fetching the records from for loop "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while fetching the records from for loop ", error)
        elif O_status == 3:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured before calling the 'UPD_ITEM_RESV_EXP' function: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured before calling the 'UPD_ITEM_RESV_EXP' function: ", error)
        else:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured: ", error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False,err_return


#---------------------------------------------------#
#function upd_item_resv_exp
#should sent upd_item_resv will be I_from_loc warehouse
#should sent updd_item_exp will be I_to_loc   store
#---------------------------------------------------#

def upd_item_resv_exp(conn,O_status,I_item,I_allocated_qty,I_from_loc,I_from_loc_type,I_to_loc,I_to_loc_type): #UPD_ITEM_RESV_EXP
    try:

        L_func_name = 'upd_item_resv_exp'
        I_get_mysql_conn = list()
        I_get_mysql_conn.append(0) 
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/upd_item_resv_exp_queries.yaml') as fh:
            queries         = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_sel_pack_ind     = queries['upd_item_resv_exp']['Q_sel_pack_ind']
            Q_upd_item_resv     = queries['upd_item_resv_exp']['Q_upd_item_resv']
            Q_upd_item_exp     = queries['upd_item_resv_exp']['Q_upd_item_exp']
            Q_pack_ind_y     = queries['upd_item_resv_exp']['Q_pack_ind_y']
            Q_items_in_packs     = queries['upd_item_resv_exp']['Q_items_in_packs']

            O_status = 1
            mycursor=conn.cursor()
            df_sel_pack_ind = pd.read_sql(Q_sel_pack_ind,conn,params=(I_item,))
            L_pack_ind=df_sel_pack_ind["pack_ind"][0]

            O_status = 2
            if L_pack_ind == 'N':
                mycursor.execute(Q_upd_item_resv,(L_pack_ind,I_allocated_qty,I_from_loc,I_from_loc_type,I_item))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status = 3
                mycursor.execute(Q_upd_item_exp,(I_allocated_qty,I_to_loc,I_to_loc_type,I_item))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)
            
            O_status = 4
            if L_pack_ind == 'Y':
                mycursor.execute(Q_pack_ind_y,(I_allocated_qty,I_from_loc,I_item))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status = 5
                if I_to_loc_type == 'W':
                    mycursor.execute(Q_upd_item_exp,(I_allocated_qty,I_to_loc,I_to_loc_type,I_item))
                    print(O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status = 6
            df_sel_rec = pd.read_sql(Q_items_in_packs,conn,params=(I_item,))
            L_rec_pack=df_sel_rec

            for rec in range(len(L_rec_pack)):
                L_item = L_rec_pack.item[0]
                L_item_qty = float(I_allocated_qty) * L_rec_pack.pack_qty[0]

                O_status = 7
                mycursor.execute(Q_upd_item_resv,(L_pack_ind,L_item_qty,I_from_loc,I_from_loc_type,L_item))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status = 8
                mycursor.execute(Q_upd_item_exp,(L_item_qty,I_to_loc,I_to_loc_type,L_item))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)
        
        conn.commit()
        conn.cursor().close()
        return True,""
            

    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while selecting the pack_ind from item_dtl table: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while selecting the pack_ind from item_dtl table: ", error)
        elif O_status == 2:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while updating the query Q_upd_item_resv: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while updating the query Q_upd_item_resv: ", error)
        elif O_status == 3:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while updating the query Q_upd_item_exp: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while updating the query Q_upd_item_exp: ", error)
        elif O_status == 4:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while updating the query Q_upd_item_resv: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while updating the query Q_upd_item_resv: ", error)
        elif O_status == 5:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while updating the query Q_upd_item_exp: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while updating the query Q_upd_item_exp: ", error)
        elif O_status == 6:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while selecting the pack_ind records: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while selecting the pack_ind records: ", error)
        elif O_status == 7:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while updating the query Q_upd_item_resv: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while updating the query Q_upd_item_resv: ", error)
        elif O_status == 8:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while updating the query Q_upd_item_exp: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while updating the query Q_upd_item_exp: ", error)
        else:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured: ", error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False,err_return

