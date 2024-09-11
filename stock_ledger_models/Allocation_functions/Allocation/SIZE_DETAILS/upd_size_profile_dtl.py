import pandas as pd
#import numpy as np
import yaml
from ..INVENTORY_SETUP.update_alloc_ext import update_alloc

def upd_size_profile(conn,O_status,I_alloc,I_source_item,I_diff_ID,I_order,I_wh_id,I_loc,I_tran_item_value,I_input_qty):
    L_func_name ="upd_size_profile"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        O_status = 1
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/upd_size_profile_queries.yaml') as fh:
            print("inside with yaml")
            queries                                         = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_get_avail_before_qty                          = queries['upd_size_profile']['Q_get_avail_before_qty']
            Q_get_avail_qty                                 = queries['upd_size_profile']['Q_get_avail_qty']
            Q_upd_calc_item_loc                             = queries['upd_size_profile']['Q_upd_calc_item_loc']
            Q_get_l_alloc_qty_if                            = queries['upd_size_profile']['Q_get_l_alloc_qty_if']
            Q_get_l_total_alloc_qty1_if                     = queries['upd_size_profile']['Q_get_l_total_alloc_qty1_if']
            Q_get_l_total_alloc_qty1_if1                    = queries['upd_size_profile']['Q_get_l_total_alloc_qty1_if1']
            Q_get_l_alloc_qty_else                          = queries['upd_size_profile']['Q_get_l_alloc_qty_else']
            Q_get_l_total_alloc_qty1_else                   = queries['upd_size_profile']['Q_get_l_total_alloc_qty1_else']

            mycursor= conn.cursor()
            df_get_avail_before_qty=pd.read_sql(Q_get_avail_before_qty, conn, params=(I_alloc,I_source_item,I_diff_ID,I_order,I_wh_id,I_loc,I_tran_item_value))
            L_sku_avail_qty         = df_get_avail_before_qty.sum_sku_avail_qty[0]
            L_sku_total_calc_qty    = df_get_avail_before_qty.sum_sku_calc_qty[0]
            L_som_qty               = df_get_avail_before_qty.max_som_qty[0]
            print("L_sku_avail_qty is:",L_sku_avail_qty)
            print("L_sku_total_calc_qty is:",L_sku_total_calc_qty)
            print("L_som_qty is:", L_som_qty,I_input_qty)
            O_status = 2
            L_sku_cur_calc_qty = I_input_qty
            if L_sku_total_calc_qty == None:
                df_get_avail_qty= pd.read_sql(Q_get_avail_qty, conn, params=(I_alloc,I_source_item,I_diff_ID,I_order,I_wh_id,I_tran_item_value))
                L_sku_avail_qty         = df_get_avail_qty.sum_dis_sku_avail_qty[0]
                L_sku_total_calc_qty    = df_get_avail_qty.sm_sku_calc_qty[0]
                L_som_qty               = df_get_avail_qty.mx_som_qty[0]
                
                print("inside if L_sku_avail_qty is:",L_sku_avail_qty)
                print("inside if L_sku_total_calc_qty is:",L_sku_total_calc_qty)
                print("inside if L_som_qty is:",L_som_qty)
                print("inside if L_sku_cur_calc_qty is:",L_sku_cur_calc_qty)

                df_alloc_qty = pd.read_sql(Q_get_l_alloc_qty_if, conn, params=(L_sku_cur_calc_qty,L_som_qty,L_som_qty))
                if len(df_alloc_qty) >0:
                    L_alloc_qty         = df_alloc_qty.L_alloc_qty[0]

                df_total_alloc_qty1= pd.read_sql(Q_get_l_total_alloc_qty1_if, conn, params=(L_sku_cur_calc_qty,L_som_qty,L_som_qty,L_sku_total_calc_qty))
                if len(df_total_alloc_qty1) >0:
                    L_total_alloc_qty   = df_total_alloc_qty1.L_total_alloc_qty[0]

                df_total_alloc_qty2= pd.read_sql(Q_get_l_total_alloc_qty1_if1, conn, params=(L_sku_cur_calc_qty,L_som_qty,L_som_qty,L_sku_total_calc_qty))
                if len(df_total_alloc_qty2) >0:
                    L_total_alloc_qty   = df_total_alloc_qty2.L_total_alloc_qty[0]
                    print("L_alloc_qty inside if is:", L_alloc_qty)
                    print("L_total_alloc_qty inside if is:", L_total_alloc_qty)
            else:
                O_status = 3
                print("inside else L_sku_avail_qty is:",L_sku_avail_qty)
                print("inside else L_sku_total_calc_qty is:",L_sku_total_calc_qty)
                print("inside else L_som_qty is:",L_som_qty)
                print("inside else L_sku_cur_calc_qty is:",L_sku_cur_calc_qty)

                df_alloc_qty = pd.read_sql(Q_get_l_alloc_qty_else, conn, params=(L_sku_cur_calc_qty,L_som_qty,L_som_qty))
                if len(df_alloc_qty) >0:
                    L_alloc_qty = df_alloc_qty.L_alloc_qty[0]
                
                df_total_alloc_qty1= pd.read_sql(Q_get_l_total_alloc_qty1_else, conn, params=(L_sku_cur_calc_qty,L_som_qty,L_som_qty,L_sku_total_calc_qty))
                if len(df_total_alloc_qty1) >0:
                    L_total_alloc_qty   = df_total_alloc_qty1.L_total_alloc_qty[0]
                    print("L_alloc_qty inside else is:", L_alloc_qty)
                    print("L_total_alloc_qty inside else is:", L_total_alloc_qty)
            O_status = 4
            L_sku_avail_var_qty = L_sku_avail_qty - L_sku_total_calc_qty
            if L_alloc_qty<0:
                L_err_msg="Allocated units can not go lower than the remaining units."
                return L_err_msg, False
            if L_total_alloc_qty > L_sku_avail_qty:
                L_err_msg= "Allocated units is greater than the remaining units."
                return L_err_msg, False
            else:
                mycursor.execute(Q_upd_calc_item_loc, (L_alloc_qty,I_alloc, I_source_item,I_diff_ID, I_order,I_wh_id,I_loc,I_tran_item_value))
                I_input_data = list()
                if update_alloc(conn,O_status,I_alloc,None,None,'N',I_input_data) ==False:
                    L_err_msg="Error occured while updating recalc ind"
                    print("Error occured while updating recalc ind: ",O_status)
                    conn.cursor().close()
                    return L_err_msg, False
                O_status = 5
                df_get_avail_qty= pd.read_sql(Q_get_avail_qty, conn, params=(I_alloc, I_source_item,I_diff_ID,I_order,I_wh_id,I_tran_item_value))
                L_sku_avail_qty         = df_get_avail_qty.sum_dis_sku_avail_qty[0]
                L_sku_total_calc_qty    = df_get_avail_qty.sm_sku_calc_qty[0]
                L_som_qty               = df_get_avail_qty.mx_som_qty[0]

                L_sku_avail_var_qty = abs(float(L_sku_avail_qty) - float(L_sku_total_calc_qty)) # return
            return L_total_alloc_qty,L_sku_avail_qty,L_sku_avail_var_qty,True


    except Exception as error:
        err_return = ""
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while executing df_get_avail_before_qty: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while executing df_get_avail_before_qty :"+ str(error)
        elif O_status==2:
            print(L_func_name,":",O_status,":","Exception occured while executing checking L_sku_total_calc_qty: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while executing checking L_sku_total_calc_qty :"+ str(error)
        elif O_status==3 :
            print(L_func_name,":",O_status,":","Exception occured while executing updating L_alloc_qty and L_total_alloc_qty: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while executing updating L_alloc_qty and L_total_alloc_qty :"+ str(error)
        elif O_status==4 :
            print(L_func_name,":",O_status,":","Exception occured while executing updating L_sku_avail_var_qty: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while executing updating L_sku_avail_var_qty :"+ str(error)
        elif O_status==5 :
            print(L_func_name,":",O_status,":","Exception occured while executing Q_get_avail_qty: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while executing Q_get_avail_qty :"+ str(error)
        else:
            print("Exception occured in: ",L_func_name,":",O_status,":",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(error)
        return err_return,False


