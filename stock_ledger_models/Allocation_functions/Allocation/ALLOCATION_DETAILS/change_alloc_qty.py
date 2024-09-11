
from pickle import FALSE
from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
import pandas as pd
import numpy as np
from ..ALLOCATION_DETAILS.update_sku_calc_qty import update_alloc_detail_diff_qty
import yaml

def change_alloc_qty(conn,O_status,
                     I_alloc_no,I_wh_id,
                     I_item_id,I_diff_id,
                     I_order_no,I_location,
                     I_alloc_qty,I_som_qty):
    L_func_name = "change_alloc_qty"
    try:
        #with open('GLOBAL_FILES\setup_alloc_details_queries.yaml') as fh:
        #    queries          = yaml.load(fh, Loader=yaml.SafeLoader)

            Q_sel_rule = "SELECT net_need_ind,rule_type FROM alloc_rule WHERE alloc_no = %s;"

            Q_cal_variance = """SELECT coalesce((SUM(TMP.SKU_CALC_QTY)-SUM(TMP.GROSS_NEED_NO_OWN_SHIP)
                                                    / NULLIF(SUM(TMP.GROSS_NEED_NO_OWN_SHIP),0))
                                                    * 100,0) calc_variance  
                                  FROM item_dtl                 IM,
                                       alloc_calc_item_loc   TMP,
                                       alloc_loc_group_detail       LG,
                                       alloc_location               LC
                                WHERE  TMP.alloc_no = %s
                                  AND  TMP.SOURCE_ITEM = IM.ITEM
                                  AND  LG.alloc_no = TMP.alloc_no
                                  AND  LC.LOC_GROUP_ID = LG.LOC_GROUP_ID
                                  AND  LC.LOCATION_ID  = TMP.TO_LOC
                                  AND  TMP.WH_ID = %s
                                  AND  TMP.SOURCE_ITEM = %s
                                  AND  coalesce(TMP.DIFF_ID,'$')   = coalesce(%s,coalesce(TMP.DIFF_ID,'$'))
                                  AND  coalesce(TMP.ORDER_NO, '$') = coalesce(%s, coalesce(TMP.ORDER_NO, '$'))
                                  AND  TMP.TO_LOC = %s;"""
           
            L_alloc_qty = I_alloc_qty
            L_som = I_som_qty
            print("L_som:::::",L_alloc_qty,conn)
            #L_som = L_som if L_som is not None else 0
            if I_alloc_qty != None:
                L_orig_alloc_qty = round(L_alloc_qty/L_som,0)*L_som
                print("L_orig_alloc_qty::::::::",L_orig_alloc_qty)
                L_alloc_list,err_msg= update_alloc_detail_diff_qty(conn,I_alloc_no,I_wh_id,I_item_id,I_diff_id,I_order_no,I_location,L_orig_alloc_qty,O_status)
                print("function output values",L_alloc_list)
                #L_dif_tot_qty,L_diff_alloc_qty,L_alloc_qty,L_func
                if len(L_alloc_list) == 0:
                    return [],err_msg

                #1st-loc_item_level,2nd-item_wh_level,3rd-item_head
                print("L_alloc_qty:::::",L_alloc_qty)
                mycursor = conn.cursor()

                df_alloc_rec = pd.read_sql(Q_sel_rule,conn,params=(I_alloc_no,))
                L_rule_type = df_alloc_rec.rule_type[0]

                df_calc_variance = pd.read_sql(Q_cal_variance,conn,params=(I_alloc_no,I_wh_id,I_item_id,None,None,I_location,))
                L_variance = df_calc_variance.calc_variance[0]
                print("L_variance::::::",L_variance)
                O_variance=0
                if L_variance >0 or L_variance <0 or L_variance == 0 :
                   O_variance =  L_variance

                #if L_rule_type == 'M': # react should update calc_qty column in all grids
                #    O_alloc_qty = L_alloc_qty
                #    O_diff_alloc_qty = L_diff_alloc_qty
                #    O_dif_tot_qty = L_dif_tot_qty
                return [*L_alloc_list,O_variance],""

    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while checking the I_status: "+ str(error)
        elif O_status == 2:
            err_return = L_func_name+":"+str(O_status)+": Exception occured before calling insert_table function: "+ str(error)
        elif O_status == 3:
            err_return = L_func_name+":"+str(O_status)+": Exception occured before calling complete_create function: "+ str(error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(error)
        print(err_return)
        conn.rollback()
        return [], err_return