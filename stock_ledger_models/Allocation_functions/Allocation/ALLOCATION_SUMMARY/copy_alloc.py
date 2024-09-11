
from ..CREATE_SCREEN.populate_search_result import fetch_inventory
from ..RULES_AND_LOCATIONS.change_weight import load_change_weight_dates
from ..RULES_AND_LOCATIONS.setup_rules_locations import RETREIVE_LOCATIONS,INSERT_LOCATIONS
from ..INVENTORY_SETUP.load_item_source import load_item
from ..LIKE_ITEM.setup_like_item import insert_like_item_map
from ..WHATIF_SUMMARY.alloc_wisummary import *
from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
import pandas as pd
import math as mt
from decimal import Decimal
import yaml

def copy_alloc_data(conn,
                    I_copy_alloc_no,
                    I_create_id,
                    O_status):
    L_func_name    = "copy_alloc_data"
    L_avail_qty    = -1
    L_inactive_qty = -1
    no_data = list()
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/copy_alloc_data.yaml') as cad:  
            queries              = yaml.load(cad,Loader=yaml.SafeLoader)
            Q_get_alloc_type     = queries['func_copy_alloc']['Q_get_alloc_type']
            Q_alloc_criteria     = queries['func_copy_alloc']['Q_alloc_criteria']
            Q_wh_last_up_inv_sku = queries['func_copy_alloc']['Q_wh_last_up_inv_sku']
            Q_new_alloc_no       = queries['func_copy_alloc']['Q_new_alloc_no']
            Q_ins_alloc_head     = queries['func_copy_alloc']['Q_ins_alloc_head']
            Q_ins_alloc_rule     = queries['func_copy_alloc']['Q_ins_alloc_rule']
            Q_ins_loc_gtt        = queries['func_copy_alloc']['Q_ins_loc_gtt']
            Q_del_gtt_alloc_no   = queries['func_copy_alloc']['Q_del_gtt_alloc_no']
            Q_get_like_item_fnd  = queries['func_copy_alloc']['Q_get_like_item_fnd']
            Q_ins_like_item_ext  = queries['func_copy_alloc']['Q_ins_like_item_ext']
            Q_merge_like_item    = queries['func_copy_alloc']['Q_merge_like_item']
            Q_crt_diff_map_like  = queries['func_copy_alloc']['Q_crt_diff_map_like']
            Q_chk_diff_map_like  = queries['func_copy_alloc']['Q_chk_diff_map_like']
            Q_ins_diff_map_like  = queries['func_copy_alloc']['Q_ins_diff_map_like']
            Q_crt_like_item_tmp  = queries['func_copy_alloc']['Q_crt_like_item_tmp']
            Q_chk_like_map_like  = queries['func_copy_alloc']['Q_chk_like_map_like']
            Q_ins_qnty_limits    = queries['func_copy_alloc']['Q_ins_qnty_limits']
            Q_del_qnty_limits    = queries['func_copy_alloc']['Q_del_qnty_limits']
            Q_del_qnty_limits1   = queries['func_copy_alloc']['Q_del_qnty_limits1']

            Q_po_last_up_inv_sku  = queries['func_copy_alloc']['Q_po_last_up_inv_sku']
            Q_tsf_last_up_inv_sku = queries['func_copy_alloc']['Q_tsf_last_up_inv_sku']
            Q_asn_last_up_inv_sku = queries['func_copy_alloc']['Q_asn_last_up_inv_sku']

            #style/diff
            Q_wh_last_up_inv_style  = queries['func_copy_alloc']['Q_wh_last_up_inv_style']
            Q_po_last_up_inv_style  = queries['func_copy_alloc']['Q_po_last_up_inv_style']
            Q_tsf_last_up_inv_style = queries['func_copy_alloc']['Q_tsf_last_up_inv_style']
            Q_asn_last_up_inv_style = queries['func_copy_alloc']['Q_asn_last_up_inv_style']

            mycursor = conn.cursor()
            print("copy1::::",I_copy_alloc_no)
            O_status = 1
            df_get_alloc_type =  pd.read_sql(Q_get_alloc_type,conn,params=(I_copy_alloc_no,))
            print('copy2:::', df_get_alloc_type,len(df_get_alloc_type), Q_get_alloc_type%(I_copy_alloc_no,))
            if len(df_get_alloc_type) > 0:
                L_get_alloc_type     =  df_get_alloc_type.alloc_type[0]
                L_get_alloc_level    =  df_get_alloc_type.alloc_level[0]
                L_alloc_criteria     =  df_get_alloc_type.alloc_criteria[0]
                print(L_alloc_criteria,"L_alloc_criteria")
                if L_get_alloc_type != 'S':
                    O_status = 2

                    if L_get_alloc_level == 'T':
                        if L_alloc_criteria == 'W':
                            O_status = 3
                            df_wh_last_up_inv_sku = pd.read_sql(Q_wh_last_up_inv_sku,conn,params=(I_copy_alloc_no,))
                            if len(df_wh_last_up_inv_sku) > 0:
                                L_avail_qty    = df_wh_last_up_inv_sku.avail_qty[0]
                                L_inactive_qty = df_wh_last_up_inv_sku.inactive_qty[0]
                        elif L_alloc_criteria == 'P':
                            df_po_last_up_inv_sku = pd.read_sql(Q_po_last_up_inv_sku,conn,params=(I_copy_alloc_no,))
                            if len(df_po_last_up_inv_sku) > 0:
                                L_avail_qty    = df_po_last_up_inv_sku.available_qty[0]
                                L_inactive_qty = df_po_last_up_inv_sku.inactive_qty[0]
                        elif L_alloc_criteria == 'T':
                            df_tsf_last_up_inv_sku = pd.read_sql(Q_tsf_last_up_inv_sku,conn,params=(I_copy_alloc_no,))
                            if len(df_tsf_last_up_inv_sku) > 0:
                                L_avail_qty    = df_tsf_last_up_inv_sku.avail_qty[0]
                                L_inactive_qty = df_tsf_last_up_inv_sku.inactive_qty[0]
                        elif L_alloc_criteria == 'A':
                            df_asn_last_up_inv_sku = pd.read_sql(Q_asn_last_up_inv_sku,conn,params=(I_copy_alloc_no,))
                            if len(df_asn_last_up_inv_sku) > 0:
                                L_avail_qty    = df_asn_last_up_inv_sku.available_qty[0]
                                L_inactive_qty = 0
                    elif L_get_alloc_level == 'D':
                        print("inside_level")
                        if L_alloc_criteria == 'W':
                            O_status = 3
                            df_wh_last_up_inv_style = pd.read_sql(Q_wh_last_up_inv_style,conn,params=(I_copy_alloc_no,))
                            if len(df_wh_last_up_inv_style) > 0:
                                L_avail_qty    = df_wh_last_up_inv_style.avail_qty[0]
                                L_inactive_qty = df_wh_last_up_inv_style.inactive_qty[0]
                        elif L_alloc_criteria == 'P':
                            df_po_last_up_inv_style = pd.read_sql(Q_po_last_up_inv_style,conn,params=(I_copy_alloc_no,))
                            if len(df_po_last_up_inv_style) > 0:
                                L_avail_qty    = df_po_last_up_inv_style.avail_qty[0]
                                L_inactive_qty = df_po_last_up_inv_style.inactive_qty[0]
                        elif L_alloc_criteria == 'T':
                            print("inside_lac")
                            df_tsf_last_up_inv_style = pd.read_sql(Q_tsf_last_up_inv_style,conn,params=(I_copy_alloc_no,))
                            print("after_df")
                            if len(df_tsf_last_up_inv_style) > 0:
                                L_avail_qty    = df_tsf_last_up_inv_style.avail_qty[0]
                                L_inactive_qty = df_tsf_last_up_inv_style.inactive_qty[0]
                        elif L_alloc_criteria == 'A':
                            print(L_alloc_criteria,"L_alloc_criteria1")
                            df_asn_last_up_inv_style = pd.read_sql(Q_asn_last_up_inv_style,conn,params=(I_copy_alloc_no,))
                            if len(df_asn_last_up_inv_style) > 0:
                                L_avail_qty    = df_asn_last_up_inv_style.available_qty[0]
                                L_inactive_qty = 0
                    else:
                        print("Invalid allocation/order/item/warehouse combination.")
                        return False,L_func_name+": "+O_status+": Invalid allocation/order/item/warehouse combination."

                    if (L_avail_qty + L_inactive_qty) == 0 and  (L_alloc_criteria != 'F' or doc_type != 'F') :  #wis
                        print("Only items with available / inactive inventory can be copied over")
                        return False,L_func_name+": "+O_status+": Only items with available / inactive inventory can be copied over."

                O_status = 4
                df_new_alloc_no = pd.read_sql(Q_new_alloc_no,conn)
                I_new_alloc_no  = df_new_alloc_no.new_alloc_no[0]
                I_new_alloc_no  = mt.trunc(I_new_alloc_no)
                L_new_alloc_no = I_new_alloc_no + 1

                mycursor.execute("LOCK TABLES alloc_seq WRITE;")
                mycursor.execute("update alloc_seq set alloc_no = %s ;",(L_new_alloc_no,))#RC-lock&unlock
                conn.commit()
                mycursor.execute("UNLOCK TABLES;")

                O_status = 5
                print("INPUT VALUE: ",I_new_alloc_no,doc_type,order_no,I_copy_alloc_no,I_create_id,I_copy_alloc_no)
                mycursor.execute(Q_ins_alloc_head,(I_new_alloc_no,doc_type,order_no,I_copy_alloc_no,I_create_id,I_copy_alloc_no))   #wis
                conn.commit()
                print("\nALLOC HEAD CHECK:\n",Q_ins_alloc_head%(I_new_alloc_no,doc_type,order_no,I_copy_alloc_no,I_create_id,I_copy_alloc_no))

                print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)
                CS_Check, err_msg = copy_search(conn,
                               I_copy_alloc_no,
                               I_new_alloc_no,
                               L_get_alloc_type,
                               L_get_alloc_level,
                               L_alloc_criteria,
                               O_status)
                if  len(err_msg)  > 0:
                    return False, err_msg
                print("copy_search") #insert into alloc_itm_search_dtl
                
                O_status = 6
                mycursor.execute(Q_ins_alloc_rule,(I_new_alloc_no,I_copy_alloc_no))
                print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)
                CW_Check, err_msg =load_change_weight_dates(conn,
                                           I_new_alloc_no,
                                           O_status)
                if CW_Check == False and len(err_msg) > 0:
                    return False, err_msg
                print("load_change_weight_date") #insert into alloc_calc_need_dates
                
                df_retreive_locs, err_msg = RETREIVE_LOCATIONS(conn,I_copy_alloc_no) #temp data for locations
                if len(df_retreive_locs) ==0:
                    return False, err_msg
                if len(df_retreive_locs) > 0:
                    for i in range(len(df_retreive_locs)):
                        L_retreive_locs = (I_new_alloc_no,                            df_retreive_locs.loc[i,"LOC"],           
                                           df_retreive_locs.loc[i,"LOC_DESC"],        df_retreive_locs.loc[i,"LOC_TYPE"],     df_retreive_locs.loc[i,"DEFAULT_WH"],    
                                           df_retreive_locs.loc[i,"GROUP_ID"],        df_retreive_locs.loc[i,"GROUP_DESC"],   df_retreive_locs.loc[i,"LIKE_LOC"],      
                                           df_retreive_locs.loc[i,"LIKE_LOC_DESC"],   df_retreive_locs.loc[i,"WEIGHT_PCT"],   df_retreive_locs.loc[i,"CLEARANCE_IND"], 
                                           df_retreive_locs.loc[i,"ITEM_LOC_STATUS"], df_retreive_locs.loc[i,"RELEASE_DATE"], df_retreive_locs.loc[i,"DEL_IND"])
                        
                        O_status = 7
                        L_retreive_locs= convert_numpy(L_retreive_locs)
                        mycursor.execute("SET sql_mode = '';")
                        mycursor.execute(Q_ins_loc_gtt,L_retreive_locs)
                        print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

                        O_status = 8
                        #mycursor.execute(Q_del_gtt_alloc_no,(I_copy_alloc_no,))
                        print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)
                IL_Check, err_msg = INSERT_LOCATIONS(conn,I_new_alloc_no) 
                if IL_Check == False:
                    return False, err_msg
                print("INSERT_LOCATIONS") #insert into alloc_loc_group_detail,alloc_location
                
                mycursor.execute("SET sql_mode = '';")
                LI_Check, err_msg =load_item(conn,I_new_alloc_no,O_status)
                if LI_Check == True:
                    print("Load item source") #insert into alloc_item_source_dtl,alloc_calc_source_temp,alloc_calc_allitemloc
                else:
                    return False, err_msg
                df_get_like_item_fnd = pd.read_sql(Q_get_like_item_fnd,conn,params=(I_copy_alloc_no,))
                if len(df_get_like_item_fnd) > 0:
                    L_like_item_chck  =  df_get_like_item_fnd.chck[0]
                    if L_like_item_chck == 1:
                        O_status = 9
                        mycursor.execute(Q_ins_like_item_ext,(I_new_alloc_no,))
                        print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

                        O_status = 10
                        mycursor.execute(Q_merge_like_item,(I_copy_alloc_no,I_new_alloc_no))
                        print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

                        df_chk_diff_map_like = pd.read_sql(Q_chk_diff_map_like,conn)
                        L_chk_diff_map_like  = df_chk_diff_map_like.chk[0]
                        if L_chk_diff_map_like == 1:
                            print("alloc_like_item_diff_map_temp is already exists")
                        else:
                            mycursor.execute(Q_crt_diff_map_like)
                        mycursor.execute(Q_ins_diff_map_like,(I_new_alloc_no,))

                        df_chk_like_map_like = pd.read_sql(Q_chk_like_map_like,conn)
                        L_chk_like_map_like  = df_chk_like_map_like.chk[0]
                        if L_chk_like_map_like == 1:
                            print("alloc_like_item_diff_temp is already exists")
                        else:
                            mycursor.execute(Q_crt_like_item_tmp)
                        LIM_Check, err_msg =insert_like_item_map(conn,I_new_alloc_no,O_status)
                        if LIM_Check == True:
                            print("insert_like_item_map") #mapped item data
                        else:
                            return False, err_msg

                O_status = 11
                mycursor.execute(Q_ins_qnty_limits,(I_new_alloc_no,I_copy_alloc_no))
                print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status = 12
                mycursor.execute(Q_del_qnty_limits1,(I_new_alloc_no,I_new_alloc_no,I_new_alloc_no))
                print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

                conn.commit()
                conn.cursor().close()
                return I_new_alloc_no,""

    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while fetching the alloc_type "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while fetching the alloc_type: ", error)
        elif O_status == 2:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while fetching the alloc_criteria: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while fetching the alloc_criteria: ", error)
        elif O_status == 3:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while fetching the avail_qty and inactive_qty: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while fetching the avail_qty and inactive_qty: ", error)
        elif O_status == 4:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while fetching the new_alloc_no: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while fetching the new_alloc_no: ", error)
        elif O_status == 5:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting into alloc_head: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while inserting into alloc_head: ", error)
        elif O_status == 6:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting into alloc_rule: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while inserting into alloc_rule: ", error)
        elif O_status == 7:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting into alloc_location_temp: "+ str(error)

            #print(L_func_name,":",O_status,":","Exception occured while inserting into alloc_location_temp: ", error)
        elif O_status == 8:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while deleting from alloc_location_temp:  "+ str(error)

            #print(L_func_name,":",O_status,":","Exception occured while deleting from alloc_location_temp: ", error)
        elif O_status == 9:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting into alloc_like_item_source: "+ str(error)

            #print(L_func_name,":",O_status,":","Exception occured while inserting into alloc_like_item_source: ", error)
        elif O_status == 10:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating alloc_like_item_source: "+ str(error)

            #print(L_func_name,":",O_status,":","Exception occured while updating alloc_like_item_source: ", error)
        elif O_status == 11:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating alloc_quantity_limits: "+ str(error)

            #print(L_func_name,":",O_status,":","Exception occured while updating alloc_quantity_limits: ", error)
        elif O_status == 12:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while deleting duplicate records from alloc_quantity_limits: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while deleting duplicate records from alloc_quantity_limits: ", error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False,err_return
#----------------------------------------------------------------------------------------------------------------------------

def copy_search(conn,
                I_copy_alloc_no,
                I_new_alloc_no,
                L_get_alloc_type,
                L_get_alloc_level,
                L_alloc_criteria,
                O_status):
    L_func_name  = "copy_search"
    L_wh_source  = 0
    L_wi_source  = 0
    L_po_source  = 0
    L_asn_source = 0
    L_tsf_source = 0
    #style/sku
    L_po  = list()
    L_tsf = list()
    L_asn = list()
    no_data =  list()
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/copy_alloc_data.yaml') as cad:
            queries             = yaml.load(cad,Loader=yaml.SafeLoader)
            Q_create_wh_tbl     = queries['func_copy_search']['Q_create_wh_tbl']
            Q_create_il_tbl     = queries['func_copy_search']['Q_create_il_tbl']
            Q_create_items_tbl  = queries['func_copy_search']['Q_create_items_tbl']
            Q_chck_wh_tbl       = queries['func_copy_search']['Q_chck_wh_tbl']
            Q_chck_il_tbl       = queries['func_copy_search']['Q_chck_il_tbl']
            Q_chck_itms_tbl     = queries['func_copy_search']['Q_chck_itms_tbl']
            Q_insert_wh_tbl     = queries['func_copy_search']['Q_insert_wh_tbl']
            Q_insert_il_tbl     = queries['func_copy_search']['Q_insert_il_tbl']
            Q_ins_il_sku_tbl    = queries['func_copy_search']['Q_ins_il_sku_tbl']
            Q_ins_items_sku_tbl = queries['func_copy_search']['Q_ins_items_sku_tbl']
            Q_upd_sel_ind       = queries['func_copy_search']['Q_upd_sel_ind']
            Q_upd_sel_ind_wis   = queries['func_copy_search']['Q_upd_sel_ind_wis']
            Q_del_dup_recs      = queries['func_copy_search']['Q_del_dup_recs']

            #style/diff
            Q_ins_style_wh_tbl    = queries['func_copy_search']['Q_ins_style_wh_tbl']
            Q_asn_id              = queries['func_copy_search']['Q_asn_id']
            Q_tsf_no              = queries['func_copy_search']['Q_tsf_no']
            Q_po_no               = queries['func_copy_search']['Q_po_no']
            Q_ins_style_il_tbl    = queries['func_copy_search']['Q_ins_style_il_tbl']
            Q_ins_il_style_tbl    = queries['func_copy_search']['Q_ins_il_style_tbl']
            Q_ins_items_style_tbl = queries['func_copy_search']['Q_ins_items_style_tbl']

            mycursor = conn.cursor()

            O_status = 1

            df_chck_wh_tbl = pd.read_sql(Q_chck_wh_tbl,conn)
            L_chck_wh_tbl = df_chck_wh_tbl.chk[0]
            if L_chck_wh_tbl == 1:
                print("Please drop the table alloc_whs_search_gtt.")
                conn.cursor().close()
                return no_data, ""
            else:
                mycursor.execute(Q_create_wh_tbl)

                O_status = 2
                if L_get_alloc_level == 'T':  #style/diff
                    mycursor.execute(Q_insert_wh_tbl,(I_copy_alloc_no,))
                    print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)
                elif L_get_alloc_level == 'D': #style/diff
                    mycursor.execute(Q_ins_style_wh_tbl,(I_copy_alloc_no,))
                    print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)
            
            if L_alloc_criteria == 'A':
                mycursor.execute(Q_asn_id,(I_copy_alloc_no,)) 
                L_result = mycursor.fetchall()

                if len(L_result) > 0:
                    for i in range(len(L_result)):
                        L_asn_id = L_result[i]
                        L_asn_id = L_asn_id[0]
                        L_asn.append(L_asn_id)
            elif L_alloc_criteria == 'T':
                mycursor.execute(Q_tsf_no,(I_copy_alloc_no,)) 
                L_result = mycursor.fetchall()

                if len(L_result) > 0:
                    for i in range(len(L_result)):
                        L_tsf_no = L_result[i]
                        L_tsf_no = L_tsf_no[0]
                        L_tsf.append(L_tsf_no)
            elif L_alloc_criteria == 'P':
                mycursor.execute(Q_po_no,(I_copy_alloc_no,)) 
                L_result = mycursor.fetchall()

                if len(L_result) > 0:
                    for i in range(len(L_result)):
                        L_po_no = L_result[i]
                        L_po_no = L_po_no[0]
                        L_po.append(L_po_no)

            O_status = 3
            df_chck_il_tbl = pd.read_sql(Q_chck_il_tbl,conn)
            L_chck_il_tbl = df_chck_il_tbl.chk[0]
            if L_chck_il_tbl == 1:
                print("Please drop the table alloc_search_criteria_itm_temp.")
                conn.cursor().close()
                #return no_data
            else:
                mycursor.execute(Q_create_il_tbl)

                if doc_type != 'F': #wis
                    if L_get_alloc_type == 'C' and L_get_alloc_level == 'T':  #style/diff
                        O_status = 4
                        mycursor.execute(Q_insert_il_tbl,(I_copy_alloc_no,))
                        print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)
                    elif L_get_alloc_type == 'C' and L_get_alloc_level == 'D': #style/diff
                        mycursor.execute(Q_ins_style_il_tbl,(I_copy_alloc_no,))
                        print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)
                    else:
                        O_status = 5
                        if L_get_alloc_level == 'T':
                            mycursor.execute(Q_ins_il_sku_tbl,(I_copy_alloc_no,))
                            print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)
                        elif L_get_alloc_level == 'D':
                            mycursor.execute(Q_ins_il_style_tbl,(I_copy_alloc_no,))
                            print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)
            
            print(pd.read_sql("select * from alloc_search_criteria_itm_temp;",conn))
            
            O_status = 6
            df_chck_itms_tbl = pd.read_sql(Q_chck_itms_tbl,conn)
            L_chck_itms_tbl = df_chck_itms_tbl.chk[0]
            if L_chck_itms_tbl == 1:
                print("Please drop the table alloc_items_srch_gtt.")
                conn.cursor().close()
                return no_data,""
            else:
                mycursor.execute(Q_create_items_tbl)
                O_status = 7
                if L_get_alloc_level == 'T':
                    mycursor.execute(Q_ins_items_sku_tbl)
                    print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)
                elif L_get_alloc_level == 'D':
                    mycursor.execute(Q_ins_items_style_tbl)
                    print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit()

            print(pd.read_sql("select * from alloc_items_srch_temp;",conn))

            if L_alloc_criteria == 'W':
                L_wh_source = 1 
            elif L_alloc_criteria == 'F':
                L_wi_source = 1
            elif L_alloc_criteria == 'P':
                L_po_source = 1
            elif L_alloc_criteria == 'T':
                L_tsf_source = 1
            elif L_alloc_criteria == 'A':
                L_asn_source = 1

            I_search_criteria = {"WHATIF_SOURCE_TYPE_IND" :L_wi_source,
                                 "WH_SOURCE_TYPE_IND"     :L_wh_source,
                                 "TSF_SOURCE_TYPE_IND"    :L_tsf_source,
                                 "PO_SOURCE_TYPE_IND"     :L_po_source,
                                 "ASN_SOURCE_TYPE_IND"    :L_asn_source,
                                 "PO"                     :L_po, #5285918,4491407,5514033 po,5514033
                                 "PO_TYPE"                :None,
                                 "ASN"                    :L_asn, #asn '989-12-40994'
                                 "TSF"                    :L_tsf,
                                 "ALLOC_NO"               :I_new_alloc_no,
                                 "ALLOC_CRITERIA"         :L_alloc_criteria,
                                 "ALLOC_LEVEL"            :L_get_alloc_level,
                                 "ALLOC_TYPE"             :L_get_alloc_type,
                                 "CLEARANCE_IND"          :None,
                                 "DIFF_ID"                :[],
                                 "MIN_AVAIL_QTY"          :None,
                                 "MAX_AVAIL_QTY"          :None,
                                 "HIER1"                  :[],
                                 "HIER2"                  :[],
                                 "HIER3"                  :[],
                                 "SUPPLIER"               :[],
                                 "SUPPLIER_SITE"          :[],
                                 "PACK_NO"                :[],
                                 "PACK_IND"               :'N',
                                 "ITEM_PARENT"            :[], #201119710
                                 "SKU"                    :[],
                                 "ITEM_GRANDPARENT"       :[],
                                 "ITEM_LIST"              :[],
                                 "VPN"                    :[],
                                 "UDA"                    :[],
                                 "UDA_VALUE"              :[],
                                 "EXCLUDE_UDA"            :[],
                                 "EXCLUDE_UDA_VALUE"      :[],
                                 "START_DATE"             :None,
                                 "END_DATE"               :None,
                                 "EISD_START_DATE"        :None,
                                 "EISD_END_DATE"          :None,
                                  "SKU"                    :[]                           
                                 }

            L_fetch_inventory, err_msg = fetch_inventory(conn,I_search_criteria,O_status)
            if len(err_msg) > 0 :
                return no_data, err_msg
            print("Func fetch_inventory")

            if L_alloc_criteria != 'F':  
                O_status = 8
                mycursor.execute(Q_upd_sel_ind,(I_copy_alloc_no,I_new_alloc_no))
                print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)
            else: #wis
                O_status = 9
                mycursor.execute(Q_upd_sel_ind_wis,(I_copy_alloc_no,I_new_alloc_no))
                print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

            if doc_type != 'F': #wis
                O_status = 10
                mycursor.execute(Q_del_dup_recs,(I_copy_alloc_no,I_new_alloc_no,I_copy_alloc_no))
                print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)
                
            conn.commit()
            conn.cursor().close()
            return I_new_alloc_no, ""

    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while checking the wh table: "+ str(error)
        elif O_status == 2:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting into wh table: "+ str(error)
        elif O_status == 3:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while checking the item loc table: "+ str(error)
        elif O_status == 4:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting into item loc table: "+ str(error)
        elif O_status == 5:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting into alloc_search_criteria_itm_temp: "+ str(error)
        elif O_status == 6:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while checking the alloc_items_srch_temp table: "+ str(error)
        elif O_status == 7:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting into alloc_items_srch_temp: "+ str(error)
        elif O_status == 8:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating the sel_ind in alloc_itm_search_dtl: "+ str(error)
        elif O_status == 9:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating the sel_ind,avail_qty in alloc_itm_search_dtl for wif: "+ str(error)
        elif O_status == 10:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while deleting records from alloc_itm_search_dtl: "+ str(error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return no_data,err_return
