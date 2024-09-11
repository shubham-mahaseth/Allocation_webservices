from ..CALCULATION.calculation_setup import calculation_setup
from ..CALCULATION.pack_priority_need import pack_priority_need
from ..QUANTITY_LIMITS.setup_qty_limits import P360_RETREIVE_QUANTITY_LIMITS
from ..INVENTORY_SETUP.update_alloc_ext import update_alloc
from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
from ..GLOBAL_FILES.null_handler import null_py
from datetime import datetime,date, timedelta
import pandas as pd
import numpy as np
import yaml

#############################################################
# Created By - Himanshu Maheshwari                          #                
# File Name - calculate.py                                  #
# Purpose - calculate allocation                            #
#############################################################

def calculate(conn
              ,I_alloc
              ,O_status):
    L_func_name ="calculate"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        print("inside try")
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculate_queries.yaml') as fh:
            queries                                         = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_cre_calc_itm_loc_tbl                          = queries['calculate_new']['Q_cre_calc_itm_loc_tbl']
            Q_cre_item_loc_spread_tbl                       = queries['calculate_new']['Q_cre_item_loc_spread_tbl']
            Q_get_doc_type                                  = queries['calculate_new']['Q_get_doc_type']
            Q_sel_wh_str_rel_ind                            = queries['calculate_new']['Q_sel_wh_str_rel_ind']
            Q_chk_alloc                                     = queries['calculate_new']['Q_chk_alloc']
            Q_get_size_prof_ind                             = queries['calculate_new']['Q_get_size_prof_ind']
            Q_get_rule_date                                 = queries['calculate_new']['Q_get_rule_date']
            Q_del_rule_date                                 = queries['calculate_new']['Q_del_rule_date']
            Q_update_aql                                    = queries['calculate_new']['Q_update_aql']
            Q_del_cal_item_loc_gtt                          = queries['calculate_new']['Q_del_cal_item_loc_gtt']
            Q_del_whatif_sum_tmp                            = queries['calculate_new']['Q_del_whatif_sum_tmp']
            Q_del_size_prf_gtt                              = queries['calculate_new']['Q_del_size_prf_gtt']
            Q_sp_week                                       = queries['calculate_new']['Q_sp_week']
            Q_get_po_alloc                                  = queries['calculate_new']['Q_get_po_alloc']
            Q_ins_alc_size_prf_gtt_a                        = queries['calculate_new']['Q_ins_alc_size_prf_gtt_a']
            Q_ins_alc_size_prf_gtt_p                        = queries['calculate_new']['Q_ins_alc_size_prf_gtt_p']
            Q_ins_alc_size_prf_gtt_t                        = queries['calculate_new']['Q_ins_alc_size_prf_gtt_t']
            Q_ins_alc_size_prf_gtt                          = queries['calculate_new']['Q_ins_alc_size_prf_gtt']
            Q_get_comp_item                                 = queries['calculate_new']['Q_get_comp_item']
            Q_upd_gross_need                                = queries['calculate_new']['Q_upd_gross_need']
            Q_upd_sku_gross_need_no_own_ship                = queries['calculate_new']['Q_upd_sku_gross_need_no_own_ship']
            Q_upd_wh_on_ord_qty                             = queries['calculate_new']['Q_upd_wh_on_ord_qty']
            Q_upd_wh_on_alloc_qty                           = queries['calculate_new']['Q_upd_wh_on_alloc_qty']
            Q_upd_wh_on_alloc_qty_1                         = queries['calculate_new']['Q_upd_wh_on_alloc_qty_1']
            Q_upd_wh_on_order_qty_else                      = queries['calculate_new']['Q_upd_wh_on_order_qty_else']
            Q_upd_wh_on_alloc_qty_else                      = queries['calculate_new']['Q_upd_wh_on_alloc_qty_else']
            Q_upd_wh_on_alloc_qty_1_else                    = queries['calculate_new']['Q_upd_wh_on_alloc_qty_1_else']
            Q_upd_wh_on_ord_qty_else_doc_type               = queries['calculate_new']['Q_upd_wh_on_ord_qty_else_doc_type']
            Q_upd_wh_on_alloc_qty_else_doc_type             = queries['calculate_new']['Q_upd_wh_on_alloc_qty_else_doc_type']
            Q_upd_wh_on_alloc_qty_1_else_doc_type           = queries['calculate_new']['Q_upd_wh_on_alloc_qty_1_else_doc_type']
            Q_upd_wh_on_order_qty_else_doc_type_else        = queries['calculate_new']['Q_upd_wh_on_order_qty_else_doc_type_else']
            Q_upd_wh_on_alloc_qty_else_doc_type_else        = queries['calculate_new']['Q_upd_wh_on_alloc_qty_else_doc_type_else']
            Q_upd_wh_on_alloc_qty_1_else_doc_type_else      = queries['calculate_new']['Q_upd_wh_on_alloc_qty_1_else_doc_type_else']
            Q_upd_sku_avail_qty                             = queries['calculate_new']['Q_upd_sku_avail_qty']
            Q_upd_sku_avail_qty_p                           = queries['calculate_new']['Q_upd_sku_avail_qty_p']
            Q_del_calc_item_loc_temp_a                      = queries['calculate_new']['Q_del_calc_item_loc_temp_a']
            Q_upd_sku_avail_qty_a                           = queries['calculate_new']['Q_upd_sku_avail_qty_a']
            Q_del_calc_item_loc_temp_t                      = queries['calculate_new']['Q_del_calc_item_loc_temp_t']
            Q_upd_sku_avail_qty_t                           = queries['calculate_new']['Q_upd_sku_avail_qty_t']
            Q_upd_sku_ownership_ratio                       = queries['calculate_new']['Q_upd_sku_ownership_ratio']
            Q_del_calc_item_loc_1                           = queries['calculate_new']['Q_del_calc_item_loc_1']
            Q_upd_sku_ownership_ratio_else                  = queries['calculate_new']['Q_upd_sku_ownership_ratio_else']
            Q_upd_sku_calc_qty                              = queries['calculate_new']['Q_upd_sku_calc_qty']
            Q_upd_sku_calc_qty_bfr_qtlm                     = queries['calculate_new']['Q_upd_sku_calc_qty_bfr_qtlm']
            Q_upd_sku_calc_qty_bfr_sprd                     = queries['calculate_new']['Q_upd_sku_calc_qty_bfr_sprd']
            Q_ins_calc_item_loc_temp_1                      = queries['calculate_new']['Q_ins_calc_item_loc_temp_1']
            Q_del_item_loc_spread_gtt                       = queries['calculate_new']['Q_del_item_loc_spread_gtt']
            Q_check_open_adj_qty                            = queries['calculate_new']['Q_check_open_adj_qty']
            Q_get_spread_items                              = queries['calculate_new']['Q_get_spread_items']
            Q_del_item_loc_spread_temp                      = queries['calculate_new']['Q_del_item_loc_spread_temp']
            Q_ins_item_loc_spread_temp                      = queries['calculate_new']['Q_ins_item_loc_spread_temp']
            Q_upd_item_loc_spread_temp                      = queries['calculate_new']['Q_upd_item_loc_spread_temp']
            Q_upd_calc_item_loc_1                           = queries['calculate_new']['Q_upd_calc_item_loc_1']
            Q_mer_item_loc_spread_temp                      = queries['calculate_new']['Q_mer_item_loc_spread_temp']
            Q_rec_spread                                    = queries['calculate_new']['Q_rec_spread']
            Q_check_total_tran_Sku_Cal_qty                  = queries['calculate_new']['Q_check_total_tran_Sku_Cal_qty']
            Q_upd_calc_item_loc_2                           = queries['calculate_new']['Q_upd_calc_item_loc_2']
            Q_update_ql                                     = queries['calculate_new']['Q_update_ql']
            Q_del_cal_item_loc                              = queries['calculate_new']['Q_del_cal_item_loc']
            #Q_seed_calc_item_loc                            = queries['calculate_new']['Q_seed_calc_item_loc'] #Changes by Shubham#
            Q_ins_calc_item_loc                             = queries['calculate_new']['Q_ins_calc_item_loc']
            Q_chk_inval_ql_data                             = queries['calculate_new']['Q_chk_inval_ql_data']
            Q_get_calc_item_loc                             = queries['calculate_new']['Q_get_calc_item_loc']
            Q_no_avail_qty                                  = queries['calculate_new']['Q_no_avail_qty']
            Q_cre_alloc_po_eisd_temp                        = queries['calculate_new']['Q_cre_alloc_po_eisd_temp']
            Q_cre_alloc_size_prof_temp                      = queries['calculate_new']['Q_cre_alloc_size_prof_temp']
            Q_cre_alloc_calc_item_loc_temp1                 = queries['calculate_new']['Q_cre_alloc_calc_item_loc_temp1']
            Q_drop_alloc_calc_item_loc_temp1                = queries['calculate_new']['Q_drop_alloc_calc_item_loc_temp1']
            Q_cre_alloc_calc_item_loc_temp2                 = queries['calculate_new']['Q_cre_alloc_calc_item_loc_temp2']
            Q_drop_alloc_calc_item_loc_temp2                = queries['calculate_new']['Q_drop_alloc_calc_item_loc_temp2']
            Q_cre_alloc_item_loc_spread_temp1               = queries['calculate_new']['Q_cre_alloc_item_loc_spread_temp1']
            Q_drop_alloc_item_loc_spread_temp1              = queries['calculate_new']['Q_drop_alloc_item_loc_spread_temp1']
            Q_del_main_tbl                                  = queries['calculate_new']['Q_del_main_tbl']
            Q_ins_main_tbl                                  = queries['calculate_new']['Q_ins_main_tbl']
            Q_cre_temp_sku                                  = queries['calculate_new']['Q_cre_temp_sku']

            #status
            O_status = 1
            mycursor = conn.cursor()

            #creating table
            mycursor.execute(Q_cre_calc_itm_loc_tbl)
            mycursor.execute(Q_cre_item_loc_spread_tbl)
            mycursor.execute(Q_cre_alloc_po_eisd_temp)
            mycursor.execute(Q_cre_alloc_size_prof_temp)            

            O_status = 2
            df_doc_type = pd.read_sql(Q_get_doc_type,conn,params=(I_alloc,))
            
            if len(df_doc_type)>0:
                L_doc_type   = df_doc_type.alloc_criteria[0]
                L_avail_qty      = df_doc_type.avail_qty[0]

            O_status = 3
            L_doc_type_nvl = null_py(L_doc_type, 'W')
            if L_doc_type_nvl == 'F':
                L_wh_store_rel_ind = 'N'
            else:
                L_wh_store_rel_ind = mycursor.execute(Q_sel_wh_str_rel_ind,(I_alloc,))
                
            O_status = 4
            df_size_prof_ind = pd.read_sql(Q_get_size_prof_ind,conn,params=(I_alloc,))
            if len(df_size_prof_ind) >0:
                L_size_prof_ind = df_size_prof_ind.size_profile_ind[0]

            O_status = 5
            df_alloc_head = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))
            
            #status
            O_status = 6            
            if len(df_alloc_head)>0:
                L_alloc_criteria   = df_alloc_head.alloc_criteria[0]
                L_alloc_level      = df_alloc_head.alloc_level[0]
                L_wh_store_rel_ind = df_alloc_head.wh_store_rel_ind[0]

            O_status = 7
            if L_alloc_level== 'T':
                L_size_prof_ind = 'N'

            O_status = 8
            df_get_rule_dates=pd.read_sql(Q_get_rule_date,conn,params=(I_alloc,))
            if len(df_get_rule_dates) >0:
                L_rule_type             = df_get_rule_dates.rule_type[0]
                L_commit_date           = df_get_rule_dates.commit_date[0]
                L_on_order_commit_date  = df_get_rule_dates.on_order_commit_date[0]
                L_on_order_commit_weeks = df_get_rule_dates.on_order_commit_weeks[0]

            O_status = 9
            if L_rule_type == 'M':
                mycursor.execute(Q_del_rule_date,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_del_rule_date") 
                conn.commit()

                mycursor.execute(Q_update_ql,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_update_ql")  

                mycursor.execute(Q_update_aql,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_update_aql") 
                conn.commit()

            O_status = 10
            check_CSUp, err_msg = calculation_setup(conn, I_alloc, O_status)
            if check_CSUp == True:
                L_rty_ql, err_msg = P360_RETREIVE_QUANTITY_LIMITS(conn,I_alloc,'EDIT')
                if len(L_rty_ql)==0:
                    print("No records found for quantity limits: ",O_status)
                    conn.cursor().close()
                    return False, L_func_name+":"+str(O_status)+ ": " + str(err_msg)

                mycursor.execute(Q_del_cal_item_loc_gtt,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_del_cal_item_loc_gtt")

                mycursor.execute(Q_del_cal_item_loc,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_del_cal_item_loc")

                mycursor.execute(Q_del_whatif_sum_tmp,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_del_whatif_sum_tmp")
                conn.commit()

                O_status = 11               
                mycursor.execute(Q_del_size_prf_gtt)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_del_size_prf_gtt")

                df_sp_week = pd.read_sql(Q_sp_week,conn)
                L_sp_week= df_sp_week.code_desc[0]

                O_status = 12
                df_get_po_alloc= pd.read_sql(Q_get_po_alloc, conn, params=(I_alloc,))
                L_alloc_doc_type= df_get_po_alloc.alloc_criteria[0]

                O_status = 13
                if L_alloc_doc_type=='A':
                    L_ins_alc_size_prf_gtt_a =(I_alloc,L_size_prof_ind,L_wh_store_rel_ind,L_wh_store_rel_ind,L_sp_week,L_sp_week,L_size_prof_ind)
                    L_ins_alc_size_prf_gtt_a = convert_numpy(L_ins_alc_size_prf_gtt_a)

                    mycursor.execute(Q_ins_alc_size_prf_gtt_a,L_ins_alc_size_prf_gtt_a) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_ins_alc_size_prf_gtt_a")

                elif L_alloc_doc_type =='P':
                    L_ins_alc_size_prf_gtt_p =(I_alloc,L_size_prof_ind,L_sp_week,L_sp_week,L_size_prof_ind)
                    L_ins_alc_size_prf_gtt_p = convert_numpy(L_ins_alc_size_prf_gtt_p)

                    mycursor.execute(Q_ins_alc_size_prf_gtt_p,L_ins_alc_size_prf_gtt_p) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                elif L_alloc_doc_type =='T':
                    L_ins_alc_size_prf_gtt_t =(I_alloc,L_size_prof_ind,L_wh_store_rel_ind,L_wh_store_rel_ind,L_sp_week,L_sp_week,L_size_prof_ind)
                    L_ins_alc_size_prf_gtt_t = convert_numpy(L_ins_alc_size_prf_gtt_t)

                    mycursor.execute(Q_ins_alc_size_prf_gtt_t,L_ins_alc_size_prf_gtt_t) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                else:
                    print("before query Q_ins_alc_size_prf_gtt")
                    L_ins_alc_size_prf_temp_data = (I_alloc,L_size_prof_ind,L_sp_week,L_sp_week,L_size_prof_ind)
                    L_ins_alc_size_prf_temp_data = convert_numpy(L_ins_alc_size_prf_temp_data)
                    
                    mycursor.execute(Q_ins_alc_size_prf_gtt,L_ins_alc_size_prf_temp_data) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_ins_alc_size_prf_gtt")
                conn.commit()

                O_status = 14
                if L_alloc_level =='T':
                    L_num_1 = 0
                    df_get_comp_item = pd.read_sql(Q_get_comp_item,conn, params=(I_alloc,))
                    if len(df_get_comp_item) >0:
                        L_num_1 = df_get_comp_item.comp_item[0]

                    if L_num_1 ==1 :
                        check_PPN, err_msg  = pack_priority_need(conn,I_alloc,O_status)
                        if check_PPN == False: # create function
                            conn.rollback()
                            return False, L_func_name+":"+str(O_status)+": " + str(err_msg)

                params_1 = (I_alloc,I_alloc,I_alloc,L_alloc_level,I_alloc,I_alloc,L_wh_store_rel_ind,L_wh_store_rel_ind,L_alloc_level)
                #Changes by Shubham Start#
                O_status = 15
                mycursor.execute(Q_ins_calc_item_loc,params_1)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_ins_calc_item_loc") 
                # df_seed_calc = pd.read_sql(Q_seed_calc_item_loc,conn,params=params_1)
                # print(df_seed_calc)
                # if len(df_seed_calc)>0:
                    # for i in range(len(df_seed_calc)):
                        # L_insert_calc_item = (df_seed_calc.alloc_no[i],               df_seed_calc.tran_item[i],              df_seed_calc.source_item[i],
                                                # df_seed_calc.diff_id[i],                df_seed_calc.wh_id[i],                  df_seed_calc.order_no[i],
                                                # df_seed_calc.to_loc[i],                 df_seed_calc.assign_default_wh[i],      df_seed_calc.need_value[i],    
                                                # df_seed_calc.total_need_value[i],       df_seed_calc.total_avail_qty[i],        df_seed_calc.trend[i],     
                                                # df_seed_calc.wos[i],                    df_seed_calc.ql_min[i],                 df_seed_calc.ql_max[i],            
                                                # df_seed_calc.treshold[i],               df_seed_calc.min_need[i],               df_seed_calc.min_pack[i],        
                                                # df_seed_calc.max_pack[i],               df_seed_calc.avail_qty[i],              df_seed_calc.sku_avail_qty[i],        
                                                # df_seed_calc.hold_back_pct_flag[i],     df_seed_calc.hold_back_value[i],        df_seed_calc.total_need_count[i],           
                                                # df_seed_calc.ownership_ratio[i],        df_seed_calc.sku_ownership_ratio[i],    df_seed_calc.size_profile_ind[i],     
                                                # df_seed_calc.size_profile_id[i],        df_seed_calc.size_profile_qty[i],       df_seed_calc.total_profile_qty[i],     
                                                # df_seed_calc.rule_type[i],              df_seed_calc.rule_level[i],             df_seed_calc.exact_ind[i],       
                                                # df_seed_calc.net_need_ind[i],           df_seed_calc.gross_need_no_own_ship[i], df_seed_calc.sku_gross_need_no_own_ship[i],            
                                                # df_seed_calc.stock_on_hand[i],          df_seed_calc.on_order[i],               df_seed_calc.on_alloc[i], 
                                                # df_seed_calc.alloc_out[i],              df_seed_calc.in_transit_qty[i],         df_seed_calc.backorder_qty[i],   
                                                # df_seed_calc.gross_need[i],             df_seed_calc.calc_qty[i],               df_seed_calc.sku_calc_qty[i],         
                                                # df_seed_calc.sku_calc_qty_bfr_qtlm[i],  df_seed_calc.sku_calc_qty_bfr_sprd[i],  df_seed_calc.alloc_sku_calc_qty[i],     
                                                # df_seed_calc.som_qty[i],                df_seed_calc.alloc_qty[i],              df_seed_calc.status[i],     
                                                # df_seed_calc.error_message_txt[i],      df_seed_calc.create_id[i],              df_seed_calc.create_datetime[i],            
                                                # df_seed_calc.last_update_id[i],         df_seed_calc.last_update_datetime[i],   df_seed_calc.include_inv_in_min_ind[i],            
                                                # df_seed_calc.include_inv_in_max_ind[i], df_seed_calc.sku_spread_qty[i],         df_seed_calc.orig_sku_calc_qty[i],        
                                                # df_seed_calc.wh_on_order_qty[i],        df_seed_calc.wh_on_alloc_qty[i])
                        # #status
                        # O_status = 15
                        # L_insert_calc_item = convert_numpy(L_insert_calc_item)
                        # mycursor.execute(Q_ins_calc_item_loc,L_insert_calc_item)
                        # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_ins_calc_item_loc")                
                #Changes by Shubham End#                
                if L_alloc_level =='T':
                    mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)
                    print("L_wh_store_rel_ind is :", L_wh_store_rel_ind)

                    mycursor.execute(Q_upd_gross_need,(I_alloc,L_wh_store_rel_ind,L_wh_store_rel_ind))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_gross_need")

                    mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)
                    mycursor.execute(Q_upd_sku_gross_need_no_own_ship,(I_alloc,)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_sku_gross_need_no_own_ship")
                    conn.commit()

                O_status = 16
                L_hist_start_date = None
                L_hist_end_date   = None
                if L_on_order_commit_date !=None :
                    L_hist_end_date= L_on_order_commit_date
                elif L_on_order_commit_weeks != None:
                    L_hist_end_date = date.today()+ timedelta(days =(L_on_order_commit_weeks * 7))

                O_status = 17
                L_doc_type_nvl = null_py(L_doc_type, 'W')
                if L_doc_type_nvl == 'F':
                    if L_on_order_commit_date !=None or L_on_order_commit_weeks !=None:
                        mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)
                        mycursor.execute(Q_cre_alloc_calc_item_loc_temp2)

                        mycursor.execute(Q_upd_wh_on_ord_qty,(I_alloc,I_alloc,L_hist_start_date,L_hist_start_date,L_hist_end_date,L_hist_end_date))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_wh_on_ord_qty")

                        mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)
                        mycursor.execute (Q_drop_alloc_calc_item_loc_temp2)
                        mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)

                        mycursor.execute(Q_upd_wh_on_alloc_qty, (I_alloc,L_hist_start_date,L_hist_end_date))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_wh_on_alloc_qty")

                        #mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)

                        mycursor.execute(Q_upd_wh_on_alloc_qty_1, (I_alloc,L_hist_start_date,L_hist_end_date))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_wh_on_alloc_qty_1")

                        mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)
                    else:
                        mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)
                        mycursor.execute(Q_cre_alloc_calc_item_loc_temp2)

                        mycursor.execute(Q_upd_wh_on_order_qty_else,(I_alloc,I_alloc))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_wh_on_order_qty_else")

                        mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)
                        mycursor.execute (Q_drop_alloc_calc_item_loc_temp2)

                        mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)

                        mycursor.execute(Q_upd_wh_on_alloc_qty_else,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_wh_on_alloc_qty_else")

                        mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)

                        mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)

                        mycursor.execute(Q_upd_wh_on_alloc_qty_1_else,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_wh_on_alloc_qty_1_else")

                        mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)
                else:
                    if L_on_order_commit_date !=None or L_on_order_commit_weeks !=None:

                        mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)
                        mycursor.execute(Q_cre_alloc_calc_item_loc_temp2)

                        mycursor.execute(Q_upd_wh_on_ord_qty_else_doc_type,(I_alloc,I_alloc,L_hist_start_date,L_hist_start_date,L_hist_end_date,L_hist_end_date))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_wh_on_ord_qty_else_doc_type")

                        mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)
                        mycursor.execute (Q_drop_alloc_calc_item_loc_temp2)

                        mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)

                        mycursor.execute(Q_upd_wh_on_alloc_qty_else_doc_type, (I_alloc,L_hist_start_date,L_hist_end_date))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_wh_on_alloc_qty_else_doc_type")

                        mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)

                        mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)

                        mycursor.execute(Q_upd_wh_on_alloc_qty_1_else_doc_type, (I_alloc,L_hist_start_date,L_hist_end_date))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_wh_on_alloc_qty_1_else_doc_type")

                        mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)
                    else:
                        mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)
                        mycursor.execute(Q_cre_alloc_calc_item_loc_temp2)

                        mycursor.execute(Q_upd_wh_on_order_qty_else_doc_type_else,(I_alloc,I_alloc))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_wh_on_order_qty_else_doc_type_else")

                        mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)
                        mycursor.execute (Q_drop_alloc_calc_item_loc_temp2)

                        mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)

                        mycursor.execute(Q_upd_wh_on_alloc_qty_else_doc_type_else,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_wh_on_alloc_qty_else_doc_type_else")

                        mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)

                        mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)

                        mycursor.execute(Q_upd_wh_on_alloc_qty_1_else_doc_type_else,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_wh_on_alloc_qty_1_else_doc_type_else")

                        mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)
                conn.commit()
                O_status = 18
                if L_alloc_doc_type == 'W':
                    mycursor.execute("DROP TEMPORARY TABLE IF EXISTS alloc_calc_item_loc_temp1;")
                    
                    mycursor.execute(Q_cre_temp_sku,(I_alloc,))

                    mycursor.execute(Q_upd_sku_avail_qty)
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_sku_avail_qty")

                    mycursor.execute("DROP TEMPORARY TABLE IF EXISTS alloc_calc_item_loc_temp1;")
                    conn.commit()
                elif L_alloc_doc_type == 'P':
                    mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)

                    mycursor.execute(Q_upd_sku_avail_qty_p,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_sku_avail_qty_p")

                    mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)
                    conn.commit()
                elif L_alloc_doc_type == 'A':
                    mycursor.execute(Q_del_calc_item_loc_temp_a,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_del_calc_item_loc_temp_a")
                    conn.commit()

                    mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)

                    mycursor.execute(Q_upd_sku_avail_qty_a,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_sku_avail_qty_a")

                    mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)
                    conn.commit()
                elif L_alloc_doc_type == 'T':
                    mycursor.execute(Q_del_calc_item_loc_temp_t,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_del_calc_item_loc_temp_t")
                    conn.commit()

                    mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)

                    mycursor.execute(Q_upd_sku_avail_qty_t,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_sku_avail_qty_t")

                    mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)
                    #conn.commit()
                O_status = 19
                L_doc_type_nvl = null_py(L_doc_type, 'W')
                if L_doc_type_nvl != 'F':
                    mycursor.execute(Q_upd_sku_ownership_ratio,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_sku_ownership_ratio")
                    conn.commit()
                else:
                    mycursor.execute(Q_del_calc_item_loc_1,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_del_calc_item_loc_1")

                    mycursor.execute(Q_upd_sku_ownership_ratio_else,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_sku_ownership_ratio_else")
                    conn.commit()

                mycursor.execute(Q_upd_sku_calc_qty,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_sku_calc_qty")
                conn.commit()

                mycursor.execute(Q_upd_sku_calc_qty_bfr_qtlm,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_sku_calc_qty_bfr_qtlm")
                conn.commit()

                mycursor.execute(Q_upd_sku_calc_qty_bfr_sprd,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_sku_calc_qty_bfr_sprd")
                conn.commit()

                #mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)
                #print("Q_cre_alloc_calc_item_loc_temp1 executed successfully")
                #df_temp1 = pd.read_sql("SELECT * FROM alloc_calc_item_loc_temp1;", conn)
                #print(df_temp1)
                #mycursor.execute(Q_ins_calc_item_loc_temp_1)
                #print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_ins_calc_item_loc_temp_1")
                #mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)

                mycursor.execute(Q_del_item_loc_spread_gtt, (I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_del_item_loc_spread_gtt")
                conn.commit()

                O_status = 20
                mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)
                mycursor.execute(Q_cre_alloc_calc_item_loc_temp2)

                df_check_open_adj_qty = pd.read_sql(Q_check_open_adj_qty,conn, params=(I_alloc,I_alloc))

                mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)
                mycursor.execute (Q_drop_alloc_calc_item_loc_temp2)

                if len(df_check_open_adj_qty)>0:
                    L_final_check = df_check_open_adj_qty.final_check[0]
                    L_adj_items_priority = df_check_open_adj_qty.adj_items_priority[0]
                    for i in range(len(df_check_open_adj_qty)):
                        df_get_spread_items= pd.read_sql(Q_get_spread_items,conn, params=(I_alloc,))
                        mycursor.execute(Q_del_item_loc_spread_temp,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_del_item_loc_spread_temp")

                        mycursor.execute(Q_cre_alloc_item_loc_spread_temp1)

                        mycursor.execute(Q_ins_item_loc_spread_temp,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_ins_item_loc_spread_temp")

                        mycursor.execute(Q_drop_alloc_item_loc_spread_temp1)

                        O_status = 21
                        for i in range(len(df_get_spread_items)):
                            L_df_adj_units=df_get_spread_items.adj_units[i]
                            L_df_alloc_id=df_get_spread_items.alloc_no[i]
                            L_df_source_item=df_get_spread_items.source_item[i]
                            L_df_order_no=df_get_spread_items.order_no[i]
                            L_df_wh_id=df_get_spread_items.wh_id[i]
                            L_df_tran_item=df_get_spread_items.tran_item[i]

                            mycursor.execute(Q_upd_item_loc_spread_temp,(L_df_adj_units,L_df_adj_units,L_df_adj_units,L_df_adj_units,L_df_alloc_id,L_df_source_item,L_df_order_no,L_df_wh_id,L_df_tran_item))
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_item_loc_spread_temp")

                        del df_get_spread_items

                        mycursor.execute(Q_upd_calc_item_loc_1,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_calc_item_loc_1")

                        mycursor.execute(Q_cre_alloc_item_loc_spread_temp1)

                        mycursor.execute(Q_mer_item_loc_spread_temp,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_mer_item_loc_spread_temp")

                        mycursor.execute(Q_drop_alloc_item_loc_spread_temp1)

                        mycursor.execute(Q_cre_alloc_calc_item_loc_temp1)

                        df_rec_spread = pd.read_sql(Q_rec_spread,conn, params=(I_alloc,))

                        mycursor.execute (Q_drop_alloc_calc_item_loc_temp1)

                        for i in range(len(df_rec_spread)):
                            L_wh_id=df_rec_spread.wh_id[i]
                            L_order_no =df_rec_spread.order_no[i]
                            L_source_item =df_rec_spread.source_item[i]
                            L_tran_item =df_rec_spread.tran_item[i]
                            L_exact_ind =df_rec_spread.exact_ind[i]
                            L_sku_avail_qty = df_rec_spread.sku_avail_qty[i]
                            L_params_list =(I_alloc,L_exact_ind,L_wh_id,L_order_no,L_source_item,L_tran_item)
                            L_params_list = convert_numpy(L_params_list)
                            
                            df_get_total_tran_Sku_Cal_qty = pd.read_sql(Q_check_total_tran_Sku_Cal_qty, conn, params=(I_alloc,L_exact_ind,L_wh_id,L_order_no,L_source_item,L_tran_item))
                            
                            #mycursor.execute(Q_check_total_tran_Sku_Cal_qty,(I_alloc,L_exact_ind,L_wh_id,L_order_no,L_source_item,L_tran_item))
                            L_total_tran_sku_calc_qty = df_get_total_tran_Sku_Cal_qty.total_tran_sku_calc_qty[0]
                            
                            L_new_calc_qty = L_total_tran_sku_calc_qty - L_sku_avail_qty

                            O_status = 22 
                            if df_rec_spread.adj_items_priority[i] ==1:
                                L_adj_items_priority =0

                            if L_new_calc_qty >0 and (df_rec_spread.adj_items_priority[i] ==1 or L_adj_items_priority==1):
                                if df_rec_spread.som_qty[i] >L_new_calc_qty :
                                    L_new_calc_qty = L_new_calc_qty
                                else:
                                    L_new_calc_qty = df_rec_spread.som_qty[i]
                                mycursor.execute(Q_upd_calc_item_loc_2,(L_new_calc_qty,df_rec_spread.som_qty[i],df_rec_spread.som_qty[i],L_new_calc_qty,df_rec_spread.som_qty[i],df_rec_spread.som_qty[i],I_alloc,L_exact_ind,L_wh_id,L_order_no,L_source_item,L_tran_item, df_rec_spread.to_loc[i]))
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_calc_item_loc_2")
                                #conn.commit()
                O_status = 23
                mycursor.execute(Q_chk_inval_ql_data,(I_alloc,))
                myresult = mycursor.fetchall()
                if len(myresult)>0:
                    conn.rollback()
                    print('Max Qty Limits cannot be used when using Proportional rule.')
                    conn.cursor().close()
                    return False, L_func_name+":"+str(O_status)+": Max Qty Limits cannot be used when using Proportional rule."
            else:
                conn.rollback()
                conn.cursor().close()
                return False, L_func_name+":"+str(O_status) +": " + str(err_msg)

            O_status = 24            
            df_get_calc_item_loc = pd.read_sql(Q_get_calc_item_loc, conn, params=(I_alloc,))            
            L_found = df_get_calc_item_loc.sku_calc_qty_sum[0] 
            
            L_sku_calc_qty_chk = null_py(L_found,0)
            
            O_status = 50
            mycursor.execute(Q_del_main_tbl,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_del_main_tbl")

            #status
            O_status = 51
            mycursor.execute(Q_ins_main_tbl,(I_alloc,))    #inserting into main table
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_ins_main_tbl")
            conn.commit()

            if L_sku_calc_qty_chk !=0:
                I_input_data = list()   
                check_UA, err_msg = update_alloc(conn,O_status,I_alloc,None,None,'N',I_input_data)
                if  check_UA ==False:
                    print("Error occured while updating recalc ind: ",O_status)
                    conn.cursor().close()
                    return False, L_func_name+":"+str(O_status)+": "+ str(err_msg)

            O_status = 25
            L_doc_type_nvl = null_py(L_doc_type, 'W')
            if not (L_doc_type_nvl=='F' and L_avail_qty !=0):
                if L_found is None:
                    mycursor.execute(Q_no_avail_qty,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_no_avail_qty")
                    I_input_data = list()

                    check_UA, err_msg = update_alloc(conn,O_status,I_alloc,None,None,'Y',I_input_data)
                    if check_UA ==False:
                        print("Error occured while updating recalc ind: ",O_status)
                        conn.cursor().close()
                        return False, L_func_name+":"+str(O_status)+": "+ str(err_msg)

            conn.commit()            
            conn.cursor().close()
            return True,""

    except Exception as error:
        err_return = L_func_name+":"+str(O_status)+": Exception occured:  "+ str(error)
        #if O_status==1:            
        #    print(L_func_name,":",O_status,":","Exception occured while executing calculation function: ", error)
        #elif O_status==2:
        #    print(L_func_name,":",O_status,":","Exception occured while executing calculation function: ", error)
        #elif O_status>=3 and O_status<=7:
        #    print(L_func_name,":",O_status,":","Exception occured while executing calculation function: ", error)
        #elif O_status>=8 and O_status<=9:
        #    print(L_func_name,":",O_status,":","Exception occured while executing calculation function: ", error)
        #elif O_status<=10 and O_status>=24:
        #    print(L_func_name,":",O_status,":","Exception occured while executing calculation function: ", error)
        #elif O_status==25:
        #    print(L_func_name,":",O_status,":","Exception occured while executing calculation function: ", error)
        #else:
        #    print("Exception occured in: ",L_func_name,error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False,err_return