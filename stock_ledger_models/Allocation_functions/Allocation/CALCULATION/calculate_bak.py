from ..CALCULATION.calculation_setup import calculation_setup
from ..QUANTITY_LIMITS.setup_qty_limits import P360_RETREIVE_QUANTITY_LIMITS
from ..INVENTORY_SETUP.update_alloc_ext import update_alloc
from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
from datetime import datetime
import pandas as pd
import numpy as np
import yaml

#############################################################
# Created By - Priyanshu Pandey                             #                
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
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculate_queries.yaml') as fh:
            queries                      = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_chk_alloc                  = queries['calculate']['Q_chk_alloc']
            Q_fetch_rule                 = queries['calculate']['Q_fetch_rule']
            Q_chk_avail_qty              = queries['calculate']['Q_chk_avail_qty']
            Q_update_ql                  = queries['calculate']['Q_update_ql']
            Q_del_cal_item_loc           = queries['calculate']['Q_del_cal_item_loc']
            Q_seed_calc_item_loc         = queries['calculate']['Q_seed_calc_item_loc']
            Q_ins_calc_item_loc          = queries['calculate']['Q_ins_calc_item_loc']
            Q_cre_calc_itm_loc_tbl       = queries['calculate']['Q_cre_calc_itm_loc_tbl']
            Q_cre_item_loc_spread_tbl    = queries['calculate']['Q_cre_item_loc_spread_tbl']
            #merge
            Q_upd_gross_no_own           = queries['calculate2']['Q_upd_gross_no_own']
            Q_upd_wi_wh_on_ord_qty       = queries['calculate2']['Q_upd_wi_wh_on_ord_qty']
            Q_upd_wi_on_ord_qty_else     = queries['calculate2']['Q_upd_wi_on_ord_qty_else']
            Q_wi_on_alloc_qty            = queries['calculate2']['Q_wi_on_alloc_qty']
            Q_wi_on_alloc_qty_else       = queries['calculate2']['Q_wi_on_alloc_qty_else']
            Q_upd_wi_ord_on_alloc_qty    = queries['calculate2']['Q_upd_wi_ord_on_alloc_qty']
            Q_upd_wi_ord_on_alloc_qty_else = queries['calculate2']['Q_upd_wi_ord_on_alloc_qty_else']
            Q_upd_wh_on_ord_qty          = queries['calculate2']['Q_upd_wh_on_ord_qty']
            Q_upd_wh_on_ord_qty_else     = queries['calculate2']['Q_upd_wh_on_ord_qty_else']
            Q_wh_on_alloc_qty            = queries['calculate2']['Q_wh_on_alloc_qty']
            Q_wh_on_alloc_qty_else       = queries['calculate2']['Q_wh_on_alloc_qty_else']
            Q_upd_ord_on_alloc_qty       = queries['calculate2']['Q_upd_ord_on_alloc_qty']
            Q_upd_ord_on_alloc_qty_else  = queries['calculate2']['Q_upd_ord_on_alloc_qty_else']
            Q_cre_temp_sku               = queries['calculate2']['Q_cre_temp_sku']
            Q_upd_sku_avail_qty          = queries['calculate2']['Q_upd_sku_avail_qty']
            Q_fetch_itm_loc_spread       = queries['calculate2']['Q_fetch_itm_loc_spread']
            Q_ins_spread_itm_loc         = queries['calculate2']['Q_ins_spread_itm_loc']
            Q_del_spread_itm_loc         = queries['calculate2']['Q_del_spread_itm_loc']
            Q_chk_inval_ql_data          = queries['calculate2']['Q_chk_inval_ql_data']
            Q_adj_itm_priority           = queries['calculate2']['Q_adj_itm_priority']
            Q_adj_itm_priority1          = queries['calculate2']['Q_adj_itm_priority1']
            Q_update_inac_wh             = queries['calculate2']['Q_update_inac_wh']
            Q_update_inac_wh_wi          = queries['calculate2']['Q_update_inac_wh_wi']
            Q_wi_sku_ownship             = queries['calculate2']['Q_wi_sku_ownship']
            Q_update_sku_calc_qty        = queries['calculate2']['Q_update_sku_calc_qty']
            Q_upd_sku_calc_qty_bfr_qtlm  = queries['calculate2']['Q_upd_sku_calc_qty_bfr_qtlm']
            Q_upd_sku_calc_qty_bfr_sprd  = queries['calculate2']['Q_upd_sku_calc_qty_bfr_sprd']
            Q_upd_sprd_sku_calc_qty      = queries['calculate2']['Q_upd_sprd_sku_calc_qty']
            Q_upd_sprd_orig_sku_calc_qty = queries['calculate2']['Q_upd_sprd_orig_sku_calc_qty']
            Q_upd_tot_sku_calc           = queries['calculate2']['Q_upd_tot_sku_calc']
            Q_spread_rec_loop            = queries['calculate2']['Q_spread_rec_loop']
            Q_chk_tot_tran_sku_calc_qty  = queries['calculate2']['Q_chk_tot_tran_sku_calc_qty']
            Q_no_avail_qty               = queries['calculate2']['Q_no_avail_qty']
            Q_calc_chk                   = queries['calculate2']['Q_calc_chk']
            Q_upd_sprd_sku_calc_qty1    = queries['calculate2']['Q_upd_sprd_sku_calc_qty1']
            Q_del_main_tbl               = queries['calculate3']['Q_del_main_tbl']
            Q_ins_main_tbl               = queries['calculate3']['Q_ins_main_tbl']
            Q_fetch_items                = queries['calculate3']['Q_fetch_items']
            
            #status
            O_status = 1
            mycursor = conn.cursor()
            #creating table
            mycursor.execute(Q_cre_calc_itm_loc_tbl)
            mycursor.execute(Q_cre_item_loc_spread_tbl)

            #status
            O_status = 2
            mycursor.execute(Q_update_ql,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)            

            #status
            O_status = 3
            df_alloc_head = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))

            #status
            O_status = 4            
            if len(df_alloc_head)>0:
                L_alloc_criteria   = df_alloc_head.alloc_criteria[0]
                L_alloc_level      = df_alloc_head.alloc_level[0]
                L_wh_store_rel_ind = df_alloc_head.wh_store_rel_ind[0]

                df_alloc_rule = pd.read_sql(Q_fetch_rule,conn,params=(I_alloc,))
                if len(df_alloc_rule)>0:
                    L_commit_date           = df_alloc_rule.commit_date[0]
                    L_on_order_commit_date  = df_alloc_rule.on_order_commit_date[0]
                    L_on_order_commit_weeks = df_alloc_rule.on_order_commit_weeks[0]

                #status
                O_status = 5
                if calculation_setup(conn,
                                     I_alloc,
                                     O_status) == False:
                    print("calculation_setup failed: ",O_status)
                    conn.cursor().close()
                    return False
                
                #status
                O_status = 6
                mycursor.execute(Q_del_cal_item_loc,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()

                #status
                O_status = 7
                L_rty_ql = P360_RETREIVE_QUANTITY_LIMITS(conn,I_alloc,'EDIT')
                if len(L_rty_ql)==0:
                    print("No records found for quantity limits: ",O_status)
                    conn.cursor().close()
                    return False

                print("EXECUTING: ",L_func_name)
                #status
                O_status = 8
                if L_alloc_level == 'T':
                    params = (I_alloc,I_alloc,I_alloc,L_wh_store_rel_ind,L_wh_store_rel_ind,L_alloc_level)
                    df_seed_calc = pd.read_sql(Q_seed_calc_item_loc,conn,params=params)
                    if len(df_seed_calc)>0:
                        for i in range(len(df_seed_calc)):

                            L_insert_calc_item = (df_seed_calc.alloc_no[i],               df_seed_calc.tran_item[i],              df_seed_calc.source_item[i],
                                                  df_seed_calc.diff_id[i],                df_seed_calc.wh_id[i],                  df_seed_calc.order_no[i],
                                                  df_seed_calc.to_loc[i],                 df_seed_calc.assign_default_wh[i],      df_seed_calc.need_value[i],    
                                                  df_seed_calc.total_need_value[i],       df_seed_calc.total_avail_qty[i],        df_seed_calc.trend[i],     
                                                  df_seed_calc.wos[i],                    df_seed_calc.ql_min[i],                 df_seed_calc.ql_max[i],            
                                                  df_seed_calc.treshold[i],               df_seed_calc.min_need[i],               df_seed_calc.min_pack[i],        
                                                  df_seed_calc.max_pack[i],               df_seed_calc.avail_qty[i],              df_seed_calc.sku_avail_qty[i],        
                                                  df_seed_calc.hold_back_pct_flag[i],     df_seed_calc.hold_back_value[i],        df_seed_calc.total_need_count[i],           
                                                  df_seed_calc.ownership_ratio[i],        df_seed_calc.sku_ownership_ratio[i],    df_seed_calc.size_profile_ind[i],     
                                                  df_seed_calc.size_profile_id[i],        df_seed_calc.size_profile_qty[i],       df_seed_calc.total_profile_qty[i],     
                                                  df_seed_calc.rule_type[i],              df_seed_calc.rule_level[i],             df_seed_calc.exact_ind[i],       
                                                  df_seed_calc.net_need_ind[i],           df_seed_calc.gross_need_no_own_ship[i], df_seed_calc.sku_gross_need_no_own_ship[i],            
                                                  df_seed_calc.stock_on_hand[i],          df_seed_calc.on_order[i],               df_seed_calc.on_alloc[i], 
                                                  df_seed_calc.alloc_out[i],              df_seed_calc.in_transit_qty[i],         df_seed_calc.backorder_qty[i],   
                                                  df_seed_calc.gross_need[i],             df_seed_calc.calc_qty[i],               df_seed_calc.sku_calc_qty[i],         
                                                  df_seed_calc.sku_calc_qty_bfr_qtlm[i],  df_seed_calc.sku_calc_qty_bfr_sprd[i],  df_seed_calc.alloc_sku_calc_qty[i],     
                                                  df_seed_calc.som_qty[i],                df_seed_calc.alloc_qty[i],              df_seed_calc.status[i],     
                                                  df_seed_calc.error_message_txt[i],      df_seed_calc.create_id[i],              df_seed_calc.create_datetime[i],            
                                                  df_seed_calc.last_update_id[i],         df_seed_calc.last_update_datetime[i],   df_seed_calc.include_inv_in_min_ind[i],            
                                                  df_seed_calc.include_inv_in_max_ind[i], df_seed_calc.sku_spread_qty[i],         df_seed_calc.orig_sku_calc_qty[i],        
                                                  df_seed_calc.wh_on_order_qty[i],        df_seed_calc.wh_on_alloc_qty[i])
                            #status
                            O_status = 9
                            L_insert_calc_item = convert_numpy(L_insert_calc_item)
                            mycursor.execute(Q_ins_calc_item_loc,L_insert_calc_item)
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                        #status
                        O_status = 10
                        #Q_calc_chk(checking if data is inserted)
                        mycursor.execute(Q_calc_chk,(I_alloc,))
                        myresult = mycursor.fetchall()                        
                        if len(myresult)>0:
                            if L_alloc_level=='T':
                                #status
                                O_status = 11
                                mycursor.execute(Q_upd_gross_no_own,(I_alloc,))
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                            if L_alloc_criteria !='F':
                                if L_on_order_commit_date !=None or L_on_order_commit_weeks !=None:
                                    #status
                                    O_status = 12
                                    mycursor.execute(Q_upd_wh_on_ord_qty,(I_alloc,I_alloc,L_commit_date,L_commit_date))
                                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                                    
                                    #status
                                    O_status = 13
                                    mycursor.execute(Q_wh_on_alloc_qty,(I_alloc,L_commit_date))
                                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                    #status
                                    O_status = 14
                                    mycursor.execute(Q_upd_ord_on_alloc_qty,(I_alloc,L_commit_date))
                                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)                                
                                else:
                                    #status
                                    O_status = 16
                                    mycursor.execute(Q_upd_wh_on_ord_qty_else,(I_alloc,I_alloc))
                                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                    #status
                                    O_status = 17
                                    mycursor.execute(Q_wh_on_alloc_qty_else,(I_alloc,))
                                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                    #status
                                    O_status = 18
                                    mycursor.execute(Q_upd_ord_on_alloc_qty_else,(I_alloc,))
                                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                            else:
                                if L_on_order_commit_date !=None or L_on_order_commit_weeks !=None:
                                    #status
                                    O_status = 19
                                    mycursor.execute(Q_upd_wi_wh_on_ord_qty,(I_alloc,I_alloc,L_commit_date,L_commit_date))
                                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                    #status
                                    O_status = 20
                                    mycursor.execute(Q_wi_on_alloc_qty,(I_alloc,L_commit_date))
                                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                    #status
                                    O_status = 21
                                    mycursor.execute(Q_upd_wi_ord_on_alloc_qty,(I_alloc,L_commit_date))
                                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                                
                                else:
                                    #status
                                    O_status = 22
                                    mycursor.execute(Q_upd_wi_on_ord_qty_else,(I_alloc,I_alloc))
                                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                    #status
                                    O_status = 23
                                    mycursor.execute(Q_wi_on_alloc_qty_else,(I_alloc,))
                                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                    #status
                                    O_status = 24
                                    mycursor.execute(Q_upd_wi_ord_on_alloc_qty_else,(I_alloc,))
                                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                            #create a temp table to execute merge on sku_avail_qty.
                            #status
                            O_status = 25
                            mycursor.execute("DROP TEMPORARY TABLE IF EXISTS alloc_calc_item_loc_temp1;")
                            mycursor.execute(Q_cre_temp_sku,(I_alloc,))

                            if L_alloc_criteria=='W':                                
                                #status
                                O_status = 26
                                mycursor.execute(Q_upd_sku_avail_qty)
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                            if L_alloc_criteria!='F':
                                #status
                                O_status = 27
                                mycursor.execute(Q_update_inac_wh,(I_alloc,))
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                            else:
                                #status
                                O_status = 28
                                mycursor.execute(Q_update_inac_wh_wi,(I_alloc,))
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                #status
                                O_status = 28.1
                                mycursor.execute(Q_wi_sku_ownship,(I_alloc,))
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                            #status
                            O_status = 29
                            mycursor.execute(Q_update_sku_calc_qty,(I_alloc,))
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                            #status
                            O_status = 30
                            mycursor.execute(Q_upd_sku_calc_qty_bfr_qtlm,(I_alloc,))
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                            #status
                            O_status = 31
                            #Q_upd_sku_calc_qty_bfr_sprd
                            mycursor.execute(Q_upd_sku_calc_qty_bfr_sprd,(I_alloc,))
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                            #status
                            O_status = 32
                            L_final_check = 0
                            while L_final_check==0:
                                L_final_check = 0
                                df_item_loc_spread = pd.read_sql(Q_fetch_itm_loc_spread,conn,params=(I_alloc,))

                                #status
                                O_status = 33
                                mycursor.execute(Q_del_spread_itm_loc,(I_alloc,))
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                #status
                                O_status = 34
                                if len(df_item_loc_spread)>0:

                                    for i in range(len(df_item_loc_spread)):
                                        L_insert_itm_loc_spread = (df_item_loc_spread.alloc_no[i],                df_item_loc_spread.wh_id[i],     df_item_loc_spread.order_no[i],
                                                                   df_item_loc_spread.source_item[i],             df_item_loc_spread.tran_item[i], df_item_loc_spread.sku_avail_qty[i],
                                                                   df_item_loc_spread.total_tran_sku_calc_qty[i], df_item_loc_spread.som_qty[i],   df_item_loc_spread.no_of_stores[i], 
                                                                   df_item_loc_spread.adj_units[i])   
                                        #status
                                        O_status = 35
                                        L_insert_itm_loc_spread = convert_numpy(L_insert_itm_loc_spread)  
                                        mycursor.execute(Q_ins_spread_itm_loc,L_insert_itm_loc_spread)
                                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                    for i in range(len(df_item_loc_spread)):
                                        #Q_upd_sprd_sku_calc_qty
                                        L_params = (df_item_loc_spread.adj_units[i], df_item_loc_spread.adj_units[i], df_item_loc_spread.adj_units[i], 
                                                    df_item_loc_spread.adj_units[i], I_alloc,                         df_item_loc_spread.source_item[i],
                                                    df_item_loc_spread.order_no[i],  df_item_loc_spread.wh_id[i],     df_item_loc_spread.tran_item[i])

                                        #status
                                        O_status = 36
                                        L_params = convert_numpy(L_params)
                                        mycursor.execute(Q_upd_sprd_sku_calc_qty,L_params)
                                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                    for i in range(len(df_item_loc_spread)):
                                        #status
                                        O_status = 37
                                        mycursor.execute(Q_upd_sprd_orig_sku_calc_qty,(I_alloc,))
                                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                        #status
                                        O_status =38
                                        mycursor.execute(Q_upd_tot_sku_calc,(I_alloc,))
                                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                        #status
                                        O_status = 39
                                        df_spread_rec_loop = pd.read_sql(Q_spread_rec_loop,conn,params=(I_alloc,))
                                        if len(df_spread_rec_loop)>0:
                                            for i in range(len(df_spread_rec_loop)):
                                                L_spread_rec = (df_spread_rec_loop.alloc_no[i],    df_spread_rec_loop.wh_id[i],     df_spread_rec_loop.order_no[i],
                                                                df_spread_rec_loop.source_item[i], df_spread_rec_loop.tran_item[i], 
                                                                df_spread_rec_loop.exact_ind[i])  #df_spread_rec_loop.gross_need[i],df_spread_rec_loop.to_loc[i],commented out for defect CE-1428
                                                
                                                L_spread_rec = convert_numpy(L_spread_rec)                                                
                                                df_total_tran_sku_calc_qty = pd.read_sql(Q_chk_tot_tran_sku_calc_qty,conn,params=(L_spread_rec))

                                                #status
                                                O_status = 40
                                                if len(df_total_tran_sku_calc_qty)>0:
                                                    L_total_tran_sku_calc_qty   = df_total_tran_sku_calc_qty.total_tran_sku_calc_qty[0]
                                                    L_new_calc_qty = L_total_tran_sku_calc_qty - df_spread_rec_loop.sku_avail_qty[i]

                                                    #status
                                                    O_status = 41
                                                    if df_spread_rec_loop.adj_items_priority[i] == 1:
                                                        L_adj_items_priority = 0

                                                    if L_new_calc_qty>0 and (df_spread_rec_loop.adj_items_priority[i] or L_adj_items_priority == 1):
                                                        L_som = df_spread_rec_loop.som_qty[i]
                                                        L_new_calc_qty = np.where(L_som>L_new_calc_qty,L_new_calc_qty,
                                                                                    L_som)

                                                        #status
                                                        O_status = 42
                                                        L_spread_rec = (df_spread_rec_loop.alloc_no[i],    df_spread_rec_loop.wh_id[i],     df_spread_rec_loop.order_no[i],
                                                                        df_spread_rec_loop.source_item[i], df_spread_rec_loop.tran_item[i], df_spread_rec_loop.to_loc[i])

                                                        mycursor.execute(Q_upd_sprd_sku_calc_qty,L_new_calc_qty,L_som,L_som,L_new_calc_qty,L_som,L_som,L_spread_rec)
                                                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                                        #status
                                                        #O_status = 43
                                                        ##Q_upd_sprd_sku_calc_qty1
                                                        L_spread_rec = (df_spread_rec_loop.loc[i, "alloc_no"],    df_spread_rec_loop.loc[i, "wh_id"],     df_spread_rec_loop.loc[i, "order_no"],
                                                                        df_spread_rec_loop.loc[i, "source_item"], df_spread_rec_loop.loc[i, "tran_item"], df_spread_rec_loop.loc[i, "to_loc"])

                                                        mycursor.execute(Q_upd_sprd_sku_calc_qty1,L_new_calc_qty,L_som,L_som,L_new_calc_qty,L_som,L_som,L_spread_rec)
                                                        conn.commit()

                                        #status
                                        O_status=44
                                        df_1 = pd.read_sql(Q_adj_itm_priority,conn,params=(I_alloc,))
                                        df_2 = pd.read_sql(Q_adj_itm_priority1,conn,params=(I_alloc,))
                                        df_adj_itm_priority = pd.concat([df_1, df_2])

                                        if len(df_adj_itm_priority)>0:
                                            L_final_check = 1
                                        #commit
                                        conn.commit()
                                else:
                                    L_final_check = 1

                            #status
                            O_status=45
                            mycursor.execute(Q_chk_inval_ql_data,(I_alloc,))
                            myresult = mycursor.fetchall()
                            if len(myresult)>0:
                                conn.rollback()
                                print('Max Qty Limits cannot be used when using Proportional rule.')
                                conn.cursor().close()
                                return False
                            
                            #status
                            O_status=46
                            mycursor.execute(Q_chk_avail_qty,(I_alloc,))
                            myresult = mycursor.fetchall()
                            L_avail_qty = myresult[0]

                            #status
                            O_status=47
                            if L_alloc_criteria !='F' and L_avail_qty ==0: 
                                #status
                                O_status=48
                                mycursor.execute(Q_no_avail_qty,(I_alloc,))
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                #recalc_ind Y
                                #status
                                O_status=48
                                I_input_data = list()
                                if update_alloc(conn,
                                                O_status,
                                                I_alloc,
                                                None,
                                                None,
                                                'Y',
                                                I_input_data) ==False:
                                    print("Error occured while updating recalc ind: ",O_status)
                                    conn.rollback()
                                    conn.cursor().close()
                                    return False
                                #status
                                O_status = 49
                                print('There is no inventory to allocate.')
                                conn.rollback()
                                conn.cursor().close()
                                return False

                            #status
                            O_status = 50
                            mycursor.execute(Q_del_main_tbl,(I_alloc,))
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                            #status
                            O_status = 51
                            mycursor.execute(Q_ins_main_tbl,(I_alloc,))    #inserting into main table
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                            #status
                            O_status = 52
                            mycursor.execute(Q_calc_chk,(I_alloc,))
                            myresult = mycursor.fetchall()
                            if len(myresult)>0:
                                #calc success
                                #status
                                O_status = 53
                                I_input_data = list()
                                if update_alloc(conn,
                                                O_status,
                                                I_alloc,
                                                None,
                                                None,
                                                'N',
                                                I_input_data) ==False:
                                    print("Error occured while updating recalc ind: ",O_status)
                                    return False
                                
                    else:
                        #status
                        O_status = 54
                        print("no rows to process for calculation")
                        conn.rollback()
                        conn.cursor().close()
                        return False

                    conn.commit()
                    conn.cursor().close()
                    return True

    except Exception as error:
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while creating temporary tables: ", error)
        elif O_status==2:
            print(L_func_name,":",O_status,":","Exception occured while reseting quantity limits: ", error)
        elif O_status>=3 and O_status<=7:
            print(L_func_name,":",O_status,":","Exception occured while retrieving quantity limits: ", error)
        elif O_status>=8 and O_status<=9:
            print(L_func_name,":",O_status,":","Exception occured while inserting data into temp table: ", error)
        elif O_status<=10 and O_status>=24:
            print(L_func_name,":",O_status,":","Exception occured while merging future fulfill quantity : ", error)
        elif O_status<=29 and O_status>=44:
            print(L_func_name,":",O_status,":","Exception occured while merging spread quantity: ", error)
        elif O_status==45:
            print(L_func_name,":",O_status,":","Exception occured while checking for quantity limits usage: ", error)
        elif O_status>=46 and O_status<=49:
            print(L_func_name,":",O_status,":","Exception occured while updating recalc indicator: ", error)
        elif O_status>=50 and O_status<=51:
            print(L_func_name,":",O_status,":","Exception occured while inserting data into physical table: ", error)
        elif O_status>=52 and O_status<=53:
            print(L_func_name,":",O_status,":","Exception occured while updating recalc indicator: ", error)
        else:
            print("Exception occured in: ",L_func_name,error)
        conn.rollback()
        conn.cursor().close()
        return False