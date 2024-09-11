from  ..INVENTORY_SETUP.update_alloc_ext import *
from  ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
import pandas as pd
import numpy as np
import yaml

#############################################################
# Created By - Priyanshu Pandey                             #                
# File Name - calculate_whatif.py                           #
# Purpose - calculate what if allocation                    #
#############################################################

def wif_calculate(conn
                  ,I_alloc
                  ,O_status):
    L_func_name ="wif_calculate"
    O_status           = 0
    L_alloc_criteria   = None
    L_wh_store_rel_ind = None
    L_alloc_level      = None
    L_total_calc_avail_qty = 0
    L_total_calc_avail_qty = 0
    L_final_check = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculate_whatif_queries.yaml') as fh:
            queries                      = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_get_rule_rec               = queries['calculate_whatif']['Q_get_rule_rec']
            Q_chk_alloc                  = queries['calculate_whatif']['Q_chk_alloc']
            Q_get_calc_itm_loc           = queries['calculate_whatif']['Q_get_calc_itm_loc']
            Q_fetch_itm_loc_spread       = queries['calculate_whatif']['Q_fetch_itm_loc_spread']
            Q_chk_inval_ql_data_loop     = queries['calculate_whatif']['Q_chk_inval_ql_data_loop']
            Q_chk_tot_tran_sku_calc_qty  = queries['calculate_whatif']['Q_chk_tot_tran_sku_calc_qty']
            Q_adj_itm_priority           = queries['calculate_whatif']['Q_adj_itm_priority']
            Q_chk_ql_limits              = queries['calculate_whatif']['Q_chk_ql_limits']
            Q_mrg_sku_avail_sku_own      = queries['calculate_whatif']['Q_mrg_sku_avail_sku_own']
            Q_rec_1_loop                 = queries['calculate_whatif']['Q_rec_1_loop']
            Q_rec_2_loop                 = queries['calculate_whatif']['Q_rec_2_loop']
            Q_rec2_upd_sku_avail         = queries['calculate_whatif']['Q_rec2_upd_sku_avail']
            Q_upd_sku_calc               = queries['calculate_whatif']['Q_upd_sku_calc']
            Q_bfr_qtlm_calc_qty          = queries['calculate_whatif']['Q_bfr_qtlm_calc_qty']
            Q_bfr_qty_sprd               = queries['calculate_whatif']['Q_bfr_qty_sprd']
            Q_del_itm_loc_sprd           = queries['calculate_whatif']['Q_del_itm_loc_sprd']
            Q_ins_spread_itm_loc         = queries['calculate_whatif']['Q_ins_spread_itm_loc']
            Q_upd_sprd_sku_calc_qty      = queries['calculate_whatif']['Q_upd_sprd_sku_calc_qty']
            Q_upd_sprd_orig_sku_calc_qty = queries['calculate_whatif']['Q_upd_sprd_orig_sku_calc_qty']
            Q_upd_tot_sku_calc           = queries['calculate_whatif']['Q_upd_tot_sku_calc']
            Q_spread_rec_loop            = queries['calculate_whatif']['Q_spread_rec_loop']
            Q_sprd_sku_orig_sku          = queries['calculate_whatif']['Q_sprd_sku_orig_sku']
            Q_upd_sku_calc_org_aftr_sprd = queries['calculate_whatif']['Q_upd_sku_calc_org_aftr_sprd']
            Q_ins_main_tbl               = queries['calculate_whatif']['Q_ins_main_tbl']
            Q_del_main_tbl               = queries['calculate_whatif']['Q_del_main_tbl']
            Q_calc_chk                   = queries['calculate_whatif']['Q_calc_chk']
            
            #status
            O_status = 1
            mycursor = conn.cursor()

            #df_alloc_head = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))
            #if len(df_alloc_head)>0:
            #    L_alloc_criteria   = df_alloc_head["alloc_criteria"]
            #    L_wh_store_rel_ind = df_alloc_head["wh_store_rel_ind"]
            #    L_alloc_level      = df_alloc_head["alloc_level"]

            #df_alloc_rule = pd.read_sql(Q_get_rule_rec,conn,params=(I_alloc,))
            #if len(df_alloc_rule)>0:
            #    L_size_prf_ind = df_alloc_rule["SIZE_PROFILE_IND"]

            #df_chk_ql = pd.read_sql(Q_chk_ql_limits,conn,params=(I_alloc,))
            #if len(df_chk_ql)>0:
            #    L_ql_chk = df_chk_ql["ql_chk"]

            #status
            O_status = 2
            mycursor.execute(Q_mrg_sku_avail_sku_own,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            df_rec_1_loop = pd.read_sql(Q_rec_1_loop,conn,params=(I_alloc,))
            
            #status
            O_status = 3
            for rec1 in range(len(df_rec_1_loop)):
                L_total_calc_avail_qty = df_rec_1_loop["total_calc_avail_qty"][rec1]
                L_total_avail_qty      = df_rec_1_loop["total_avail_qty"][rec1]
                L_source_item          = df_rec_1_loop["source_item"][rec1]
                L_diff_id              = df_rec_1_loop["diff_id"][rec1]

                if L_total_calc_avail_qty>L_total_avail_qty:
                    L_adj_qty     = L_total_calc_avail_qty - L_total_avail_qty
                    df_rec_2_loop = pd.read_sql(Q_rec_2_loop,conn,params=(I_alloc,L_source_item,L_diff_id))
                    for rec2 in range(len(df_rec_2_loop)):
                        L_tran_item = df_rec_2_loop["tran_item"][rec2]
                        #status
                        O_status = 4
                        mycursor.execute(Q_rec2_upd_sku_avail,(L_adj_qty,L_tran_item,I_alloc))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 5
            mycursor.execute(Q_upd_sku_calc,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 6
            mycursor.execute(Q_bfr_qtlm_calc_qty,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 7
            mycursor.execute(Q_bfr_qty_sprd,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 8
            mycursor.execute(Q_del_itm_loc_sprd,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 9
            df_item_loc_spread = pd.read_sql(Q_fetch_itm_loc_spread,conn,params=(I_alloc,))

            while L_final_check == 0:
                L_final_check = 0
                #status
                O_status = 10
                mycursor.execute(Q_del_itm_loc_sprd,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                if len(df_item_loc_spread)>0:
                    for i in range(len(df_item_loc_spread)):
                        L_insert_itm_loc_spread = (df_item_loc_spread.alloc_no[i],                 df_item_loc_spread.wh_id[i],     df_item_loc_spread.order_no[i],
                                                   df_item_loc_spread.source_item[i],              df_item_loc_spread.tran_item[i], df_item_loc_spread.sku_avail_qty[i],
                                                   df_item_loc_spread.total_tran_sku_calc_qty[i],  df_item_loc_spread.som_qty[i],   df_item_loc_spread.no_of_stores[i], 
                                                   df_item_loc_spread.adj_units[i])   
                        #Q_ins_spread_itm_loc
                        L_insert_itm_loc_spread = convert_numpy(L_insert_itm_loc_spread)  
                        #status
                        O_status = 11
                        mycursor.execute(Q_ins_spread_itm_loc,L_insert_itm_loc_spread)
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                    for i in range(len(df_item_loc_spread)):
                        #Q_upd_sprd_sku_calc_qty
                        L_params = (df_item_loc_spread.adj_units[i], df_item_loc_spread.adj_units[i],
                                    df_item_loc_spread.adj_units[i], df_item_loc_spread.adj_units[i],
                                    I_alloc,                         df_item_loc_spread.source_item[i],
                                    df_item_loc_spread.order_no[i],  df_item_loc_spread.wh_id[i],   df_item_loc_spread.tran_item[i])

                        L_params = convert_numpy(L_params)
                        #status
                        O_status = 12
                        mycursor.execute(Q_upd_sprd_sku_calc_qty,L_params)
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                    for i in range(len(df_item_loc_spread)):
                        #status
                        O_status = 13
                        mycursor.execute(Q_upd_sprd_orig_sku_calc_qty,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                        #status
                        O_status = 14
                        mycursor.execute("DROP TABLE IF EXISTS alloc_item_loc_spread_temp2;")
                        mycursor.execute("""CREATE TABLE IF NOT EXISTS alloc_item_loc_spread_temp2
                                            SELECT * FROM alloc_item_loc_spread_temp;""")

                        mycursor.execute(Q_upd_tot_sku_calc,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                        #status
                        O_status = 15
                        df_spread_rec_loop = pd.read_sql(Q_spread_rec_loop,conn,params=(I_alloc,))

                        if len(df_spread_rec_loop)>0:
                            #status
                            O_status = 16
                            for i in range(len(df_spread_rec_loop)):
                                L_spread_rec = (df_spread_rec_loop.alloc_no[i],    df_spread_rec_loop.exact_ind[i],   df_spread_rec_loop.wh_id[i],     
                                                df_spread_rec_loop.order_no[i],    df_spread_rec_loop.source_item[i], df_spread_rec_loop.tran_item[i])  
                                L_spread_rec = convert_numpy(L_spread_rec)
                                #status
                                O_status = 17
                                df_total_tran_sku_calc_qty = pd.read_sql(Q_chk_tot_tran_sku_calc_qty,conn,params=(L_spread_rec))

                                #status
                                O_status = 18
                                if len(df_total_tran_sku_calc_qty)>0:
                                    L_total_tran_sku_calc_qty   = df_total_tran_sku_calc_qty.total_tran_sku_calc_qty[0]
                                    L_new_calc_qty = L_total_tran_sku_calc_qty - df_spread_rec_loop.sku_avail_qty[i]

                                    #status
                                    O_status = 19
                                    if df_spread_rec_loop.adj_items_priority[i] == 1:
                                                        L_adj_items_priority = 0

                                    #status
                                    O_status = 20
                                    if L_new_calc_qty>0 and (df_spread_rec_loop.adj_items_priority[i] or L_adj_items_priority == 1):
                                        L_som = df_spread_rec_loop.som_qty[i]
                                        L_som = int(L_som)
                                        #L_new_calc_qty = np.where(L_som>L_new_calc_qty,L_new_calc_qty,
                                        #                            L_som)
                                        L_new_calc_qty = L_new_calc_qty if L_new_calc_qty>L_som else L_som
                                        L_new_calc_qty = int(L_new_calc_qty)

                                        L_spread_rec = (L_new_calc_qty, L_som, L_som, L_new_calc_qty, L_som, L_som,
                                                        df_spread_rec_loop.alloc_no[i],    df_spread_rec_loop.wh_id[i],     df_spread_rec_loop.order_no[i],
                                                        df_spread_rec_loop.source_item[i], df_spread_rec_loop.tran_item[i], df_spread_rec_loop.to_loc[i])
                                        #status
                                        O_status = 21
                                        L_spread_rec = convert_numpy(L_spread_rec)
                                        mycursor.execute(Q_sprd_sku_orig_sku,L_spread_rec)
                                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                        #status
                        O_status = 22
                        mycursor.execute("DROP TABLE IF EXISTS alloc_calc_item_loc_temp2;")
                        mycursor.execute("""CREATE TABLE IF NOT EXISTS alloc_calc_item_loc_temp2
                                            SELECT * FROM alloc_calc_item_loc_temp;""")
                        mycursor.execute(Q_adj_itm_priority,(I_alloc,I_alloc))
                        myresult = mycursor.fetchall()
                        print(myresult)
                        if len(myresult)==1:
                            L_final_check = 1
                            break
                        #commit
                        #conn.commit()

                else:
                    #status
                    O_status = 23
                    L_final_check = 1

            #status
            O_status = 24
            mycursor.execute(Q_chk_inval_ql_data_loop,(I_alloc,))
            myresult = mycursor.fetchall()
            #status
            O_status = 25
            if len(myresult)>0:
                #throw exception
                O_status = 'Max Qty Limits cannot be used when using Proportional rule.'
                conn.rollback()
                print( O_status)
                conn.cursor().close()
                return False, L_func_name+":"+str(O_status)+ ": Max Qty Limits cannot be used when using Proportional rule."
            #status
            O_status = 26
            mycursor.execute(Q_upd_sku_calc_org_aftr_sprd,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 27
            mycursor.execute(Q_del_main_tbl,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 28
            mycursor.execute(Q_ins_main_tbl,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 29
            df_calc_chk = pd.read_sql(Q_calc_chk,conn,params=(I_alloc,))

            #status
            O_status = 30
            if len(df_calc_chk)>0:
                #calc success
                I_input_data = list()
                check_UA, err_msg = update_alloc(conn,
                                                O_status,
                                                I_alloc,
                                                None,
                                                None,
                                                'N',
                                                I_input_data)
                if check_UA ==False:
                    #status
                    O_status = 31
                    print(O_status)
                    conn.cursor().close()
                    return False, L_func_name+":"+str(O_status)+ ": "+ str(err_msg)
            else:
                I_input_data = list()
                check_UA, err_msg = update_alloc(conn,
                                                O_status,
                                                I_alloc,
                                                None,
                                                None,
                                                'Y',
                                                I_input_data)
                if check_UA ==False:
                    #status
                    O_status = 32
                    print(O_status)
                    conn.cursor().close()
                    return False, L_func_name+":"+str(O_status)+ ": "+ str(err_msg)

            conn.commit()
            conn.cursor().close()
            return True, ""
    except Exception as error:
        err_return = ""
        if O_status<=4:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating sku_avail_qty: "+ str(error)
        elif O_status==5:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating sku_calc_qty: "+ str(error)
        elif O_status==6:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating sku_calc_qty and sku_calc_qty_bfr_qtlm: "+ str(error)
        elif O_status==7:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating sku_calc_qty and sku_calc_qty_bfr_sprd: "+ str(error)
        elif O_status<=8 and O_status>=23:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while processing spread item quantities: "+ str(error)
        elif O_status<=24 and O_status>=25:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while processing setup_location: "+ str(error)
        elif O_status==26:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating sku_calc_qty after spreading item qty: "+ str(error)
        elif O_status>=27 and O_status<=29:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while processing data to main table: "+ str(error)
        elif O_status>=30 and O_status<=32:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating recalc_ind: "+ str(error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured:  "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return