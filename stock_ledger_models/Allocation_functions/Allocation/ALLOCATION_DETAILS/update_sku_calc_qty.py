from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
import pandas as pd
import numpy as np
import yaml

def update_alloc_detail_diff_qty(conn,
                                 I_alloc_no,             
                                 I_wh_id,                
                                 I_item_id,              
                                 I_diff_id,              
                                 I_order_no,             
                                 I_location,             
                                 I_adj_qty,  
                                 O_status):
    L_func_name = "update_alloc_detail_diff_qty"
    no_data = list()
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/update_sku_calc_qty.yaml') as cad:  
            queries                        = yaml.load(cad,Loader=yaml.SafeLoader)
            Q_chck_alloc_level             = queries['update_calc_qty']['Q_chck_alloc_level']
            Q_round_adj_qty                = queries['update_calc_qty']['Q_round_adj_qty']
            Q_upd_sku_calc_qty             = queries['update_calc_qty']['Q_upd_sku_calc_qty']
            Q_chck_total_sku_calc_qty      = queries['update_calc_qty']['Q_chck_total_sku_calc_qty']
            Q_cur_spread_items             = queries['update_calc_qty']['Q_cur_spread_items']
            Q_upd_sprd_sku_calc_qty        = queries['update_calc_qty']['Q_upd_sprd_sku_calc_qty']
            Q_rec_spread                   = queries['update_calc_qty']['Q_rec_spread']
            Q_chck_total_tran_sku_cal_qty  = queries['update_calc_qty']['Q_chck_total_tran_sku_cal_qty']
            Q_upd_new_sku_calc_qty         = queries['update_calc_qty']['Q_upd_new_sku_calc_qty']
            Q_chck_open_adj_qty            = queries['update_calc_qty']['Q_chck_open_adj_qty']
            Q_upd_sku_sprd_qty             = queries['update_calc_qty']['Q_upd_sku_sprd_qty']
            Q_alloc_qty                    = queries['update_calc_qty']['Q_alloc_qty']
            Q_diff_alloc_qty               = queries['update_calc_qty']['Q_diff_alloc_qty']
            Q_diff_tot_alloc_qty           = queries['update_calc_qty']['Q_diff_tot_alloc_qty']

            #pack
            Q_chck_pack_data               = queries['update_calc_qty']['Q_chck_pack_data']

            mycursor = conn.cursor()

            df_chck_alloc_level = pd.read_sql(Q_chck_alloc_level,conn,params=(I_alloc_no,))
            if len(df_chck_alloc_level) > 0:
                L_chck_alloc_level = df_chck_alloc_level.alloc_level[0]
                print('L_chck_alloc_level:::::::',L_chck_alloc_level)

            df_chck_pack_data  = pd.read_sql(Q_chck_pack_data,conn,params=(I_alloc_no,I_item_id,I_location))
            if L_chck_alloc_level == 'T' and len(df_chck_pack_data) == 0:

                return no_data,True
            else:
                L_adj_qty = pd.read_sql(Q_round_adj_qty,conn,params=(I_adj_qty,))
                I_adj_qty = L_adj_qty.adj_qty[0]
                print('I_adj_qty::::::',I_adj_qty)
                
                if I_adj_qty == 0:
                    O_status = 1
                    mycursor.execute(Q_upd_sku_calc_qty,(I_alloc_no,I_item_id,I_diff_id,I_order_no,I_wh_id,I_location))
                    print('values:::',I_alloc_no,I_item_id,I_diff_id,I_order_no,I_wh_id,I_location)
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                else:
                    #RC Q_upd_sku_calc_qty
                    print('values:::',I_alloc_no,I_item_id,I_diff_id,I_order_no,I_wh_id,I_location)
                    mycursor.execute(Q_upd_sku_calc_qty,(I_alloc_no,I_item_id,I_diff_id,I_order_no,I_wh_id,I_location))
                    #Q_chck_total_sku_calc_qty fetch sku_calc_qty,avail_qty,soh
                    L_chck_total_sku_calc_qty = pd.read_sql(Q_chck_total_sku_calc_qty,conn,params=(I_location,I_location,I_alloc_no,I_wh_id,I_order_no,I_item_id,I_diff_id))
                    print('L_chck_total_sku_calc_qty',L_chck_total_sku_calc_qty)

                    if len(L_chck_total_sku_calc_qty) > 0:
                        L_total_tran_sku_calc_qty = L_chck_total_sku_calc_qty.total_tran_sku_calc_qty[0]
                        L_avail_qty               = L_chck_total_sku_calc_qty.avail_qty[0]
                        L_soh                     = L_chck_total_sku_calc_qty.soh[0]
                        L_net_need_ind            = L_chck_total_sku_calc_qty.net_need_ind[0]
                        print('values of chck_total_sku_calc_qty',L_total_tran_sku_calc_qty,L_avail_qty,L_soh,L_net_need_ind)

                        print('input of if values',I_adj_qty, L_total_tran_sku_calc_qty, L_avail_qty)
                    if (I_adj_qty + L_total_tran_sku_calc_qty) > L_avail_qty:
                        L_adj_qty = L_avail_qty - L_total_tran_sku_calc_qty
                    else:
                        L_adj_qty = I_adj_qty 
                        print("L_adj_qty::::::",L_adj_qty)
                    
                                                      #Q_cur_spread_items fetch adj_qty,sprd_qty
                    df_cur_spread_items = pd.read_sql(Q_cur_spread_items,conn,params=(L_chck_alloc_level,L_chck_alloc_level,L_adj_qty,
                                                                                      I_alloc_no,I_item_id,I_diff_id,I_order_no,I_wh_id,
                                                                                      I_alloc_no,I_item_id,I_diff_id,I_order_no,I_wh_id,I_location))

                    if len(df_cur_spread_items) > 0:
                        for i in range(len(df_cur_spread_items)):
                            L_cur_spread_items = (df_cur_spread_items.adj_units[i],     df_cur_spread_items.adj_units[i],
                                                  df_cur_spread_items.sku_spread_qty[i],df_cur_spread_items.alloc_no[i],
                                                  df_cur_spread_items.source_item[i],   df_cur_spread_items.order_no[i],
                                                  df_cur_spread_items.wh_id[i],df_cur_spread_items.tran_item[i],df_cur_spread_items.to_loc[i])
                            L_cur_spread_items = convert_numpy(L_cur_spread_items)
                            
                            
                            O_status = 2     #Q_upd_sprd_sku_calc_qty updates sku_calc_qty with adj_qty's
                            mycursor.execute(Q_upd_sprd_sku_calc_qty,(L_cur_spread_items))
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                            

                            L_params = (I_alloc_no,I_item_id,I_diff_id,I_order_no,I_wh_id,
                                        I_alloc_no,I_item_id,I_diff_id,I_order_no,I_wh_id,I_location,
                                        I_alloc_no,I_item_id,I_diff_id,I_order_no,I_wh_id,
                                        I_alloc_no,I_item_id,I_diff_id,I_order_no,I_wh_id,I_location)
                            L_params = convert_numpy(L_params)

                                                        #Q_rec_spread fetch records if updated sku_calc_qty>avail_qty&other qtys
                            df_rec_spread = pd.read_sql(Q_rec_spread,conn,params=(L_params))
                            if len(df_rec_spread) > 0:
                                for i in range(len(df_rec_spread)):
                                    L_rec_spread = (df_rec_spread.alloc_no[i], df_rec_spread.source_item[i], df_rec_spread.exact_ind[i],
                                                    df_rec_spread.diff_id[i],  df_rec_spread.order_no[i],    df_rec_spread.wh_id[i], 
                                                    df_rec_spread.alloc_no[i], df_rec_spread.exact_ind[i],   df_rec_spread.wh_id[i], 
                                                    df_rec_spread.order_no[i], df_rec_spread.source_item[i], df_rec_spread.diff_id[i], 
                                                    df_rec_spread.to_loc[i],   df_rec_spread.tran_item[i])

                                    L_rec_spread = convert_numpy(L_rec_spread)
                                                                     #Q_chck_total_tran_sku_cal_qty fetch new_calc_qty
                                    df_chck_total_tran_sku_cal_qty = pd.read_sql(Q_chck_total_tran_sku_cal_qty,conn,params=(L_rec_spread))

                                    if len(df_chck_total_tran_sku_cal_qty) > 0:
                                        L_new_calc_qty = df_chck_total_tran_sku_cal_qty.new_total_sku_calc_qty[0]
                                        L_som_qty      = df_rec_spread.som_qty[i]

                                        if L_new_calc_qty > 0:

                                            L_new_calc_qty = L_new_calc_qty if L_som_qty>L_new_calc_qty else L_som_qty 
                                            #L_new_calc_qty = np.where(L_som_qty>L_new_calc_qty,L_new_calc_qty,L_som_qty) changed for defect CE-1931

                                            L_calc_qty_params = (L_new_calc_qty,L_som_qty,L_som_qty,df_rec_spread.alloc_no[i],df_rec_spread.source_item[i],df_rec_spread.tran_item[i],  
                                                                 df_rec_spread.diff_id[i], df_rec_spread.wh_id[i],df_rec_spread.to_loc[i])
                                            L_calc_qty_params = convert_numpy(L_calc_qty_params)

                                            O_status = 3
                                                             #Q_upd_new_sku_calc_qty update sku_calc_qty with new qty
                                            mycursor.execute(Q_upd_new_sku_calc_qty,L_calc_qty_params)
                                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                            L_params_qty = (I_alloc_no,I_item_id,I_diff_id,I_order_no,I_wh_id,I_location)
                            L_params_qty = convert_numpy(L_params_qty)
                            #RC to  remove Q_chck_open_adj_qty
                                                     #Q_cur_spread_items fetch records to update sku_spread_qty
                    
                    df_cur_spread_items = pd.read_sql(Q_cur_spread_items,conn,params=(L_chck_alloc_level,L_chck_alloc_level,L_adj_qty,
                                                                                      I_alloc_no,I_item_id,I_diff_id,I_order_no,I_wh_id,
                                                                                      I_alloc_no,I_item_id,I_diff_id,I_order_no,I_wh_id,I_location))
                    if len(df_cur_spread_items) > 0:
                        #RC to put for loop
                        for i in range(len(df_cur_spread_items)):
                            L_sprd_params = (df_cur_spread_items.alloc_no[i],df_cur_spread_items.source_item[i],df_cur_spread_items.order_no[i],
                                             df_cur_spread_items.wh_id[i],   df_cur_spread_items.tran_item[i],  df_cur_spread_items.to_loc[i])

                            L_sprd_params = convert_numpy(L_sprd_params)
                            print(L_sprd_params)
                            O_status = 4     #Q_upd_sku_sprd_qty updates sku_spread_qty&alloc_sku_calc_qty
                            mycursor.execute(Q_upd_sku_sprd_qty,(L_sprd_params))
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)                      
                                          
                df_alloc_qty = pd.read_sql(Q_alloc_qty,conn,params=(I_alloc_no,I_item_id,I_diff_id,I_wh_id,I_order_no,I_location))
                print(df_alloc_qty,"df_alloc_qty")
                O_alloc_qty  = df_alloc_qty.calc_qty[0]
                print(O_alloc_qty,"O_alloc_qty")
                                    
                                        #Q_diff_alloc_qty for middle block
                df_diff_alloc_qty = pd.read_sql(Q_diff_alloc_qty,conn,params=(I_alloc_no,I_item_id,I_diff_id,I_wh_id,I_order_no))
                O_diff_alloc_qty  = df_diff_alloc_qty.calc_qty[0]

                                          #Q_diff_tot_alloc_qty for top block
                df_diff_tot_alloc_qty = pd.read_sql(Q_diff_tot_alloc_qty,conn,params=(I_alloc_no,I_item_id,I_diff_id))
                O_diff_tot_alloc_qty  = df_diff_tot_alloc_qty.calc_qty[0]     
                           
                return [O_diff_tot_alloc_qty,O_diff_alloc_qty,O_alloc_qty],"" #RC

    except Exception as error:
        err_return = ""
        if O_status==1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating sku_calc_qty: "+ str(error)
        elif O_status==2:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating sprd_sku_calc_qty: "+ str(error)
        elif O_status==3:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating new_calc_qty: "+ str(error)
        elif O_status==4:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating sku_sprd_qty: "+ str(error)
        else: #RC
            err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(error)
        print(err_return)
        conn.rollback()
        return no_data,err_return #RC