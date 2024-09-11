from ..GLOBAL_FILES.null_handler import null_py
import pandas as pd
import yaml
import numpy as np

##################################################################################
#Created By - Naveen/Priyanshu                                                   #
#File Name  - setup_alloc_details.py                                             #
#Purpose    - All functions related to allocation details screen                 #
##################################################################################

def retreive_alloc_details(conn,I_alloc_no):
    O_status = 0
    L_fun = "retreive_alloc_details"
    no_data = list()
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_alloc_details_queries.yaml') as fh:
            queries                      = yaml.load(fh, Loader=yaml.SafeLoader)
            C_temp_tbl1                  = queries['retreive_alloc_details']['C_temp_tbl1']
            C_temp_tbl2                  = queries['retreive_alloc_details']['C_temp_tbl2']
            C_temp_tbl3                  = queries['retreive_alloc_details']['C_temp_tbl3']
            C_temp_tbl4                  = queries['retreive_alloc_details']['C_temp_tbl4']
            C_alloc_level                = queries['retreive_alloc_details']['C_alloc_level']
            #C_alloc_criteria            = queries['retreive_alloc_details']['C_alloc_criteria']              #Changes by Shubham
            L_del_1                      = queries['retreive_alloc_details']['L_del_1']
            L_del_2                      = queries['retreive_alloc_details']['L_del_2']
            L_del_3                      = queries['retreive_alloc_details']['L_del_3']
            L_del_4                      = queries['retreive_alloc_details']['L_del_4']
            L_ins_1                      = queries['retreive_alloc_details']['L_ins_1']
            L_ins_2                      = queries['retreive_alloc_details']['L_ins_2']
            L_ins_2_style                = queries['retreive_alloc_details']['L_ins_2_style']
            L_ins_4                      = queries['retreive_alloc_details']['L_ins_4']
            L_ins_5                      = queries['retreive_alloc_details']['L_ins_5']
            Q_create_item_details_temp_1 = queries['retreive_alloc_details']['Q_create_item_details_temp_1']   #Changes by Shubham
            Q_drop_item_details_temp_1   = queries['retreive_alloc_details']['Q_drop_item_details_temp_1']     #Changes by Shubham
            L_mer_1                      = queries['retreive_alloc_details']['L_mer_1']
            L_mer_2                      = queries['retreive_alloc_details']['L_mer_2']
            L_mer_1_style                = queries['retreive_alloc_details']['L_mer_1_style']                  #Changes by Shubham
            L_mer_2_style                = queries['retreive_alloc_details']['L_mer_2_style']                  #Changes by Shubham
            L_mer_5                      = queries['retreive_alloc_details']['L_mer_5']
            L_mer_5_style                = queries['retreive_alloc_details']['L_mer_5_style']                  #Changes by Shubham
            L_mer_7                      = queries['retreive_alloc_details']['L_mer_7']
            Q_item_hdr_data              = queries['retreive_alloc_details']['Q_item_hdr_data']
            Q_item_loc_data              = queries['retreive_alloc_details']['Q_item_loc_data']
            Q_item_dtls_data             = queries['retreive_alloc_details']['Q_item_dtls_data']

            mycursor = conn.cursor()
            print("Inside Function :",L_fun)       #Changes by Shubham
                                   
            #status
            O_status = 1 
            mycursor.execute(C_temp_tbl1)
            mycursor.execute(C_temp_tbl2)
            mycursor.execute(C_temp_tbl3)
            mycursor.execute(C_temp_tbl4)
            #status
            O_status = 2
            df_alloc_level = pd.read_sql(C_alloc_level,conn,params=(I_alloc_no,))
            L_alloc_level = df_alloc_level.alloc_level[0]
            L_alloc_criteria = df_alloc_level.alloc_criteria[0]   #Changes by Shubham
            #Changes by Shubham Start#
            # O_status = 3
            # df_alloc_criteria = pd.read_sql(C_alloc_criteria,conn,params=(I_alloc_no,))
            # L_alloc_criteria = df_alloc_criteria.alloc_criteria[0]
            #Changes by Shubham End#
            #status
            O_status = 4
            mycursor.execute(L_del_1,(I_alloc_no,))
            print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)  
            #status
            O_status = 5
            mycursor.execute(L_del_2,(I_alloc_no,))
            print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 6
            mycursor.execute(L_del_3,(I_alloc_no,))
            print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 7
            mycursor.execute(L_ins_1,(I_alloc_no,))
            print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 8
            mycursor.execute(L_del_4,(I_alloc_no,))
            print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            if L_alloc_level == 'T':
                #status
                O_status = 9
                mycursor.execute(L_ins_2,(I_alloc_no,I_alloc_no,I_alloc_no,I_alloc_no))
                print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #Changes by Shubham Start#
            else:
                O_status = 9.1
                mycursor.execute(L_ins_2_style,(I_alloc_no,I_alloc_no,I_alloc_no,I_alloc_no))
                print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)            
            #Changes by Shubham End#
            if null_py(L_alloc_criteria,'W') == 'F':
                if L_alloc_level == 'T':
                    #status
                    O_status = 10
                    mycursor.execute(Q_create_item_details_temp_1,(I_alloc_no,))                    
                    mycursor.execute(L_mer_1,(I_alloc_no,I_alloc_no))
                    print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_item_details_temp_1)                      #Changes by Shubham#
                    #status
                    O_status = 11
                    mycursor.execute(Q_create_item_details_temp_1,(I_alloc_no,))
                    mycursor.execute(L_mer_2,(I_alloc_no,I_alloc_no,I_alloc_no))
                    print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_item_details_temp_1)                       #Changes by Shubham#
                #Changes by Shubham Start#
                else:
                    #status
                    O_status = 10.1
                    mycursor.execute(Q_create_item_details_temp_1,(I_alloc_no,))                    
                    mycursor.execute(L_mer_1_style,(I_alloc_no,I_alloc_no))
                    print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_item_details_temp_1)  
                    #status
                    O_status = 11.1
                    mycursor.execute(Q_create_item_details_temp_1,(I_alloc_no,))
                    mycursor.execute(L_mer_2_style,(I_alloc_no,I_alloc_no,I_alloc_no))
                    print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_item_details_temp_1)  
                #Changes by Shubham End#
            else:
                if L_alloc_level == 'T':
                    #status
                    O_status = 12
                    mycursor.execute(Q_create_item_details_temp_1,(I_alloc_no,))
                    mycursor.execute(L_mer_5,(I_alloc_no,))
                    print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_item_details_temp_1)                    #Changes by Shubham#
                #Changes by Shubham Start#
                else:
                    O_status = 12.1
                    mycursor.execute(Q_create_item_details_temp_1,(I_alloc_no,))
                    mycursor.execute(L_mer_5_style,(I_alloc_no,))
                    print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_item_details_temp_1)  
                #Changes by Shubham End#
            #status
            O_status = 13
            mycursor.execute(L_ins_4,(I_alloc_no,))
            print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 14
            mycursor.execute(L_ins_5,(I_alloc_no,))
            print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 15
            mycursor.execute(L_mer_7,(I_alloc_no,))
            print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            df_item_hdr_data  =  pd.read_sql(Q_item_hdr_data,conn,params=(I_alloc_no,))
            df_item_loc_data  =  pd.read_sql(Q_item_loc_data,conn,params=(I_alloc_no,))
            df_item_dtls_data =  pd.read_sql(Q_item_dtls_data,conn,params=(I_alloc_no,))
            
            conn.commit()
            conn.cursor().close()
            return [df_item_hdr_data,df_item_loc_data,df_item_dtls_data],""

    #except Exception as error:
    except Exception as error:
        err_return = ""
        if O_status==1:
            err_return = L_fun+":"+str(O_status)+": Exception raised during creation of temporary tables: "+ str(error)
        elif O_status==2 or O_status==3:
            err_return = L_fun+":"+str(O_status)+": Exception raised during execution of cursor queries for alloc no: "+ str(error)
        elif O_status==4 or O_status==5 or O_status==6:
            err_return = L_fun+":"+str(O_status)+": Exception raised during execution of delete queries for alloc no: "+ str(error)
        elif O_status==7:
            err_return = L_fun+":"+str(O_status)+": Exception raised during inserting data into alloc_item_loc_dtls_prv_temp table for alloc no: "+ str(error)
        elif O_status==8:
            err_return = L_fun+":"+str(O_status)+": Exception raised during deleting data from alloc_item_loc_details_temp table for alloc no: "+ str(error)
        elif O_status==9:
            err_return = L_fun+":"+str(O_status)+": Exception raised during inserting data into alloc_item_details_temp table for alloc no: "+ str(error)
        elif O_status==10 or O_status==11 or O_status==12:
            err_return = L_fun+":"+str(O_status)+": Exception raised during execution of merge queries for alloc no: "+ str(error)
        elif O_status==13 or O_status==14:
            err_return = L_fun+":"+str(O_status)+": Exception raised during inserting data into alloc_item_header_temp and alloc_item_loc_details_temp table for alloc no: "+ str(error)
        elif O_status==15:
            err_return = L_fun+":"+str(O_status)+": Exception raised during updating data in alloc_item_loc_details_temp table for alloc no: "+ str(error)
        else:
            err_return = L_fun+":"+str(O_status)+": Exception Occured: "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return no_data, err_return 
#---------------------------------------------
# Function for spread allocation loction
#---------------------------------------------
def spread_alloc_loc_dtl (conn
                          ,I_alloc
                          ,O_status):
    L_func_name ="spread_alloc_loc_dtl"
    O_status = 0
    df_loop_sprd = pd.DataFrame()
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/spread_alloc_loc_queries.yaml') as fh:
            queries                    = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_get_spread_item          = queries['spread_alloc_loc_dtl']['Q_get_spread_item']
            Q_chk_tot_tran_sku_cal_qty = queries['spread_alloc_loc_dtl']['Q_chk_tot_tran_sku_cal_qty']
            #Q_chk_opn_adj_qty         = queries['spread_alloc_loc_dtl']['Q_chk_opn_adj_qty']
            Q_upd_alloc_sku_calc_qty   = queries['spread_alloc_loc_dtl']['Q_upd_alloc_sku_calc_qty']
            Q_upd_sku_calc_qty         = queries['spread_alloc_loc_dtl']['Q_upd_sku_calc_qty']
            Q_loop_u1                  = queries['spread_alloc_loc_dtl']['Q_loop_u1']
            Q_loop_u2                  = queries['spread_alloc_loc_dtl']['Q_loop_u2']
            Q_upd_sku_cal_qty_loop     = queries['spread_alloc_loc_dtl']['Q_upd_sku_cal_qty_loop']
            Q_upd_alloc_sku_calc_qty1  = queries['spread_alloc_loc_dtl']['Q_upd_alloc_sku_calc_qty1']
            Q_upd_sku_sprd_qty         = queries['spread_alloc_loc_dtl']['Q_upd_sku_sprd_qty']
            #unit testing 
            C_temp_tbl1      = queries['spread_alloc_loc_dtl']['C_temp_tbl1']
            C_temp_tbl2      = queries['spread_alloc_loc_dtl']['C_temp_tbl2']
            C_temp_tbl3      = queries['spread_alloc_loc_dtl']['C_temp_tbl3']
            C_temp_tbl4      = queries['spread_alloc_loc_dtl']['C_temp_tbl4']


            mycursor = conn.cursor()
            mycursor.execute(C_temp_tbl1)
            mycursor.execute(C_temp_tbl2)
            mycursor.execute(C_temp_tbl3)
            mycursor.execute(C_temp_tbl4)
            #status
            O_status = 1

            df_spread_items = pd.read_sql(Q_get_spread_item,conn,params=(I_alloc,))

            if len(df_spread_items)>0:
                #status
                O_status = 2
                print(df_spread_items)
                for i in range(len(df_spread_items)):
                    L_alloc_no       = df_spread_items.alloc_no[i]
                    L_source_item    = df_spread_items.source_item[i]
                    L_order_no       = df_spread_items.order_no[i]
                    L_wh_id          = df_spread_items.wh_id[i]
                    L_tran_item      = df_spread_items.tran_item[i]
                    L_adj_units      = df_spread_items.adj_units[i]
                    L_sku_spread_qty = df_spread_items.sku_spread_qty[i]
                    #status
                    O_status = 3
                    mycursor.execute(Q_upd_alloc_sku_calc_qty,(L_alloc_no,L_source_item,L_order_no,L_wh_id,L_tran_item))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    #status
                    O_status = 4
                    mycursor.execute(Q_upd_sku_calc_qty,(L_adj_units,L_sku_spread_qty,L_alloc_no,L_source_item,L_order_no,L_wh_id,L_tran_item,L_alloc_no))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 5
            df_union1 = pd.read_sql(Q_loop_u1,conn,params=(I_alloc,I_alloc))
            df_union2 = pd.read_sql(Q_loop_u2,conn,params=(I_alloc,I_alloc))

            df_loop_sprd = pd.concat([df_union1,df_union2], ignore_index=True)

            if len(df_loop_sprd)>0:
                #status
                O_status = 6
                for i in range(len(df_loop_sprd)):
                    L_total_tran_sku_calc_qty = 0
                    L_new_calc_qty            = 0  
                    L_wh_id                   = df_loop_sprd.wh_id[i]
                    L_order_no                = df_loop_sprd.order_no[i]
                    L_source_item             = df_loop_sprd.source_item[i]
                    L_tran_item               = df_loop_sprd.tran_item[i]
                    L_exact_ind               = df_loop_sprd.exact_ind[i]
                    L_new_total_sku_calc_qty  = df_loop_sprd.new_total_sku_calc_qty[i]
                    L_som_qty                 = df_loop_sprd.som_qty[i]

                    df_tot_ten_sku_calc_qty = pd.read_sql(Q_chk_tot_tran_sku_cal_qty,conn,params=(I_alloc,L_exact_ind,L_wh_id,L_order_no,L_source_item,L_tran_item))
                    if len(df_tot_ten_sku_calc_qty)>0:
                        #status
                        O_status = 7
                        L_total_tran_sku_calc_qty = df_tot_ten_sku_calc_qty.total_tran_sku_calc_qty[0]
                    L_new_calc_qty = L_total_tran_sku_calc_qty-L_new_total_sku_calc_qty

                    if L_new_calc_qty > 0:
                        #status
                        O_status = 8
                        if L_som_qty > L_new_calc_qty:
                            L_new_calc_qty = L_new_calc_qty
                        elif L_som_qty < L_new_calc_qty:
                            L_new_calc_qty = L_som_qty

                        mycursor.execute(Q_upd_sku_cal_qty_loop,(float(L_new_calc_qty),float(L_som_qty),L_som_qty,I_alloc,L_source_item,L_order_no,L_wh_id,L_tran_item,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            if len(df_spread_items)>0:
                #status
                O_status = 9
                for i in range(len(df_spread_items)):
                    L_alloc_no       = df_spread_items.alloc_no[i]
                    L_source_item    = df_spread_items.source_item[i]
                    L_order_no       = df_spread_items.order_no[i]
                    L_wh_id          = df_spread_items.wh_id[i]
                    L_tran_item      = df_spread_items.tran_item[i]

                    #status
                    O_status = 10
                    mycursor.execute(Q_upd_alloc_sku_calc_qty1,(L_alloc_no,L_source_item,L_order_no,L_wh_id,L_tran_item))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    #status
                    O_status = 11
                    mycursor.execute(Q_upd_sku_sprd_qty,(L_alloc_no,L_source_item,L_order_no,L_wh_id,L_tran_item))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()
            conn.cursor().close()
            return True,""

    except Exception as error:
        err_return = ""
        if O_status==1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while fetching spread items: "+ str(error)
        elif O_status>=2 and O_status<=11:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating sku calc qty and spread qty: "+ str(error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured : "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

#------------------------------------------------------------------
# Function to update sku_calc_qty
#------------------------------------------------------------------
def update_alloc_qty_dtl (conn,
                          I_alloc):
    L_func_name ="update_alloc_qty_dtl"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_alloc_details_queries.yaml') as fh:
            queries            = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_upd_sku_calc_qty = queries['update_alloc_qty_dtl']['Q_upd_sku_calc_qty']
            mycursor = conn.cursor()
            O_status = 1
            mycursor.execute(Q_upd_sku_calc_qty,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            return True,""

    except Exception as error:
        err_return = ''
        if O_status==1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating the data: "+ str(error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating the data: "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return