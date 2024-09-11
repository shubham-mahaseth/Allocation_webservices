from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
from datetime import datetime
import pandas as pd
import yaml

##################################################################################
#Created By - Priyanshu Pandey                                                   #
#File Name - calculation_setup.py                                                #
#Purpose - setup all inv buckets before final calculation                        #
##################################################################################

#------------------------------------------------------------------------
# Function to populate calc destination before calculation(pop_exclusion)
#------------------------------------------------------------------------

def pop_calc_destination(conn
                         ,I_alloc
                         ,O_status):
    L_func_name ="pop_calc_destination"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculation_setup_queries.yaml') as fh:
            queries                   = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_chk_alloc               = queries['pop_calc_destination']['Q_chk_alloc']
            Q_rfrsh_allitemloc        = queries['pop_calc_destination']['Q_rfrsh_allitemloc']
            #Q_fetch_allitemloc        = queries['pop_calc_destination']['Q_fetch_allitemloc'] #Changes by shubham#
            Q_insert_calc_destination = queries['pop_calc_destination']['Q_insert_calc_destination']
            Q_del_calc_destination    = queries['pop_calc_destination']['Q_del_calc_destination']

            #status
            O_status = 1

            mycursor = conn.cursor()
            #reset error columns
            df_alloc_head = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))
            #status
            O_status = 2

            if len(df_alloc_head)>0:
                #status
                O_status = 3

                L_alloc_level      = df_alloc_head.alloc_level[0]
                L_wh_store_rel_ind = df_alloc_head.wh_store_rel_ind[0]
                L_alloc_criteria   = df_alloc_head.alloc_criteria[0]

                #Q_rfrsh_allitemloc
                mycursor.execute(Q_rfrsh_allitemloc,(I_alloc,L_wh_store_rel_ind))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                conn.commit()
                
                #Changes by shubham start#
                mycursor.execute(Q_del_calc_destination,(I_alloc,))
                conn.commit()
                
                mycursor.execute(Q_insert_calc_destination,(L_alloc_level,I_alloc))
                conn.commit()
                conn.cursor().close()
                return True, ""                
                # #status
                # O_status = 4
            else:
                #status
                O_status = 5
                print("no rows to process for function pop_calc_destination")
                conn.rollback()
                conn.cursor().close()
                return False, L_func_name+":"+str(O_status)+": no rows to process for function pop_calc_destination"
            # #Q_fetch_allitemloc
            # df_allitemloc = pd.read_sql(Q_fetch_allitemloc,conn,params=(I_alloc,))
            # #status
            # O_status = 5

            # if len(df_allitemloc)>0:
                # #del_calc_destination for alloc no
                # mycursor.execute(Q_del_calc_destination,(I_alloc,))
                # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                # conn.commit()
                # #status
                # O_status = 6
                # for i in range(len(df_allitemloc)):
                    # L_insert_calc_destination = (df_allitemloc.loc[i, "alloc_no"],               df_allitemloc.loc[i, "item_type"],            df_allitemloc.loc[i, "source_item"],              
                                                 # df_allitemloc.loc[i, "source_item_level"],      df_allitemloc.loc[i, "source_tran_level"],    df_allitemloc.loc[i, "source_pack_ind"],        
                                                 # df_allitemloc.loc[i, "source_diff1_id"],        df_allitemloc.loc[i, "source_diff2_id"],      df_allitemloc.loc[i, "source_diff3_id"],        
                                                 # df_allitemloc.loc[i, "source_diff4_id"],        df_allitemloc.loc[i, "tran_item"],            df_allitemloc.loc[i, "tran_item_level"],     
                                                 # df_allitemloc.loc[i, "tran_tran_level"],        df_allitemloc.loc[i, "tran_pack_ind"],        df_allitemloc.loc[i, "tran_diff1_id"],       
                                                 # df_allitemloc.loc[i, "tran_diff2_id"],          df_allitemloc.loc[i, "tran_diff3_id"],        df_allitemloc.loc[i, "tran_diff4_id"],      
                                                 # df_allitemloc.loc[i, "hier1"],                  df_allitemloc.loc[i, "hier2"],                df_allitemloc.loc[i, "hier3"],               
                                                 # df_allitemloc.loc[i, "to_loc"],                 df_allitemloc.loc[i, "to_loc_type"],          df_allitemloc.loc[i, "to_loc_name"],         
                                                 # df_allitemloc.loc[i, "sister_store"],           df_allitemloc.loc[i, "sister_store_weight"],  df_allitemloc.loc[i, "assign_default_wh"],   
                                                 # df_allitemloc.loc[i, "clear_ind"],              df_allitemloc.loc[i, "item_loc_status"],      df_allitemloc.loc[i, "size_profile_qty"],    
                                                 # df_allitemloc.loc[i, "total_profile_qty"],      df_allitemloc.loc[i, "stock_on_hand"],        df_allitemloc.loc[i, "on_order"],            
                                                 # df_allitemloc.loc[i, "on_alloc"],               df_allitemloc.loc[i, "alloc_out"],            df_allitemloc.loc[i, "in_transit_qty"],
                                                 # df_allitemloc.loc[i, "backorder_qty"],          df_allitemloc.loc[i, "need_value"],           df_allitemloc.loc[i, "rloh_current_value"],
                                                 # df_allitemloc.loc[i, "rloh_future_value"],      df_allitemloc.loc[i, "like_source_item"],     df_allitemloc.loc[i, "like_source_item_level"],
                                                 # df_allitemloc.loc[i, "like_source_tran_level"], df_allitemloc.loc[i, "like_source_pack_ind"], df_allitemloc.loc[i, "like_source_diff1_id"],   
                                                 # df_allitemloc.loc[i, "like_source_diff2_id"],   df_allitemloc.loc[i, "like_source_diff3_id"], df_allitemloc.loc[i, "like_source_diff4_id"],   
                                                 # df_allitemloc.loc[i, "like_tran_item"],         df_allitemloc.loc[i, "like_tran_item_level"], df_allitemloc.loc[i, "like_tran_tran_level"],  
                                                 # df_allitemloc.loc[i, "like_tran_pack_ind"],     df_allitemloc.loc[i, "like_tran_diff1_id"],   df_allitemloc.loc[i, "like_tran_diff2_id"],     
                                                 # df_allitemloc.loc[i, "like_tran_diff3_id"],     df_allitemloc.loc[i, "like_tran_diff4_id"],   df_allitemloc.loc[i, "like_hier1"],              
                                                 # df_allitemloc.loc[i, "like_hier2"],             df_allitemloc.loc[i, "like_hier3"],           df_allitemloc.loc[i, "like_pack_no"],           
                                                 # df_allitemloc.loc[i, "like_item_weight"],       df_allitemloc.loc[i, "like_size_prof_ind"],   df_allitemloc.loc[i, "create_id"],              
                                                 # df_allitemloc.loc[i, "create_datetime"],        df_allitemloc.loc[i, "last_update_id"],       df_allitemloc.loc[i, "last_update_datetime"])
                    # #insert calc destination
                    # L_insert_calc_destination = convert_numpy(L_insert_calc_destination)

                    # mycursor.execute(Q_insert_calc_destination,L_insert_calc_destination)                                                      
                    # conn.commit()
                    # #status
                    # O_status = 7
                # return True
                #Changes by shubham end#
    except Exception as argument:
        err_return = L_func_name+":"+str(O_status)+": Exception occured:  "+ str(argument)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return


#--------------------------------------------------------------
# Function to populate inventory buckets before calculation
#--------------------------------------------------------------

def pop_inv_buckets(conn
                    ,I_alloc
                    ,O_status):
    L_func_name ="pop_inv_buckets"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculation_setup_queries.yaml') as fh:
            queries                = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_chk_alloc            = queries['pop_calc_destination']['Q_chk_alloc']
            Q_fetch_rule           = queries['pop_inv_buckets']['Q_fetch_rule']
            #Changes by Shubham Start#
            Q_merge_calc_destination_temp_1 = queries['pop_inv_buckets']['Q_merge_calc_destination_temp_1']
            Q_merge_tsf_in_transit_sku      = queries['pop_inv_buckets']['Q_merge_tsf_in_transit_sku']
            Q_merge_tsf_in_transit_style    = queries['pop_inv_buckets']['Q_merge_tsf_in_transit_style']
            Q_merge_tsf_on_alloc_sku        = queries['pop_inv_buckets']['Q_merge_tsf_on_alloc_sku']
            Q_merge_tsf_on_alloc_style      = queries['pop_inv_buckets']['Q_merge_tsf_on_alloc_style']
            Q_merge_on_order_sku            = queries['pop_inv_buckets']['Q_merge_on_order_sku']
            Q_merge_on_order_style          = queries['pop_inv_buckets']['Q_merge_on_order_style']
            Q_merge_ord_on_alloc_sku        = queries['pop_inv_buckets']['Q_merge_ord_on_alloc_sku']
            Q_merge_ord_on_alloc_style      = queries['pop_inv_buckets']['Q_merge_ord_on_alloc_style']
            Q_merge_alloc_out_sku           = queries['pop_inv_buckets']['Q_merge_alloc_out_sku']
            Q_merge_alloc_out_style         = queries['pop_inv_buckets']['Q_merge_alloc_out_style']
            #Changes by Shubham End#

            mycursor = conn.cursor()
            #status
            O_status = 1
            
            df_alloc_head = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))
            #status
            O_status = 2
            if len(df_alloc_head)>0:
                #status
                O_status = 3
                L_alloc_level      = df_alloc_head.alloc_level[0]
                L_alloc_criteria   = df_alloc_head.alloc_criteria[0]

                #Q_fetch_rule
                df_alloc_rule = pd.read_sql(Q_fetch_rule,conn,params=(I_alloc,))
                L_on_order_commit_date = df_alloc_rule.commit_date[0]
                #Changes by Shubham Start#
                L_on_order_commit_date_start = None
                L_all_orders = 'N'
                L_use_rule_level_on_hand_ind = df_alloc_rule.use_rule_level_on_hand_ind[0]
                
                O_status = 4
                mycursor.execute(Q_merge_calc_destination_temp_1,(I_alloc,I_alloc,L_use_rule_level_on_hand_ind))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()
                #Updating in_transit_qty
                O_status = 5
                if L_alloc_level =='T':
                    mycursor.execute(Q_merge_tsf_in_transit_sku,(I_alloc,I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit()                
                else:
                    mycursor.execute(Q_merge_tsf_in_transit_style,(I_alloc,I_alloc,I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit() 

                #Updating on_alloc
                O_status = 6
                if L_alloc_level =='T':
                    mycursor.execute(Q_merge_tsf_on_alloc_sku,(I_alloc,I_alloc,I_alloc,L_on_order_commit_date))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit()                
                else:
                    mycursor.execute(Q_merge_tsf_on_alloc_style,(I_alloc,I_alloc,I_alloc,I_alloc,L_on_order_commit_date))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit()   

                #Updating on_order
                O_status = 7
                if L_alloc_level =='T':
                    mycursor.execute(Q_merge_on_order_sku,(I_alloc,I_alloc,I_alloc,L_all_orders,L_on_order_commit_date_start,L_on_order_commit_date_start,L_on_order_commit_date,L_on_order_commit_date))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit()                
                else:
                    mycursor.execute(Q_merge_on_order_style,(I_alloc,I_alloc,I_alloc,I_alloc,L_all_orders,L_on_order_commit_date_start,L_on_order_commit_date_start,L_on_order_commit_date,L_on_order_commit_date))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit() 
                #Updating ord_on_alloc
                O_status = 8
                if L_alloc_level =='T':
                    mycursor.execute(Q_merge_ord_on_alloc_sku,(I_alloc,I_alloc,I_alloc,L_on_order_commit_date_start,L_on_order_commit_date_start,L_on_order_commit_date,L_on_order_commit_date))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit()                
                else:
                    mycursor.execute(Q_merge_ord_on_alloc_style,(I_alloc,I_alloc,I_alloc,I_alloc,L_on_order_commit_date_start,L_on_order_commit_date_start,L_on_order_commit_date,L_on_order_commit_date))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit()

                #Updating alloc_out
                O_status = 9
                if L_alloc_level =='T':
                    mycursor.execute(Q_merge_alloc_out_sku,(I_alloc,I_alloc,I_alloc,L_all_orders,L_on_order_commit_date_start,L_on_order_commit_date,L_on_order_commit_date_start,L_on_order_commit_date,L_on_order_commit_date_start,L_on_order_commit_date))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit()                
                else:
                    mycursor.execute(Q_merge_alloc_out_style,(I_alloc,I_alloc,L_all_orders,L_on_order_commit_date_start,L_on_order_commit_date,L_on_order_commit_date_start,L_on_order_commit_date,L_on_order_commit_date_start,L_on_order_commit_date))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit()
                    #Changes by Shubham End#
                conn.cursor().close()
                return True, ""

    except Exception as error:
        err_return = ""
        if O_status<=3:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while fetching rule records: "+ str(error)
        elif O_status==4:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while merging stock-on-hand: "+ str(error)
        elif O_status==5:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while merging tsf-in-transit: "+ str(error)
        elif O_status==6:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while merging tsf-on-alloc: "+ str(error)
        elif O_status==7:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while merging on-order: "+ str(error)
        elif O_status==8:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while merging on-order-on-alloc: "+ str(error)
        elif O_status==9:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while merging alloc out: "+ str(error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured:  "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return


#--------------------------------------------------------------
# Function to populate calc needs temp before calculation
#--------------------------------------------------------------

def seed_calc_need(conn
                   ,I_alloc
                   ,O_status):
    L_func_name ="seed_calc_need"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculation_setup_queries.yaml') as fh:
            queries                  = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_insert_calc_need_temp  = queries['seed_calc_need']['Q_insert_calc_need_temp']
            Q_fetch_rule_row         = queries['seed_calc_need']['Q_fetch_rule_row']
            Q_chk_alloc              = queries['pop_calc_destination']['Q_chk_alloc']
            Q_del_calc_need          = queries['seed_calc_need']['Q_del_calc_need']
            Q_chk_calc_seed_table    = queries['seed_calc_need']['Q_chk_calc_seed_table']
            Q_create_calc_seed_temp  = queries['seed_calc_need']['Q_create_calc_seed_temp']

            #dept&class&subclass
            Q_ins_dpt_class_sub_class = queries['seed_calc_need']['Q_ins_dpt_class_sub_class']
            #style
            Q_ins_calc_style_sku = queries['seed_calc_need']['Q_ins_calc_style_sku']
            Q_ins_calc_style     = queries['seed_calc_need']['Q_ins_calc_style']
            Q_ins_style_diff     = queries['seed_calc_need']['Q_ins_style_diff']
            Q_ins_style_sku      = queries['seed_calc_need']['Q_ins_style_sku']
            Q_ins_forecast_sku   = queries['seed_calc_need']['Q_ins_forecast_sku']
            #skulist
            Q_ins_his_sku_list   = queries['seed_calc_need']['Q_ins_his_sku_list']
            Q_del_his_skulist    = queries['seed_calc_need']['Q_del_his_skulist']
            Q_ins_frst_sku_list  = queries['seed_calc_need']['Q_ins_frst_sku_list']
            Q_del_frst_skulist   = queries['seed_calc_need']['Q_del_frst_skulist']

            mycursor = conn.cursor()
            #status
            O_status = 1
            #Q_chk_allitemloc_table
            df_chk = pd.read_sql(Q_chk_calc_seed_table,conn)
            L_chk = df_chk.chk[0]

            if L_chk == 1:
                #status
                print(" Please drop the table alloc_calc_need_temp")
                print(O_status,L_func_name)
                #return no_data
            else:
                mycursor.execute(Q_create_calc_seed_temp)

            #alloc level
            df_alloc_head=pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))
            #status
            O_status = 2
            if len(df_alloc_head)>0:
                L_alloc_level = df_alloc_head.alloc_level[0]
                #status
                O_status = 3

            #alloc rule level
            df_alloc_rule = pd.read_sql(Q_fetch_rule_row,conn,params=(I_alloc,))
            #status
            O_status = 4

            if len(df_alloc_rule)>0:
                
                L_rule_level = df_alloc_rule.rule_level[0]
                L_rule_type = df_alloc_rule.rule_type[0] #ce-177 #To check for forecast

                if L_rule_level == 'H1' or L_rule_level == 'H2' or L_rule_level == 'H3': #ce-177
                    O_status = 6
                    mycursor.execute(Q_ins_dpt_class_sub_class,(L_rule_level,L_rule_level,L_rule_level,L_rule_level,I_alloc,I_alloc)) #ce-177
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                if L_rule_level == 'S' and L_rule_type == 'H':  #sku #ce-177
                    #status

                    O_status = 1 
                    mycursor.execute(Q_insert_calc_need_temp,(L_rule_level,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                    O_status = 2
                    mycursor.execute(Q_ins_his_sku_list,(L_rule_level,I_alloc))  #ce-177
                    print(111)
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                    O_status = 3
                    mycursor.execute(Q_del_his_skulist,(I_alloc,))  #ce-177
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                elif L_rule_level =='ST':  #ce-177
                    if L_alloc_level == 'T':
                        O_status = 4
                        mycursor.execute(Q_ins_calc_style_sku,(L_rule_level,I_alloc,I_alloc,I_alloc))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    elif L_alloc_level == 'D':
                        O_status = 5
                        mycursor.execute(Q_ins_calc_style,(L_rule_level,I_alloc,I_alloc,I_alloc))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                elif L_rule_level =='SD':  #ce-177
                    if L_alloc_level == 'D':
                        O_status = 7
                        mycursor.execute(Q_ins_style_diff,(L_rule_level,I_alloc,I_alloc,I_alloc))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    elif L_alloc_level == 'T':
                        O_status = 5
                        mycursor.execute(Q_ins_style_sku,(L_rule_level,I_alloc))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                elif L_rule_level == 'S' and L_rule_type == 'F':
                    mycursor.execute(Q_ins_forecast_sku,(I_alloc,L_rule_level,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                    if L_alloc_level == 'T':
                        mycursor.execute(Q_ins_frst_sku_list,(I_alloc,L_rule_level,I_alloc)) 
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                        mycursor.execute(Q_del_frst_skulist,(I_alloc,)) 
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            conn.commit()
            conn.cursor().close()
            return True,""

    except Exception as argument:
        err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(argument)
        print(err_return)
        #print("Exception occured in: ",L_func_name,argument)
        conn.rollback()
        conn.cursor().close()
        return False,err_return

#--------------------------------------------------------------
# Function to setup need
#--------------------------------------------------------------

def setup_need(conn
               ,I_alloc
               ,O_status):
    L_func_name ="setup_need"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculation_setup_queries.yaml') as fh:
            queries      = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_fetch_rule = queries['setup_need']['Q_fetch_rule']

            mycursor = conn.cursor()
            #status
            O_status = 1
            check_SCN, err_msg = seed_calc_need(conn,I_alloc,O_status)
            if check_SCN == False:
                #status
                O_status = ("seed_calc_need: 2")
                print(O_status)
                conn.cursor().close()
                return False, L_func_name+":"+str(O_status)+": "+str(err_msg)
            #status
            O_status = 3
            #Q_fetch_rule_type
            df_alloc_rule = pd.read_sql(Q_fetch_rule,conn,params=(I_alloc,))

            if len(df_alloc_rule)>0:
                L_rule_type = df_alloc_rule.rule_type[0]
                #status
                O_status = 4

                #if rule type is history
                if L_rule_type =='H':
                    #status
                    O_status = 5
                    check_SHist, err_msg = setup_sales_hist_need(conn,I_alloc,O_status)
                    if check_SHist == False:
                        #status
                        O_status = 6
                        print(O_status)
                        conn.cursor().close()
                        return False, L_func_name+":"+str(O_status)+": "+str(err_msg)
                    #status
                    O_status = 7
                    conn.cursor().close()
                    return True, ""
                #Changes by shubham start#
                #if rule type is forcast
                if L_rule_type =='F':
                    #status
                    O_status = 5.1
                    check_SFcst, err_msg = setup_sales_fcst_need(conn,I_alloc,O_status)
                    if check_SFcst == False:
                        #status
                        O_status = 6.1
                        print(O_status)
                        conn.cursor().close()
                        return False,  L_func_name+":"+str(O_status)+": "+str(err_msg)
                    #status
                    O_status = 7.1
                    conn.cursor().close()
                    return True, ""
                #Changes by shubham end#
            return True, ""
    except Exception as argument:
        err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(argument)
        print(err_return)
        #print("Exception occured in: ",L_func_name,argument)
        conn.rollback()
        conn.cursor().close()
        return False, err_return 

#--------------------------------------------------------------
# Function to setup sales hist need
#--------------------------------------------------------------

def setup_sales_hist_need(conn
                          ,I_alloc
                          ,O_status):
    L_func_name ="setup_sales_hist_need"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculation_setup_queries.yaml') as fh:
            queries      = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_fetch_rule = queries['setup_sales_hist_need']['Q_fetch_rule']
            
            #tables
            Q_create_temp_tble = queries['setup_sales_hist_need']['Q_create_temp_tble']
            Q_drop_temp_tble   = queries['setup_sales_hist_need']['Q_drop_temp_tble']

            #hist_dept
            Q_upd_dept_sales_need         = queries['setup_need_sales_hist_dept']['Q_upd_dept_sales_need'] 
            Q_upd_ecom_dept_sales_need    = queries['setup_need_sales_hist_dept']['Q_upd_ecom_dept_sales_need'] 
            Q_upd_lp_dept_sales_need      = queries['setup_need_sales_hist_dept']['Q_upd_lp_dept_sales_need'] 
            Q_upd_lp_ecom_dept_sales_need = queries['setup_need_sales_hist_dept']['Q_upd_lp_ecom_dept_sales_need'] 

            #class_hist
            Q_upd_class_sales_need         = queries['setup_need_sales_hist_class']['Q_upd_class_sales_need']
            Q_upd_ecom_class_sales_need    = queries['setup_need_sales_hist_class']['Q_upd_ecom_class_sales_need']
            Q_upd_lp_class_sales_need      = queries['setup_need_sales_hist_class']['Q_upd_lp_class_sales_need']
            Q_upd_lp_ecom_class_sales_need = queries['setup_need_sales_hist_class']['Q_upd_lp_ecom_class_sales_need']

            #subclass_hist
            Q_upd_subclass_sales_need          = queries['setup_need_sales_hist_subclass']['Q_upd_subclass_sales_need']
            Q_upd_ecom_subclass_sales_need     = queries['setup_need_sales_hist_subclass']['Q_upd_ecom_subclass_sales_need']
            Q_upd_lp_subclass_sales_need       = queries['setup_need_sales_hist_subclass']['Q_upd_lp_subclass_sales_need']
            Q_upd_lp_ecom_subclass_sales_need  = queries['setup_need_sales_hist_subclass']['Q_upd_lp_ecom_subclass_sales_need']

            #style_hist
            Q_upd_style_sales_need              = queries['setup_need_sales_hist_style']['Q_upd_style_sales_need']
            Q_upd_like_style_sales_need         = queries['setup_need_sales_hist_style']['Q_upd_like_style_sales_need']
            Q_upd_ecom_style_sales_need         = queries['setup_need_sales_hist_style']['Q_upd_ecom_style_sales_need']
            Q_upd_ecom_like_style_sales_need    = queries['setup_need_sales_hist_style']['Q_upd_ecom_like_style_sales_need']
            Q_upd_lp_style_sales_need           = queries['setup_need_sales_hist_style']['Q_upd_lp_style_sales_need']
            Q_upd_lp_like_style_sales_need      = queries['setup_need_sales_hist_style']['Q_upd_lp_like_style_sales_need']
            Q_upd_ecom_lp_style_sales_need      = queries['setup_need_sales_hist_style']['Q_upd_ecom_lp_style_sales_need']
            Q_upd_ecom_lp_like_style_sales_need = queries['setup_need_sales_hist_style']['Q_upd_ecom_lp_like_style_sales_need']

            Q_upd_stylediff_sales_need          = queries['setup_need_sales_hist_stylediff']['Q_upd_stylediff_sales_need']
            Q_upd_like_stylediff_sales_need     = queries['setup_need_sales_hist_stylediff']['Q_upd_like_stylediff_sales_need']
            Q_upd_lp_stylediff_sales_need       = queries['setup_need_sales_hist_stylediff']['Q_upd_lp_stylediff_sales_need']
            Q_upd_lp_like_stylediff_sales_need  = queries['setup_need_sales_hist_stylediff']['Q_upd_lp_like_stylediff_sales_need']

            mycursor = conn.cursor()

            #status
            O_status = 1

            df_alloc_rule = pd.read_sql(Q_fetch_rule,conn,params=(I_alloc,))
            #status
            O_status = 2

            if len(df_alloc_rule)>0:                
                L_rule_level = df_alloc_rule.rule_level[0]
                L_rule_type           = df_alloc_rule.rule_type[0]
                L_regular_sales_ind   = df_alloc_rule.regular_sales_ind[0]
                L_promo_sales_ind     = df_alloc_rule.promo_sales_ind[0]
                L_clearance_sales_ind = df_alloc_rule.clearance_sales_ind[0]

                #status
                O_status = 3
                #sku level sales hist
                if L_rule_level =='S':
                    #status
                    O_status = 4
                    check_sHist, err_msg = setup_sku_sales_hist_need(conn,I_alloc,O_status) 
                    if check_sHist == False:
                        #status
                        O_status = 5
                        conn.cursor().close()
                        return False, L_func_name+":"+str(O_status)+": " + str(err_msg)
                    conn.cursor().close()
                    return True, ""
                elif L_rule_level == 'H1':  #1720
                    print(L_rule_level,"L_rule_level")

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))

                    print(pd.read_sql("select sales_hist_need,to_loc,i.* from alloc_calc_need_temp1 i;",conn))

                    O_status = 1
                    mycursor.execute(Q_upd_dept_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                    mycursor.execute(Q_drop_temp_tble)

                    print(pd.read_sql("select sales_hist_need,to_loc,i.* from alloc_calc_need_temp i;",conn))

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))

                    O_status = 2
                    mycursor.execute(Q_upd_ecom_dept_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                    print(pd.read_sql("select sales_hist_need,to_loc,i.* from alloc_calc_need_temp i;",conn))

                    mycursor.execute(Q_drop_temp_tble)

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 3
                    mycursor.execute(Q_upd_lp_dept_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)
                    
                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 4
                    mycursor.execute(Q_upd_lp_ecom_dept_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)

                elif L_rule_level == 'H2':  #1720 RC to test for  ecom
                    print(L_rule_level,"L_rule_level")

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    print(pd.read_sql("select sales_hist_need,to_loc,i.* from alloc_calc_need_temp1 i;",conn))
                    O_status = 5
                    mycursor.execute(Q_upd_class_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    print(pd.read_sql("select sales_hist_need,to_loc,i.* from alloc_calc_need_temp i;",conn))
                    mycursor.execute(Q_drop_temp_tble)

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 6
                    mycursor.execute(Q_upd_ecom_class_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)
                    
                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 7
                    mycursor.execute(Q_upd_lp_class_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)
                    
                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 8
                    mycursor.execute(Q_upd_lp_ecom_class_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)

                elif L_rule_level == 'H3':  #1720
                    print(L_rule_level,"L_rule_level")

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    print(pd.read_sql("select sales_hist_need,to_loc,i.* from alloc_calc_need_temp1 i;",conn))
                    O_status = 9
                    mycursor.execute(Q_upd_subclass_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    print(pd.read_sql("select sales_hist_need,to_loc,i.* from alloc_calc_need_temp i;",conn))
                    mycursor.execute(Q_drop_temp_tble)

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 10
                    mycursor.execute(Q_upd_ecom_subclass_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)
                    
                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 11
                    mycursor.execute(Q_upd_lp_subclass_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)
                    
                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 12
                    mycursor.execute(Q_upd_lp_ecom_subclass_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)

                elif L_rule_level == 'ST':  #1720
                    print(L_rule_level,"L_rule_level")

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    print(pd.read_sql("select sales_hist_need,to_loc,i.* from alloc_calc_need_temp1 i;",conn))
                    O_status = 13
                    mycursor.execute(Q_upd_style_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    print(pd.read_sql("select sales_hist_need,to_loc,i.* from alloc_calc_need_temp i;",conn))
                    mycursor.execute(Q_drop_temp_tble)

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 14
                    mycursor.execute(Q_upd_like_style_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)
                    
                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 15
                    mycursor.execute(Q_upd_ecom_style_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 16
                    mycursor.execute(Q_upd_ecom_like_style_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 17
                    mycursor.execute(Q_upd_lp_style_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 18
                    mycursor.execute(Q_upd_lp_like_style_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 19
                    mycursor.execute(Q_upd_ecom_lp_style_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 20
                    mycursor.execute(Q_upd_ecom_lp_like_style_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)

                elif L_rule_level == 'SD':  #1720
                    print(L_rule_level,"L_rule_level")

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 21
                    mycursor.execute(Q_upd_stylediff_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 22
                    mycursor.execute(Q_upd_like_stylediff_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 23
                    mycursor.execute(Q_upd_lp_stylediff_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)

                    mycursor.execute(Q_create_temp_tble,(I_alloc,))
                    O_status = 24
                    mycursor.execute(Q_upd_lp_like_stylediff_sales_need,(L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,I_alloc,L_rule_level)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    mycursor.execute(Q_drop_temp_tble)

                conn.commit()
                conn.cursor().close()
                return True, ""
    except Exception as argument:
        err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(argument)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

#Changes by shubham start#
#--------------------------------------------------------------
# Function to setup sales fcst need
#--------------------------------------------------------------

def setup_sales_fcst_need(conn
                          ,I_alloc
                          ,O_status):
    L_func_name ="setup_sales_fcst_need"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculation_setup_queries.yaml') as fh:
            queries      = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_fetch_rule = queries['setup_sales_hist_need']['Q_fetch_rule']
            
            #status
            O_status = 1

            df_alloc_rule = pd.read_sql(Q_fetch_rule,conn,params=(I_alloc,))
            #status
            O_status = 2

            if len(df_alloc_rule)>0:                
                L_rule_level = df_alloc_rule.rule_level[0]
                #status
                O_status = 3
                #Hier1 level sales hist
                if L_rule_level =='H1':
                    O_status = 4
                    check_H1Fsct, err_msg = setup_hier1_sales_fcst_need(conn,I_alloc,O_status)
                    if check_H1Fsct == False:
                        O_status = 5
                        conn.cursor().close()
                        return False,  L_func_name+":"+str(O_status)+": " + str(err_msg)
                    conn.cursor().close()
                    return True, ""
                O_status = 6
                #Hier2 level sales hist
                if L_rule_level =='H2':
                    O_status = 7
                    check_H2Fsct, err_msg = setup_hier2_sales_fcst_need(conn,I_alloc,O_status)
                    if check_H2Fsct == False:
                        O_status = 8
                        conn.cursor().close()
                        return False,  L_func_name+":"+str(O_status)+": " + str(err_msg)
                    conn.cursor().close()
                    return True, ""
                O_status = 9
                #Hier3 level sales hist
                if L_rule_level =='H3':
                    O_status = 10
                    check_H3Fsct, err_msg =  setup_hier3_sales_fcst_need(conn,I_alloc,O_status)
                    if check_H3Fsct== False:
                        O_status = 11
                        conn.cursor().close()
                        return False,  L_func_name+":"+str(O_status)+": " + str(err_msg)
                    conn.cursor().close()
                    return True, ""
                O_status = 12
                #Style level sales hist
                if L_rule_level =='ST':
                    O_status = 13
                    check_StylFsct, err_msg =  setup_style_sales_fcst_need(conn,I_alloc,O_status)
                    if check_StylFsct == False:
                        O_status = 14
                        conn.cursor().close()
                        return False, L_func_name+":"+str(O_status)+": " + str(err_msg)
                    conn.cursor().close()
                    return True, ""
                O_status = 15
                #Sku level sales hist
                if L_rule_level =='S':
                    O_status = 16
                    check_SkuFsct, err_msg = setup_sku_sales_fcst_need(conn,I_alloc,O_status)
                    if check_SkuFsct == False:
                        O_status = 17
                        conn.cursor().close()
                        return False, L_func_name+":"+str(O_status)+": " + str(err_msg)
                    conn.cursor().close()
                    return True, ""                   
    except Exception as argument:
        err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(argument)
        print(err_return)
        print("Exception occured in: ",L_func_name,argument)
        conn.rollback()
        return False, err_return
        
#--------------------------------------------------------------
# Function to get hier1 level sales fcst
#--------------------------------------------------------------

def setup_hier1_sales_fcst_need(conn
                              ,I_alloc
                              ,O_status):
    L_func_name ="setup_hier1_sales_fcst_need"
    O_status = 0
    L_use_sister_store = 'Y'
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculation_setup_queries.yaml') as fh:
            queries                   = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_create_calc_need_temp_1 = queries['setup_hier1_sales_fcst_need']['Q_create_calc_need_temp_1']
            Q_drop_calc_need_temp_1   = queries['setup_hier1_sales_fcst_need']['Q_drop_calc_need_temp_1']
            Q_merge_calc_need_temp    = queries['setup_hier1_sales_fcst_need']['Q_merge_calc_need_temp']
            Q_merge_ss_calc_need_temp = queries['setup_hier1_sales_fcst_need']['Q_merge_ss_calc_need_temp']
          
            mycursor = conn.cursor()
            #status
            O_status = 1
            mycursor.execute(Q_create_calc_need_temp_1,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 2
            mycursor.execute(Q_merge_calc_need_temp,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 3
            mycursor.execute(Q_drop_calc_need_temp_1)
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()
            
            O_status = 4
            if L_use_sister_store == 'Y':
                O_status = 5
                mycursor.execute(Q_create_calc_need_temp_1,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()            
                
                O_status = 6
                mycursor.execute(Q_merge_ss_calc_need_temp,(I_alloc,L_use_sister_store))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()                

                O_status = 7
                mycursor.execute(Q_drop_calc_need_temp_1)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()
            conn.cursor().close()
            return True, ""

    except Exception as argument:
        err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(argument)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

#--------------------------------------------------------------
# Function to get hier2 level sales fcst
#--------------------------------------------------------------

def setup_hier2_sales_fcst_need(conn
                              ,I_alloc
                              ,O_status):
    L_func_name ="setup_hier2_sales_fcst_need"
    O_status = 0
    L_use_sister_store = 'Y'
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculation_setup_queries.yaml') as fh:
            queries                   = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_create_calc_need_temp_1 = queries['setup_hier1_sales_fcst_need']['Q_create_calc_need_temp_1']
            Q_drop_calc_need_temp_1   = queries['setup_hier1_sales_fcst_need']['Q_drop_calc_need_temp_1']
            Q_merge_calc_need_temp    = queries['setup_hier2_sales_fcst_need']['Q_merge_calc_need_temp']
            Q_merge_ss_calc_need_temp = queries['setup_hier2_sales_fcst_need']['Q_merge_ss_calc_need_temp']
          
            mycursor = conn.cursor()
            #status
            O_status = 1
            mycursor.execute(Q_create_calc_need_temp_1,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 2
            mycursor.execute(Q_merge_calc_need_temp,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 3
            mycursor.execute(Q_drop_calc_need_temp_1)
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()
            
            O_status = 4
            if L_use_sister_store == 'Y':
                O_status = 5
                mycursor.execute(Q_create_calc_need_temp_1,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()            
                
                O_status = 6
                mycursor.execute(Q_merge_ss_calc_need_temp,(I_alloc,L_use_sister_store))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()                

                O_status = 7
                mycursor.execute(Q_drop_calc_need_temp_1)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()
            conn.cursor().close()
            return True, ""
            
    except Exception as argument:
        err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(argument)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return         

#--------------------------------------------------------------
# Function to get hier3 level sales fcst
#--------------------------------------------------------------
        
def setup_hier3_sales_fcst_need(conn
                              ,I_alloc
                              ,O_status):
    L_func_name ="setup_hier3_sales_fcst_need"
    O_status = 0
    L_use_sister_store = 'Y'
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculation_setup_queries.yaml') as fh:
            queries                   = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_create_calc_need_temp_1 = queries['setup_hier1_sales_fcst_need']['Q_create_calc_need_temp_1']
            Q_drop_calc_need_temp_1   = queries['setup_hier1_sales_fcst_need']['Q_drop_calc_need_temp_1']
            Q_merge_calc_need_temp    = queries['setup_hier3_sales_fcst_need']['Q_merge_calc_need_temp']
            Q_merge_ss_calc_need_temp = queries['setup_hier3_sales_fcst_need']['Q_merge_ss_calc_need_temp']
          
            mycursor = conn.cursor()
            #status
            O_status = 1
            mycursor.execute(Q_create_calc_need_temp_1,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 2
            mycursor.execute(Q_merge_calc_need_temp,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 3
            mycursor.execute(Q_drop_calc_need_temp_1)
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()
            
            O_status = 4
            if L_use_sister_store == 'Y':
                O_status = 5
                mycursor.execute(Q_create_calc_need_temp_1,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()            
                
                O_status = 6
                mycursor.execute(Q_merge_ss_calc_need_temp,(I_alloc,L_use_sister_store))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()                

                O_status = 7
                mycursor.execute(Q_drop_calc_need_temp_1)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()
            conn.cursor().close()
            return True, ""

    except Exception as argument:
        err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(argument)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return       

#--------------------------------------------------------------
# Function to get sku level sales fcst
#--------------------------------------------------------------

def setup_sku_sales_fcst_need(conn
                              ,I_alloc
                              ,O_status):
    L_func_name ="setup_sku_sales_fcst_need"
    O_status = 0
    L_use_sister_store = 'Y'
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculation_setup_queries.yaml') as fh:
            queries                      = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_create_calc_need_temp_1    = queries['setup_hier1_sales_fcst_need']['Q_create_calc_need_temp_1']
            Q_drop_calc_need_temp_1      = queries['setup_hier1_sales_fcst_need']['Q_drop_calc_need_temp_1']
            Q_merge_like1_calc_need_temp = queries['setup_sku_sales_fcst_need']['Q_merge_like1_calc_need_temp']
            Q_merge_like2_calc_need_temp = queries['setup_sku_sales_fcst_need']['Q_merge_like2_calc_need_temp']
            Q_merge_ss1_calc_need_temp   = queries['setup_sku_sales_fcst_need']['Q_merge_ss1_calc_need_temp']
            Q_merge_ss2_calc_need_temp   = queries['setup_sku_sales_fcst_need']['Q_merge_ss2_calc_need_temp']
          
            mycursor = conn.cursor()
            #status
            O_status = 1
            mycursor.execute(Q_create_calc_need_temp_1,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 2
            mycursor.execute(Q_merge_like1_calc_need_temp,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 3
            mycursor.execute(Q_drop_calc_need_temp_1)
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 4
            mycursor.execute(Q_create_calc_need_temp_1,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 5
            mycursor.execute(Q_merge_like2_calc_need_temp,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 6
            mycursor.execute(Q_drop_calc_need_temp_1)
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()
            
            O_status = 7
            if L_use_sister_store == 'Y':
                O_status = 8
                mycursor.execute(Q_create_calc_need_temp_1,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()            
                
                O_status = 9
                mycursor.execute(Q_merge_ss1_calc_need_temp,(I_alloc,L_use_sister_store))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()                

                O_status = 10
                mycursor.execute(Q_drop_calc_need_temp_1)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()

                O_status = 11
                mycursor.execute(Q_create_calc_need_temp_1,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()            
                
                O_status = 12
                mycursor.execute(Q_merge_ss2_calc_need_temp,(I_alloc,L_use_sister_store))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()                

                O_status = 13
                mycursor.execute(Q_drop_calc_need_temp_1)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()
            conn.cursor().close()    
            return True, ""

    except Exception as argument:
        err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(argument)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return  

#--------------------------------------------------------------
# Function to get style level sales fcst
#--------------------------------------------------------------

def setup_style_sales_fcst_need(conn
                              ,I_alloc
                              ,O_status):
    L_func_name ="setup_style_sales_fcst_need"
    O_status = 0
    L_use_sister_store = 'Y'
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculation_setup_queries.yaml') as fh:
            queries                      = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_create_calc_need_temp_1    = queries['setup_hier1_sales_fcst_need']['Q_create_calc_need_temp_1']
            Q_drop_calc_need_temp_1      = queries['setup_hier1_sales_fcst_need']['Q_drop_calc_need_temp_1']
            Q_merge_like1_calc_need_temp = queries['setup_style_sales_fcst_need']['Q_merge_like1_calc_need_temp']
            Q_merge_like2_calc_need_temp = queries['setup_style_sales_fcst_need']['Q_merge_like2_calc_need_temp']
            Q_merge_ss1_calc_need_temp   = queries['setup_style_sales_fcst_need']['Q_merge_ss1_calc_need_temp']
            Q_merge_ss2_calc_need_temp   = queries['setup_style_sales_fcst_need']['Q_merge_ss2_calc_need_temp']
          
            mycursor = conn.cursor()
            #status
            O_status = 1
            mycursor.execute(Q_create_calc_need_temp_1,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 2
            mycursor.execute(Q_merge_like1_calc_need_temp,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 3
            mycursor.execute(Q_drop_calc_need_temp_1)
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 4
            mycursor.execute(Q_create_calc_need_temp_1,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 5
            mycursor.execute(Q_merge_like2_calc_need_temp,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 6
            mycursor.execute(Q_drop_calc_need_temp_1)
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()
            
            O_status = 7
            if L_use_sister_store == 'Y':
                O_status = 8
                mycursor.execute(Q_create_calc_need_temp_1,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()            
                
                O_status = 9
                mycursor.execute(Q_merge_ss1_calc_need_temp,(I_alloc,L_use_sister_store))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()                

                O_status = 10
                mycursor.execute(Q_drop_calc_need_temp_1)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()

                O_status = 11
                mycursor.execute(Q_create_calc_need_temp_1,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()            
                
                O_status = 12
                mycursor.execute(Q_merge_ss2_calc_need_temp,(I_alloc,L_use_sister_store))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()                

                O_status = 13
                mycursor.execute(Q_drop_calc_need_temp_1)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()
            conn.cursor().close()    
            return True, ""

    except Exception as argument:
        err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(argument)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return          
#Changes by shubham end#
#--------------------------------------------------------------
# Function to get sku level sales hist
#--------------------------------------------------------------

def setup_sku_sales_hist_need(conn
                              ,I_alloc
                              ,O_status):
    L_func_name ="setup_sku_sales_hist_need"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculation_setup_queries.yaml') as fh:
            queries                   = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_fetchall_rule           = queries['setup_sku_sales_hist_need']['Q_fetchall_rule']
            Q_item_loc_hist_loop      = queries['setup_sku_sales_hist_need']['Q_item_loc_hist_loop']
            Q_ecom_item_loc_hist_loop = queries['setup_sku_sales_hist_need']['Q_ecom_item_loc_hist_loop']
            Q_update_sales            = queries['setup_sku_sales_hist_need']['Q_update_sales']
            Q_update_ecom_sales       = queries['setup_sku_sales_hist_need']['Q_update_ecom_sales']
          
            mycursor = conn.cursor()
            #status
            O_status = 1

            #Q_fetchall_rule
            df_alloc_rule = pd.read_sql(Q_fetchall_rule,conn,params=(I_alloc,))
            #status
            O_status = 2

            if len(df_alloc_rule)>0:               
                L_regular_sales_ind   = df_alloc_rule.regular_sales_ind[0]
                L_promo_sales_ind     = df_alloc_rule.promo_sales_ind[0]
                L_clearance_sales_ind = df_alloc_rule.clearance_sales_ind[0]
                L_rule_type           = df_alloc_rule.rule_type[0]
                
                #status
                O_status = 3
                df_item_loc_hist_loop = pd.read_sql(Q_item_loc_hist_loop,conn,params=(I_alloc,))

                #status
                O_status = 4
                if len(df_item_loc_hist_loop)>0:
                    #status
                    O_status = 5

                    for i in range(len(df_item_loc_hist_loop)):
                        L_alloc_no = df_item_loc_hist_loop.loc[i, "alloc_no"]
                        L_eow_date = df_item_loc_hist_loop.loc[i, "eow_date"]
                        L_weight   = df_item_loc_hist_loop.loc[i, "weight"]
                        params     = (L_weight,L_weight,L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,L_alloc_no,L_eow_date) #L_rule_type parameter is not used

                        params     = convert_numpy(params)
                        #Q_update_sales
                        mycursor.execute(Q_update_sales,params)
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    #commit
                    conn.commit()
                    #status
                    O_status = 6

                #Q_ecom_item_loc_hist_loop
                df_ecom_item_loc_hist_loop = pd.read_sql(Q_ecom_item_loc_hist_loop,conn, params=(I_alloc,))
                #status
                O_status = 7

                if len(df_ecom_item_loc_hist_loop)>0:
                    #status
                    O_status = 8
                    for i in range(len(df_ecom_item_loc_hist_loop)):
                        L_alloc_no            = df_ecom_item_loc_hist_loop.loc[i, "alloc_no"]
                        #L_rule_many_to_one_id = df_ecom_item_loc_hist_loop.loc[i, "rule_many_to_one_id"]
                        L_eow_date            = df_ecom_item_loc_hist_loop.loc[i, "eow_date"]
                        L_weight              = df_ecom_item_loc_hist_loop.loc[i, "weight"]
                        #Q_update_sales
                        params     = (L_weight,L_weight,L_regular_sales_ind,L_promo_sales_ind,L_clearance_sales_ind,L_alloc_no,L_rule_type,L_eow_date)
                        params     = convert_numpy(params)
                        mycursor.execute(Q_update_ecom_sales,params)
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    #commit
                    conn.commit()
                    #status
                    O_status = 9
                conn.cursor().close()
                return True, ""

    except Exception as argument:
        err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(argument)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

#--------------------------------------------------------------
# Function to finalize sales setup
#--------------------------------------------------------------

def finalize_sale_setup(conn
                        ,I_alloc
                        ,O_status):
    L_func_name ="finalize_sale_setup"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculation_setup_queries.yaml') as fh:
            queries               = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_fetch_rule          = queries['finalize_sale_setup']['Q_fetch_rule']
            #Changes by Shubham Start#
            Q_merge_calc_dest_hier1 = queries['finalize_sale_setup']['Q_merge_calc_dest_hier1']
            Q_merge_calc_dest_hier2 = queries['finalize_sale_setup']['Q_merge_calc_dest_hier2']
            Q_merge_calc_dest_hier3 = queries['finalize_sale_setup']['Q_merge_calc_dest_hier3']
            Q_merge_calc_dest_style = queries['finalize_sale_setup']['Q_merge_calc_dest_style']
            Q_merge_calc_dest_sku   = queries['finalize_sale_setup']['Q_merge_calc_dest_sku']
            Q_merge_calc_dest       = queries['finalize_sale_setup']['Q_merge_calc_dest']
            #Changes by Shubham End#
            mycursor = conn.cursor()
            #status
            O_status = 1
            #Q_fetch_rule
            df_alloc_rule = pd.read_sql(Q_fetch_rule,conn,params=(I_alloc,))
            #status
            O_status = 2
            if len(df_alloc_rule)>0:               
                L_rule_level = df_alloc_rule.rule_level[0]
                #status
                O_status = 3
                #Changes by Shubham Start#
                if L_rule_level =='H1':
                    #status
                    O_status = 4
                    mycursor.execute(Q_merge_calc_dest_hier1,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit()
                    conn.cursor().close()
                    return True, ""
                elif L_rule_level =='H2':
                    #status
                    O_status = 4
                    mycursor.execute(Q_merge_calc_dest_hier2,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit()
                    conn.cursor().close()
                    return True, ""                    
                elif L_rule_level =='H3':
                    #status
                    O_status = 4
                    mycursor.execute(Q_merge_calc_dest_hier3,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit()
                    conn.cursor().close()
                    return True, ""  
                elif L_rule_level =='ST':
                    #status
                    O_status = 4
                    mycursor.execute(Q_merge_calc_dest_style,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit()
                    conn.cursor().close()
                    return True, ""  
                elif L_rule_level =='S':
                    #status
                    O_status = 4
                    mycursor.execute(Q_merge_calc_dest_sku,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit()
                    conn.cursor().close()
                    return True, ""  
                elif L_rule_level == None:
                    #status
                    O_status = 4
                    mycursor.execute(Q_merge_calc_dest,(I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    conn.commit()
                    conn.cursor().close()
                    return True, ""        
            else:
                conn.cursor().close()
                return False, L_func_name+":"+str(O_status) 
                #Changes by Shubham End#
    except Exception as argument:
        err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(argument)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

#--------------------------------------------------------------
# Function to setup calculation
#--------------------------------------------------------------

def calculation_setup(conn
                      ,I_alloc
                      ,O_status):
    L_func_name ="calculation_setup"
    O_status =0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculation_setup_queries.yaml') as fh:
            queries      = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_chk_alloc  = queries['calculation_setup']['Q_chk_alloc']
            Q_fetch_rule = queries['calculation_setup']['Q_fetch_rule']
            
            #status
            O_status = 1

            df_alloc_head = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))
            L_alloc_level = df_alloc_head.alloc_level[0]
            check_PopCD, err_msg = pop_calc_destination(conn,I_alloc,O_status)
            if check_PopCD == False:
                #status
                O_status = 2
                print("pop_calc_destination: ",O_status)
                conn.cursor().close()
                return False, L_func_name+":"+str(O_status)+": "+ str(err_msg)
            
            if L_alloc_level == 'T':
                check_Pkaacdt, err_msg = ins_sku_for_pack_aacdt(conn,I_alloc)
                if check_Pkaacdt == False:
                    print("ins_sku_for_pack_aacdt failed: ",O_status)
                    conn.cursor().close()
                    return False, L_func_name+":"+str(O_status)+": "+ str(err_msg)

            #pop_inv_buckets
            check_PopInv, err_msg = pop_inv_buckets(conn,I_alloc,O_status)
            if check_PopInv == False:
                #status
                O_status = 3
                print("pop_inv_buckets: ",O_status)
                conn.cursor().close()
                return False, L_func_name+":"+str(O_status)+": "+ str(err_msg)
            
                                                                           
            #status
            O_status = 4
            if len(df_alloc_head)>0:

                #status
                O_status = 5
                if len(L_alloc_level)>0:
                    #status
                    O_status = 5
                    #setup_need
                    check_SNeed, err_msg = setup_need(conn,I_alloc,O_status)
                    if check_SNeed == False:
                        #status
                        O_status = 6
                        print("setup_need: ",O_status)
                        conn.cursor().close()
                        return False, L_func_name+":"+str(O_status)+": "+ str(err_msg)

            check_Fsetup, err_msg =  finalize_sale_setup(conn,I_alloc,O_status)
            if check_Fsetup == False:
                #status
                O_status = 7
                print("finalize_sale_setup: ",O_status)
                conn.cursor().close()
                return False, L_func_name+":"+str(O_status)+": "+ str(err_msg)

            if L_alloc_level == 'T':
                check_PPack, err_msg = pop_pack_item_loc(conn,I_alloc)
                if check_PPack == False:
                    print("pop_pack_item_loc")
                    conn.cursor().close()
                    return False, L_func_name+":"+str(O_status)+": "+ str(err_msg)

            return True, ""
    except Exception as argument:
        err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(argument)
        print(err_return)        
        conn.rollback()
        conn.cursor().close()
        return False, err_return


#----------------------------------------------------------
# FUNCTION ins_sku_for_pack_aacdt
#----------------------------------------------------------

def ins_sku_for_pack_aacdt (conn, 
                          I_alloc_id):
    L_func_name = 'ins_sku_for_pack_aacdt'
    O_status  = 0
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculation_setup_queries.yaml') as fh:
            queries                     = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_ins_calc_destination_temp = queries['ins_sku_for_pack_aacdt']['Q_ins_calc_destination_temp']
            cursor = conn.cursor()
            #status
            O_status = 1
            cursor.execute (Q_ins_calc_destination_temp, (I_alloc_id,))
            print("No of records inserted: ", cursor.rowcount)                
            conn.commit()
            conn.cursor().close()
            return True, "" 
    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserted data in alloc_calc_destination_temp: "+ str(error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

#----------------------------------------------------------
# FUNCTION pop_pack_item_loc
#----------------------------------------------------------

def pop_pack_item_loc (conn, 
                          I_alloc_id):
    L_func_name = 'pop_pack_item_loc'
    O_status  = 0
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculation_setup_queries.yaml') as fh:
            queries                           = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_del_pack_item_loc_need          = queries['pop_pack_item_loc']['Q_del_pack_item_loc_need']
            Q_del_alloc_comp_inv              = queries['pop_pack_item_loc']['Q_del_alloc_comp_inv']
            Q_ins_pack_item_loc_need          = queries['pop_pack_item_loc']['Q_ins_pack_item_loc_need']
            Q_ins_alloc_comp_inv              = queries['pop_pack_item_loc']['Q_ins_alloc_comp_inv']
            Q_del_alloc_calc_destination_temp = queries['pop_pack_item_loc']['Q_del_alloc_calc_destination_temp']
            cursor = conn.cursor()
            #status
            O_status = 1
            cursor.execute (Q_del_pack_item_loc_need, (I_alloc_id,))
            print("No of records Deleted: ", cursor.rowcount)
            #status
            O_status = 2
            cursor.execute (Q_del_alloc_comp_inv, (I_alloc_id,))
            print("No of records Deleted: ", cursor.rowcount)
            #status
            O_status = 3
            cursor.execute (Q_ins_pack_item_loc_need, (I_alloc_id,))
            print("No of records Inserted: ", cursor.rowcount)
            #status
            O_status = 4
            cursor.execute (Q_ins_alloc_comp_inv, (I_alloc_id,))
            print("No of records Inserted: ", cursor.rowcount)
            #status
            O_status = 5
            cursor.execute (Q_del_alloc_calc_destination_temp, (I_alloc_id,))
            print("No of records Deleted: ", cursor.rowcount)
            conn.commit()
            conn.cursor().close()
            return True, ""
    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while deleting data in pack_item_loc_need: "+ str(error)
        if O_status == 2:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while deleting data in alloc_comp_inv: "+ str(error)
        if O_status == 3:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting data in pack_item_loc_need: "+ str(error)
        if O_status == 4: 
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting data in alloc_comp_inv: "+ str(error)
        if O_status == 5:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while deleting data in alloc_calc_destination_temp: "+ str(error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(error)

        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return
