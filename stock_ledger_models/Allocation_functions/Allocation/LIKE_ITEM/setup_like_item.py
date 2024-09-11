from ..INVENTORY_SETUP.update_alloc_ext import update_alloc
from ..INVENTORY_SETUP.inventory_setup import setup_location
import pandas as pd
import yaml

#############################################################
# Created By - Priyanshu Pandey                             #                
# File Name - calculate.py                                  #
# Purpose - calculate allocation                            #
#############################################################

#--------------------------------------------------------------
# Function to map like items 
#--------------------------------------------------------------

def map_like_item_details(conn
                          ,I_alloc
                          ,I_item_list
                          ,I_item_parent         #for style diff
                          ,I_sku                 
                          ,I_diff_id             #for style diff
                          ,I_no_sizes            
                          ,I_weight              
                          ,I_size_prf_ind        #for style diff
                          ,O_status):
    L_func_name ="map_like_item_details"
    O_status = 0
    emp_list = list()
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/like_item_queries.yaml') as fh:
            queries                      = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_chk_alloc                  = queries['map_like_item_details']['Q_chk_alloc']
            Q_ins_diff_map_item_list_sku = queries['map_like_item_details']['Q_ins_diff_map_item_list_sku']
            Q_ins_diff_map_item_sku      = queries['map_like_item_details']['Q_ins_diff_map_item_sku']
            Q_del_diff_map_item_sku      = queries['map_like_item_details']['Q_del_diff_map_item_sku']
            Q_sku_map_ins_loop           = queries['map_like_item_details']['Q_sku_map_ins_loop']
            Q_del_map_itm_list           = queries['map_like_item_details']['Q_del_map_itm_list']
            Q_fetch_item_diff_temp       = queries['map_like_item_details']['Q_fetch_item_diff_temp']
            Q_fetch_item_diff_map_temp   = queries['map_like_item_details']['Q_fetch_item_diff_map_temp']
            Q_validate_item              = queries['map_like_item_details']['Q_validate_item']
            Q_val_style                  = queries['map_like_item_details']['Q_val_style']
            Q_ins_map_style              = queries['map_like_item_details']['Q_ins_map_style']
            Q_del_diff_map_item_style    = queries['map_like_item_details']['Q_del_diff_map_item_style']

            #status
            O_status = 1 
            mycursor = conn.cursor()

            df_chk_alloc = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))
            df_item_loop = pd.read_sql(Q_sku_map_ins_loop,conn,params=(I_alloc,))

            if df_chk_alloc.alloc_level[0] =='T':
                #status
                O_status = 2
                if I_sku != None:
                    O_status = 3
                    if len(df_item_loop)>0:
                        #status
                        O_status = 4
                        for i in range(len(df_item_loop)):
                            L_item = df_item_loop.loc[i,"ITEM"]
                            #Q_validate_item
                            df_val_item = pd.read_sql(Q_validate_item,conn,params=(I_sku,L_item))
                            if len(df_val_item)>0:
                                #print("invalid mapping item")
                                return emp_list,L_func_name+" -"+str(O_status)+": invalid mapping item"

                            mycursor.execute(Q_ins_diff_map_item_sku,(I_weight,I_alloc,L_item,I_sku))
                            L_count = mycursor.rowcount
                            print(L_func_name,"-",O_status,"-","rows_affected: ",L_count)

                            #status
                            O_status = 5
                            if L_count>0:
                                mycursor.execute(Q_del_diff_map_item_sku,(I_alloc,L_item))
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                if I_item_list !=None:
                    #status
                    O_status = 6
                    mycursor.execute(Q_ins_diff_map_item_list_sku,(I_weight,I_alloc,I_item_list))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                    #status
                    O_status = 7
                    #Q_del_map_itm_list
                    for i in range(len(df_item_loop)):
                        L_item = df_item_loop.loc[i,"ITEM"]
                        mycursor.execute(Q_del_map_itm_list,(I_alloc,L_item,I_alloc))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            else:
                if len(df_item_loop)>0:
                    #status
                    O_status = 4
                    for i in range(len(df_item_loop)):
                        L_item = df_item_loop.loc[i,"ITEM"]
                        L_diff = df_item_loop.loc[i,"DIFF_ID"]
                        L_no_of_sizes = df_item_loop.loc[i,"NO_OF_SIZES"]
                        params=(I_item_parent,I_diff_id,I_no_sizes,I_item_parent,I_diff_id,I_no_sizes,I_item_parent,I_diff_id,I_no_sizes,I_item_parent,I_diff_id,I_no_sizes,I_item_parent,I_diff_id,I_no_sizes,L_item,L_diff,L_no_of_sizes,L_item,L_diff,L_no_of_sizes,L_item,L_diff,L_no_of_sizes,L_item,L_diff,L_no_of_sizes,L_item,L_diff,L_no_of_sizes,)
                        df_val_item = pd.read_sql(Q_val_style,conn,params=params)
                        if len(df_val_item)==0:
                            print("invalid mapping item")
                            return emp_list,L_func_name+" -"+str(O_status)+": invalid mapping item"

                        mycursor.execute(Q_ins_map_style,(I_diff_id,I_weight,I_size_prf_ind,I_alloc,L_item,L_diff,L_no_of_sizes,I_item_parent))
                        L_count = mycursor.rowcount
                        print(L_func_name,"-",O_status,"-","rows_affected: ",L_count)

                        #status
                        O_status = 5
                        if L_count>0:
                            mycursor.execute(Q_del_diff_map_item_style,(I_alloc,L_item,L_diff,L_no_of_sizes))
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 8
            conn.commit()
            df_item_diff_temp     = pd.read_sql(Q_fetch_item_diff_temp,conn,params=(I_alloc,))
            df_item_diff_map_temp = pd.read_sql(Q_fetch_item_diff_map_temp,conn,params=(I_alloc,))

            conn.commit()
            conn.cursor().close()
            return [df_item_diff_temp,df_item_diff_map_temp],''

    except Exception as error:
        err_return = ""
        if O_status==1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while creating table: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while creating table: ", error)
        elif O_status>=2 or O_status <=4:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting alloc_like_item_diff_map_temp for sku: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while inserting alloc_like_item_diff_map_temp for sku: ", error)
        elif O_status==5:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while deleting alloc_like_item_diff_temp for sku: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while deleting alloc_like_item_diff_temp for sku: ", error)
        elif O_status==6:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting alloc_like_item_diff_map_temp for skulist: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while inserting alloc_like_item_diff_map_temp for skulist: ", error)
        elif O_status==7:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while deleting alloc_like_item_diff_temp for sku: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while deleting alloc_like_item_diff_temp for sku: ", error)
        elif O_status==8:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while processing dataframe for output: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while processing dataframe for output: ", error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured in: "+ str(error)
            #print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
        conn.rollback()
        conn.cursor().close()
        return emp_list,err_return
#--------------------------------------------------------------
# Function to insert mapped like items into DB
#--------------------------------------------------------------

def insert_like_item_map(conn
                         ,I_alloc
                         ,O_status):
    L_func_name ="insert_like_item_map"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/like_item_queries.yaml') as fh:
            queries             = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_chk_alloc         = queries['insert_like_item_map']['Q_chk_alloc']
            Q_merge_calc_source = queries['insert_like_item_map']['Q_merge_calc_source']
            Q_merge_calc_source_style_1 = queries['insert_like_item_map']['Q_merge_calc_source_style_1']  #Changes for style/diff  by shubham#
            Q_merge_calc_source_style_2 = queries['insert_like_item_map']['Q_merge_calc_source_style_2']  #Changes for style/diff  by shubham#
            Q_upd_calc_source   = queries['insert_like_item_map']['Q_upd_calc_source']
            Q_upd_src_ext       = queries['insert_like_item_map']['Q_upd_src_ext']
            Q_del_src_ext       = queries['insert_like_item_map']['Q_del_src_ext']
            Q_ins_src_ext       = queries['insert_like_item_map']['Q_ins_src_ext']
            #Q_chk_dest_range    = queries['insert_like_item_map']['Q_chk_dest_range']
            Q_chk_alloc_rule    = queries['insert_like_item_map']['Q_chk_alloc_rule']

            mycursor = conn.cursor()
            L_row_cnt = 0
            #status
            O_status = 2
            df_chk_alloc = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))

            if df_chk_alloc.alloc_level[0] =='T':
                #status
                O_status = 3

                #Q_merge_calc_source
                mycursor.execute(Q_merge_calc_source,(I_alloc,))
                L_row_cnt = mycursor.rowcount
                print(L_func_name,"-",O_status,"-","rows_affected: ",L_row_cnt)
            #Changes for style/diff  by shubham Start#
                #if L_row_cnt>0:
                #    L_row_cnt =1
            else:
                #status
                O_status = 3.1                
                #Q_merge_calc_source
                mycursor.execute(Q_merge_calc_source_style_1,(I_alloc,))
                L_row_cnt = mycursor.rowcount
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            if L_row_cnt>0:
                L_row_cnt =1
            
            if df_chk_alloc.alloc_level[0] =='D':
                #status
                O_status = 3.2
                #Q_merge_calc_source
                mycursor.execute(Q_merge_calc_source_style_2,(I_alloc,I_alloc,I_alloc,I_alloc,I_alloc,I_alloc,I_alloc,I_alloc,I_alloc))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #Changes for style/diff  by shubham End#
            #status
            O_status = 4
            #Q_upd_calc_source
            mycursor.execute(Q_upd_calc_source,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 5
            #Q_upd_src_ext
            mycursor.execute(Q_upd_src_ext,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            if L_row_cnt ==1:
                #status
                O_status = 6
                #Q_del_src_ext
                mycursor.execute(Q_del_src_ext,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                #status
                O_status = 7
                #Q_ins_src_ext
                mycursor.execute(Q_ins_src_ext,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                I_input_data = list()
                update_Check, err_msg = update_alloc(conn,
                                O_status,
                                I_alloc,
                                None,
                                None,
                                'Y',
                                I_input_data)
                if update_Check ==False:
                    #status
                    O_status=8
                    print(O_status,"- update_alloc failed")
                    conn.rollback()
                    conn.cursor().close()                     
                    return False,L_func_name+": "+str(O_status)+ ": "+ err_msg

                #status
                O_status = 9
                ##Q_chk_dest_range
                #df_dest_ranged = pd.read_sql(Q_chk_dest_range,conn,params=(I_alloc,))

                #if df_dest_ranged.chk[0] == 1:
                #status
                O_status = 10
                #Q_chk_dest_range
                df_chk_rule = pd.read_sql(Q_chk_alloc_rule,conn,params=(I_alloc,))

                if len(df_chk_rule)>0:
                    #status
                    O_status=11
                    SL_Check , err_msg = setup_location(conn
                                        ,I_alloc
                                        ,O_status)
                    if SL_Check ==False:
                        #status
                        O_status=12
                        print(O_status,"- setup_location failed")
                        conn.rollback()
                        conn.cursor().close()                     
                        return False, L_func_name+": "+str(O_status)+": " +err_msg

            conn.commit()
            conn.cursor().close()                     
            return True,''

    except Exception as error:
        err_return=""
        if O_status<=2:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while fetching alloc_level: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while fetching alloc_level: ", error)
        elif O_status==3:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while merging alloc_calc_source_temp: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while merging alloc_calc_source_temp: ", error)
        #Changes for style/diff  by shubham Start#
        elif O_status==3.1 or O_status==3.2:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while merging alloc_calc_source_temp for style/diff: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while merging alloc_calc_source_temp for style/diff: ", error)
        #Changes for style/diff  by shubham End#
        elif O_status==4:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating alloc_calc_source_temp: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while updating alloc_calc_source_temp: ", error)
        elif O_status==5:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating alloc_like_item_source: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while updating alloc_like_item_source: ", error)
        elif O_status==6:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while deleting alloc_like_item_source: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while deleting alloc_like_item_source: ", error)
        elif O_status==7:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting alloc_like_item_source "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while inserting alloc_like_item_source: ", error)
        elif O_status==8:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while processing update_alloc function: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while processing update_alloc function: ", error)
        elif O_status==9:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while fetching data from alloc_calc_allitemloc_temp: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while fetching data from alloc_calc_allitemloc_temp: ", error)
        elif O_status==10:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while fetching data from alloc_rule: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while fetching data from alloc_rule: ", error)
        elif O_status==11:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while processing setup_location function: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while processing setup_location function: ", error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured in: "+ str(error)
            #print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
        conn.rollback()
        conn.cursor().close()                     
        return False, err_return

#--------------------------------------------------------------
# Function to delete mapped like items from screen
#--------------------------------------------------------------

def delete_like_item_map(conn
                         ,I_alloc
                         ,O_status):
    L_func_name ="delete_like_item_map"
    O_status = 0
    L_count = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/like_item_queries.yaml') as fh:
            queries                    = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_chk_alloc                = queries['delete_like_item_map']['Q_chk_alloc']
            Q_ins_item_sku             = queries['delete_like_item_map']['Q_ins_item_sku']
            Q_ins_item_style           = queries['delete_like_item_map']['Q_ins_item_style']  #Changes for style/diff  by shubham#
            Q_del_map_item             = queries['delete_like_item_map']['Q_del_map_item']
            Q_fetch_item_diff_temp     = queries['delete_like_item_map']['Q_fetch_item_diff_temp']
            Q_fetch_item_diff_map_temp = queries['delete_like_item_map']['Q_fetch_item_diff_map_temp']

            mycursor = conn.cursor()
            #status
            O_status = 1
            print(Q_chk_alloc,(I_alloc,))
            df_chk_alloc = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))
            print(df_chk_alloc)
            if df_chk_alloc.alloc_level[0] =='T':
                #status
                O_status = 2
                #Q_ins_item_sku
                mycursor.execute(Q_ins_item_sku,(I_alloc,))
                L_count = mycursor.rowcount
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #Changes for style/diff  by shubham start#
            else:
                #status
                O_status = 2.1
                #Q_ins_item_sku
                mycursor.execute(Q_ins_item_style,(I_alloc,))
                L_count = mycursor.rowcount
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #Changes for style/diff  by shubham end#
            #Q_del_map_item
            #status
            O_status = 3
            if L_count>0:
                mycursor.execute(Q_del_map_item,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()

            #dataframe
            #status
            O_status = 4
            df_item_diff_temp     = pd.read_sql(Q_fetch_item_diff_temp,conn,params=(I_alloc,))
            df_item_diff_map_temp = pd.read_sql(Q_fetch_item_diff_map_temp,conn,params=(I_alloc,))

            conn.commit()
            return [df_item_diff_temp,df_item_diff_map_temp], ''

    except Exception as error:
        err_return=""
        if O_status<=1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while retrieving data from alloc_head: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while retrieving data from alloc_head: ", error)
        elif O_status==2:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting alloc_like_item_diff_temp: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while inserting alloc_like_item_diff_temp: ", error)
        #Changes for style/diff  by shubham Start#
        elif O_status==2.1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting alloc_like_item_diff_temp for style/diff: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while inserting alloc_like_item_diff_temp for style/diff: ", error)
        #Changes for style/diff  by shubham end#
        elif O_status==3:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while deleting alloc_like_item_diff_map_temp: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while deleting alloc_like_item_diff_map_temp: ", error)
        elif O_status==4:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while processing output dataframe: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while processing output dataframe: ", error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured in: "+ str(error)
            #print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
        conn.rollback()
        return [],err_return

#--------------------------------------------------------------------------------
#Created By - Naveen Ramanathan                                                 
#Purpose    - Retrieving data for like item screen                              
#---------------------------------------------------------------------------------
def RETREIVE_LIKE_ITEM_MAP(conn,I_alloc_no):
    O_status = 0
    L_fun = "RETREIVE_LIKE_ITEM_MAP"
    print("EXECUTING: ",L_fun)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_like_item_queries.yaml') as fh:
            queries       = yaml.load(fh, Loader=yaml.SafeLoader)
            C_temp_tbl1   = queries['retreive_like_item_map']['C_temp_tbl1']
            C_temp_tbl2   = queries['retreive_like_item_map']['C_temp_tbl2']
            C_temp_tbl3   = queries['retreive_like_item_map']['C_temp_tbl3']
            C_alloc_level = queries['retreive_like_item_map']['C_alloc_level']
            L_del_1       = queries['retreive_like_item_map']['L_del_1']
            L_del_2       = queries['retreive_like_item_map']['L_del_2']
            L_del_3       = queries['retreive_like_item_map']['L_del_3']
            L_del_4       = queries['retreive_like_item_map']['L_del_4']
            L_ins_1       = queries['retreive_like_item_map']['L_ins_1']
            L_ins_2       = queries['retreive_like_item_map']['L_ins_2']            
            L_ins_2_style       = queries['retreive_like_item_map']['L_ins_2_style']  #Changes for style/diff  by shubham#
            L_ins_4       = queries['retreive_like_item_map']['L_ins_4']
            L_ins_5       = queries['retreive_like_item_map']['L_ins_5']
            C_left_out    = queries['retreive_like_item_map']['C_left_out']
            C_right_out   = queries['retreive_like_item_map']['C_right_out']

            mycursor = conn.cursor()

            mycursor.execute("SET sql_mode = '';")
            #status
            O_status = 1 
            mycursor.execute(C_temp_tbl1)
            mycursor.execute(C_temp_tbl2)
            mycursor.execute(C_temp_tbl3)
            #status
            O_status = 2
            df_alloc_level = pd.read_sql(C_alloc_level,conn,params=(I_alloc_no,))
            L_alloc_level = df_alloc_level.alloc_level[0]
            #status
            O_status = 3
            mycursor.execute(L_ins_1,(I_alloc_no,))
            L_rc = mycursor.rowcount
            print(O_status,"-","rows_affected: ",L_rc) 

            #if L_rc>0:
            #status
            O_status = 4
            mycursor.execute(L_del_1,(I_alloc_no,))
            print(O_status,"-","rows_affected: ",mycursor.rowcount) 
            if L_alloc_level == 'T':
                #status
                O_status = 5
                mycursor.execute(L_ins_2,(I_alloc_no,))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)
                #status
                O_status = 6
                mycursor.execute(L_del_2,(I_alloc_no,))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)
            #Changes for style/diff  by shubham start#
            else:
                #status
                O_status = 6.1
                mycursor.execute(L_ins_2_style,(I_alloc_no,))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)
            #Changes for style/diff  by shubham end#
            #status
            O_status = 7
            mycursor.execute(L_del_3,(I_alloc_no,))
            print(O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 8
            mycursor.execute(L_ins_4,(I_alloc_no,))
            print(O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 9
            mycursor.execute(L_del_4,(I_alloc_no,))
            print(O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 10
            mycursor.execute(L_ins_5,(I_alloc_no,))
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

            conn.commit()
            df_left_out = pd.read_sql(C_left_out,conn,params=(I_alloc_no,))
            df_right_out = pd.read_sql(C_right_out,conn,params =(I_alloc_no,))

            return df_left_out,df_right_out,''
            
    except Exception as error:
        err_return=""
        if O_status==1: 
            err_return = L_fun+":"+str(O_status)+": Exception raised during temporary table creation: "+ str(error)
            #print(L_fun,":",O_status,":","Exception raised during temporary table creation:", error)
        elif O_status==2:
            err_return = L_fun+":"+str(O_status)+": Exception raised during execution of alloc_level cursor: "+ str(error)
            #print(L_fun,":",O_status,":","Exception raised during execution of alloc_level cursor:", error)
        elif O_status==3:
            err_return = L_fun+":"+str(O_status)+": Exception raised during data insertion into alloc_item_source table: "+ str(error)
            #print(L_fun,":",O_status,":","Exception raised during data insertion into alloc_item_source table:", error)
        elif O_status==4:
            err_return = L_fun+":"+str(O_status)+": Exception raised during data deletion from alloc_like_item_diff_temp table: "+ str(error)
            #print(L_fun,":",O_status,":","Exception raised during data deletion from alloc_like_item_diff_temp table:", error)
        elif O_status==5:
            err_return = L_fun+":"+str(O_status)+": Exception raised during data insertion into alloc_like_item_diff_temp table: "+ str(error)
            #print(L_fun,":",O_status,":","Exception raised during data insertion into alloc_like_item_diff_temp table:", error)
        elif O_status==6 or O_status==7:
            err_return = L_fun+":"+str(O_status)+": Exception raised during data deletion from alloc_like_item_diff_temp or alloc_like_item_diff_map_temp table: "+ str(error)
            #print(L_fun,":",O_status,":","Exception raised during data deletion from alloc_like_item_diff_temp or alloc_like_item_diff_map_temp table:", error)
        #Changes for style/diff  by shubham  start#
        elif O_status==6.1: 
            err_return = L_fun+":"+str(O_status)+": Exception raised during data insertion into alloc_like_item_diff_temp table for style/diff: "+ str(error)
            #print(L_fun,":",O_status,":","Exception raised during data insertion into alloc_like_item_diff_temp table for style/diff", error)
        #Changes for style/diff  by shubham  end#
        elif O_status==8:
            err_return = L_fun+":"+str(O_status)+": Exception raised during data insertion into alloc_like_item_diff_map_temp table: "+ str(error)
            #print(L_fun,":",O_status,":","Exception raised during data insertion into alloc_like_item_diff_map_temp table:", error)
        elif O_status==9:
            err_return = L_fun+":"+str(O_status)+": Exception raised during data deletion from alloc_like_item_map_temp table: "+ str(error)
            #print(L_fun,":",O_status,":","Exception raised during data deletion from alloc_like_item_map_temp table:", error)
        elif O_status==10:
            err_return = L_fun+":"+str(O_status)+": Exception raised during data insertion into alloc_like_item_map_temp table: "+ str(error)
            #print(L_fun,":",O_status,":","Exception raised during data insertion into alloc_like_item_map_temp table:", error)
        else:
            err_return = L_fun+":"+str(O_status)+": Exception Occured: "+ str(error)
            #print(L_fun,":",O_status,":","Exception Occured: ", error)
        return [],err_return        