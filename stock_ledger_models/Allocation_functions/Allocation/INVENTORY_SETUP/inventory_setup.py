from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
from ..CREATE_SCREEN.populate_search_result import fetch_inventory
from datetime import datetime
import pandas as pd
import yaml

##################################################################################
#Created By - Priyanshu Pandey                                                   #
#File Name - inventory_setup.py                                                  #
#Purpose - Contains all the functions to setup inventory before calculation      #
##################################################################################

#--------------------------------------------------------------
# Function to setup destination item 
#--------------------------------------------------------------
#Changes by Shubham Start#
# def setup_item_location (conn
                         # ,I_alloc
                         # ,O_status):
    # L_func_name ="setup_item_location"
    # O_status = 0
    # print("EXECUTING: ",L_func_name)
    # try:
        # with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/inventory_setup_queries.yaml') as fh:
            # queries = yaml.load(fh, Loader=yaml.SafeLoader)
            # Q_delete_calc_source      = queries['setup_item_location']['Q_delete_calc_source']
            # Q_fetch_calc_like_source  = queries['setup_item_location']['Q_fetch_calc_like_source']
            # Q_fetch_calc_item_source  = queries['setup_item_location']['Q_fetch_calc_item_source']
            # Q_fetch_calc_source       = queries['setup_item_location']['Q_fetch_calc_source']
            # Q_insert_calc_source      = queries['setup_item_location']['Q_insert_calc_source']
            # Q_like_item_chk           = queries['setup_item_location']['Q_like_item_chk']
            # Q_chk_alloc               = queries['setup_location']['Q_chk_alloc']

            # #status
            # O_status =1
            # mycursor = conn.cursor()
            # mycursor.execute(Q_delete_calc_source,(I_alloc,))
            # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            # #status
            # O_status =2

            # mycursor.execute(Q_chk_alloc,(I_alloc,))
            # my_alloc_head = mycursor.fetchall()

            # df_alloc_head = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))
            # #status
            # O_status =3

            # if len(my_alloc_head)>0:
                # L_alloc_level = df_alloc_head.alloc_level[0]

                # mycursor.execute(Q_like_item_chk,(I_alloc,))
                # my_like_item_chk = mycursor.fetchall()
                # #status
                # O_status =4

                # if L_alloc_level == 'T':
                    # #status
                    # O_status =5

                    # if my_like_item_chk == 1:
                        # df_calc_like_source = pd.read_sql(Q_fetch_calc_like_source,conn,params=(I_alloc,))
                        # #status
                        # O_status =6

                        # if len(df_calc_like_source)>0:
                            # #status
                            # O_status =7

                            # for i in range(len(df_calc_like_source)):
                                # #status
                                # O_status =8

                                # L_insert_calc_source_temp = (df_calc_like_source.loc[i, "alloc_no"],             df_calc_like_source.loc[i, "item_source_id"],          df_calc_like_source.loc[i, "release_date"], 
                                                             # df_calc_like_source.loc[i, "item_type"],            df_calc_like_source.loc[i, "source_item"],             df_calc_like_source.loc[i, "source_item_level"],
                                                             # df_calc_like_source.loc[i, "source_tran_level"],    df_calc_like_source.loc[i, "source_pack_ind"],         df_calc_like_source.loc[i, "source_diff1_id"],
                                                             # df_calc_like_source.loc[i, "source_diff2_id"],      df_calc_like_source.loc[i, "source_diff3_id"],         df_calc_like_source.loc[i, "source_diff4_id"], 
                                                             # df_calc_like_source.loc[i, "tran_item"],            df_calc_like_source.loc[i, "tran_item_level"],         df_calc_like_source.loc[i, "tran_tran_level"],
                                                             # df_calc_like_source.loc[i, "tran_pack_ind"],        df_calc_like_source.loc[i, "tran_diff1_id"],           df_calc_like_source.loc[i, "tran_diff2_id"], 
                                                             # df_calc_like_source.loc[i, "tran_diff3_id"],        df_calc_like_source.loc[i, "tran_diff4_id"],           df_calc_like_source.loc[i, "hier1"], 
                                                             # df_calc_like_source.loc[i, "hier2"],                df_calc_like_source.loc[i, "hier3"],                   df_calc_like_source.loc[i, "pack_no"], 
                                                             # df_calc_like_source.loc[i, "like_source_item"],     df_calc_like_source.loc[i, "like_source_item_level"],  df_calc_like_source.loc[i, "like_source_tran_level"],
                                                             # df_calc_like_source.loc[i, "like_source_pack_ind"], df_calc_like_source.loc[i, "like_source_diff1_id"],    df_calc_like_source.loc[i, "like_source_diff2_id"],
                                                             # df_calc_like_source.loc[i, "like_source_diff3_id"], df_calc_like_source.loc[i, "like_source_diff4_id"],    df_calc_like_source.loc[i, "like_tran_item"],
                                                             # df_calc_like_source.loc[i, "like_tran_item_level"], df_calc_like_source.loc[i, "like_tran_tran_level"],    df_calc_like_source.loc[i, "like_tran_pack_ind"],
                                                             # df_calc_like_source.loc[i, "like_tran_diff1_id"],   df_calc_like_source.loc[i, "like_tran_diff2_id"],      df_calc_like_source.loc[i, "like_tran_diff3_id"],
                                                             # df_calc_like_source.loc[i, "like_tran_diff4_id"],   df_calc_like_source.loc[i, "like_hier1"],              df_calc_like_source.loc[i, "like_hier2"],
                                                             # df_calc_like_source.loc[i, "like_hier3"],           df_calc_like_source.loc[i, "like_pack_no"],            df_calc_like_source.loc[i, "like_item_weight"],
                                                             # df_calc_like_source.loc[i, "like_size_prof_ind"],   df_calc_like_source.loc[i, "create_id"],               df_calc_like_source.loc[i, "create_datetime"],
                                                             # df_calc_like_source.loc[i, "last_update_id"],       df_calc_like_source.loc[i, "last_update_datetime"],    df_calc_like_source.loc[i, "som_qty"])
                                # #insert calc source temp
                                # L_insert_calc_source_temp = convert_numpy(L_insert_calc_source_temp)
                                # mycursor.execute(Q_insert_calc_source,L_insert_calc_source_temp)  
                                # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                                # #status
                                # O_status =9
                    # else:
                        # #status
                        # O_status =10

                        # #mycursor.execute(Q_fetch_calc_item_source,(I_alloc,))
                        # df_calc_item_source = pd.read_sql(Q_fetch_calc_item_source,conn,params=(I_alloc,))
                        # #status
                        # O_status =11

                        # if len(df_calc_item_source)>0:
                            # #status
                            # O_status =12

                            # for i in range(len(df_calc_item_source)):
                                # L_insert_calc_source_temp = (df_calc_item_source.loc[i, "alloc_no"],             df_calc_item_source.loc[i, "item_source_id"],          df_calc_item_source.loc[i, "release_date"], 
                                                             # df_calc_item_source.loc[i, "item_type"],            df_calc_item_source.loc[i, "source_item"],             df_calc_item_source.loc[i, "source_item_level"],
                                                             # df_calc_item_source.loc[i, "source_tran_level"],    df_calc_item_source.loc[i, "source_pack_ind"],         df_calc_item_source.loc[i, "source_diff1_id"],
                                                             # df_calc_item_source.loc[i, "source_diff2_id"],      df_calc_item_source.loc[i, "source_diff3_id"],         df_calc_item_source.loc[i, "source_diff4_id"], 
                                                             # df_calc_item_source.loc[i, "tran_item"],            df_calc_item_source.loc[i, "tran_item_level"],         df_calc_item_source.loc[i, "tran_tran_level"],
                                                             # df_calc_item_source.loc[i, "tran_pack_ind"],        df_calc_item_source.loc[i, "tran_diff1_id"],           df_calc_item_source.loc[i, "tran_diff2_id"], 
                                                             # df_calc_item_source.loc[i, "tran_diff3_id"],        df_calc_item_source.loc[i, "tran_diff4_id"],           df_calc_item_source.loc[i, "hier1"], 
                                                             # df_calc_item_source.loc[i, "hier2"],                df_calc_item_source.loc[i, "hier3"],                   df_calc_item_source.loc[i, "pack_no"], 
                                                             # df_calc_item_source.loc[i, "like_source_item"],     df_calc_item_source.loc[i, "like_source_item_level"],  df_calc_item_source.loc[i, "like_source_tran_level"],
                                                             # df_calc_item_source.loc[i, "like_source_pack_ind"], df_calc_item_source.loc[i, "like_source_diff1_id"],    df_calc_item_source.loc[i, "like_source_diff2_id"],
                                                             # df_calc_item_source.loc[i, "like_source_diff3_id"], df_calc_item_source.loc[i, "like_source_diff4_id"],    df_calc_item_source.loc[i, "like_tran_item"],
                                                             # df_calc_item_source.loc[i, "like_tran_item_level"], df_calc_item_source.loc[i, "like_tran_tran_level"],    df_calc_item_source.loc[i, "like_tran_pack_ind"],
                                                             # df_calc_item_source.loc[i, "like_tran_diff1_id"],   df_calc_item_source.loc[i, "like_tran_diff2_id"],      df_calc_item_source.loc[i, "like_tran_diff3_id"],
                                                             # df_calc_item_source.loc[i, "like_tran_diff4_id"],   df_calc_item_source.loc[i, "like_hier1"],              df_calc_item_source.loc[i, "like_hier2"],
                                                             # df_calc_item_source.loc[i, "like_hier3"],           df_calc_item_source.loc[i, "like_pack_no"],            df_calc_item_source.loc[i, "like_item_weight"],
                                                             # df_calc_item_source.loc[i, "like_size_prof_ind"],   df_calc_item_source.loc[i, "create_id"],               df_calc_item_source.loc[i, "create_datetime"],
                                                             # df_calc_item_source.loc[i, "last_update_id"],       df_calc_item_source.loc[i, "last_update_datetime"],    df_calc_item_source.loc[i, "som_qty"])
                                # #insert calc source temp
                                # L_insert_calc_source_temp = convert_numpy(L_insert_calc_source_temp)
                                # mycursor.execute(Q_insert_calc_source,L_insert_calc_source_temp)  
                                # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                # #fetching inserted record
                # #returning inserted data
                # #status
                # O_status =13

                # #df_calc_source = pd.read_sql(Q_fetch_calc_source,conn,params=(I_alloc,))

                # #if len(df_calc_source)>0:
                # #    #status
                # #    O_status =14
                
                # #commit
                # conn.commit()
                # conn.cursor().close()
                # return True

            # else:
                # #status
                # O_status =404
                # print("No record found: ",L_func_name)
                # print(O_status)
                # conn.cursor().close()
                # return False

    # except Exception as argument:
        # print(L_func_name,O_status)
        # print("Exception occured in: ",L_func_name,argument)
        # conn.rollback()
        # conn.cursor().close()
        # return False

def setup_item_location (conn
                         ,I_alloc
                         ,O_status):
    L_func_name ="setup_item_location"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/inventory_setup_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_delete_calc_source      = queries['setup_item_location']['Q_delete_calc_source']
            Q_insert_calc_source      = queries['setup_item_location']['Q_insert_calc_source']
            Q_insert_calc_source_style = queries['setup_item_location']['Q_insert_calc_source_style']
            Q_merge_calc_source_1     = queries['setup_item_location']['Q_merge_calc_source_1']
            Q_merge_calc_source_2     = queries['setup_item_location']['Q_merge_calc_source_2']
            Q_del_calc_source_temp_1  = queries['setup_item_location']['Q_del_calc_source_temp_1']
            Q_del_calc_source_temp_2  = queries['setup_item_location']['Q_del_calc_source_temp_2']
            Q_del_calc_source_temp_3  = queries['setup_item_location']['Q_del_calc_source_temp_3']
            Q_chk_alloc               = queries['setup_location']['Q_chk_alloc']

            O_status =1
            mycursor = conn.cursor()
            mycursor.execute(Q_delete_calc_source,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            O_status =2

            mycursor.execute(Q_chk_alloc,(I_alloc,))
            my_alloc_head = mycursor.fetchall()

            
            df_alloc_head = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))
            O_status =3

            if len(my_alloc_head)>0:
                L_alloc_level = df_alloc_head.alloc_level[0]
                L_alloc_criteria = df_alloc_head.alloc_criteria[0]

                O_status =4
                
                if L_alloc_level == 'T':
                    O_status =5                    
                    mycursor.execute(Q_insert_calc_source,(I_alloc,))  
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                else:
                    O_status =6                    
                    mycursor.execute(Q_insert_calc_source_style,(I_alloc,))  
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)               
                
                O_status =7
                conn.commit()
                conn.cursor().close()
                if L_alloc_criteria == 'P':
                    O_status =8
                    mycursor.execute(Q_del_calc_source_temp_1,(I_alloc,))  
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    
                elif L_alloc_criteria == 'A':
                    O_status =9
                    mycursor.execute(Q_del_calc_source_temp_2,(I_alloc,))  
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)    
                    
                elif L_alloc_criteria == 'T':
                    O_status =10
                    mycursor.execute(Q_del_calc_source_temp_3,(I_alloc,))  
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    
                O_status =11                    
                mycursor.execute(Q_merge_calc_source_1,(I_alloc,L_alloc_level))  
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)  

                O_status = 12
                mycursor.execute(Q_merge_calc_source_2,(I_alloc,I_alloc,I_alloc,I_alloc,I_alloc,I_alloc,I_alloc,I_alloc,I_alloc))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                
                O_status =13
                conn.commit()
                conn.cursor().close()
                return True,""
            else:
                O_status =404
                print("No record found: ",L_func_name)
                print(O_status)
                return False,L_func_name + ": No record found"

    except Exception as error:
        err_return = ""
        if O_status ==1:
            print(L_func_name,":",O_status,":","Exception occured before deleteing from table alloc_calc_source_temp: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured before deleteing from table alloc_calc_source_temp :"+ str(error)
        elif O_status==2:
            print(L_func_name,":",O_status,":","Exception occured befor fetching data from table alloc_head: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured befor fetching data from table alloc_head :"+ str(error)
        elif O_status==3:
            print(L_func_name,":",O_status,":","Exception occured before first if condition: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured before first if condition :"+ str(error)
        elif O_status==4:
            print(L_func_name,":",O_status,":","Exception occured before second if condition: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured before second if condition :"+ str(error)
        elif O_status==5:
            print(L_func_name,":",O_status,":","Exception occured while inserting into alloc_calc_source_temp table for SKU: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting into alloc_calc_source_temp table for SKU :"+ str(error)
        elif O_status==6:
            print(L_func_name,":",O_status,":","Exception occured while inserting into alloc_calc_source_temp table for STYLE/DIFF: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting into alloc_calc_source_temp table for STYLE/DIFF :"+ str(error)
        elif O_status==7:
            print(L_func_name,":",O_status,":","Exception occured before commit: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured before commit :"+ str(error)
        elif O_status==8:
            print(L_func_name,":",O_status,":","Exception occured while deleting from alloc_calc_source_temp table for PO allocation: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting from alloc_calc_source_temp table for PO allocation :"+ str(error)
        elif O_status==9:
            print(L_func_name,":",O_status,":","Exception occured while deleting from alloc_calc_source_temp table for ASN allocation: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting from alloc_calc_source_temp table for ASN allocation :"+ str(error)
        elif O_status==10:
            print(L_func_name,":",O_status,":","Exception occured while deleting from alloc_calc_source_temp table for TSF allocation: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting from alloc_calc_source_temp table for TSF allocation :"+ str(error)
        elif O_status==11 or O_status==12:
            print(L_func_name,":",O_status,":","Exception occured while merging into alloc_calc_source_temp table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while merging into alloc_calc_source_temp table :"+ str(error)
        elif O_status==13:
            print(L_func_name,":",O_status,":","Exception occured befor final commit: ", error)     
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured befor final commit :"+ str(error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(error)
        conn.rollback()
        conn.cursor().close()                     
        return False, err_return


#--------------------------------------------------------------
# Function to setup destination 
#--------------------------------------------------------------
# def setup_location (conn
                    # ,I_alloc
                    # ,O_status):

    # L_func_name ="setup_location"
    # O_status = 0
    # print("EXECUTING: ",L_func_name)
    # try:
        # with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/inventory_setup_queries.yaml') as fh:
            # queries = yaml.load(fh, Loader=yaml.SafeLoader)

            # Q_create_allitemloc_tem       = queries['setup_location']['Q_create_allitemloc_tem']
            # Q_insert_calc_allitemloc_temp = queries['setup_location']['Q_insert_calc_allitemloc_temp']
            # Q_trunc_calc_allitemloc       = queries['setup_location']['Q_trunc_calc_allitemloc']
            # Q_fetch_calc_allitemloc       = queries['setup_location']['Q_fetch_calc_allitemloc']
            # Q_delete_clear_1              = queries['setup_location']['Q_delete_clear_1']
            # Q_delete_clear_2              = queries['setup_location']['Q_delete_clear_2']
            # Q_del_loc_status_ind1         = queries['setup_location']['Q_del_loc_status_ind1']
            # Q_del_loc_status_ind2         = queries['setup_location']['Q_del_loc_status_ind2']
            # Q_del_loc_status_ind3         = queries['setup_location']['Q_del_loc_status_ind3']
            # Q_del_loc_status_ind_wi1      = queries['setup_location']['Q_del_loc_status_ind_wi1']
            # Q_del_loc_status_ind_wi2      = queries['setup_location']['Q_del_loc_status_ind_wi2']
            # Q_del_def_wh                  = queries['setup_location']['Q_del_def_wh']
            # Q_del_frozen                  = queries['setup_location']['Q_del_frozen']
            # Q_del_rec_allitemloc        = queries['setup_location']['Q_del_rec_allitemloc']
            # Q_ins_calcallitem_main        = queries['setup_location']['Q_ins_calcallitem_main']
            # Q_chk_alloc                   = queries['setup_location']['Q_chk_alloc']
            # Q_chk_allitemloc_table        = queries['setup_location']['Q_chk_allitemloc_table']
            
            # mycursor = conn.cursor()
            # mycursor.execute("SET SESSION sql_mode = '';")
            # #status
            # O_status =1
            # #Q_chk_allitemloc_table
            # df_chk = pd.read_sql(Q_chk_allitemloc_table,conn)
            # L_chk = df_chk.chk[0]

            # if L_chk == 1:
                # #status
                # O_status =3
                # print(" Please drop the table alloc_calc_allitemloc_temp")
                # print(O_status,L_func_name)
                # conn.cursor().close()
                # return False

            # mycursor.execute(Q_create_allitemloc_tem)

            # #fetching alloc header
            # df_alloc_head = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))
            # #status
            # O_status =2
            
            # if len(df_alloc_head)>0:
                # L_alloc_level      = df_alloc_head.alloc_level[0]
                # L_alloc_criteria   = df_alloc_head.alloc_criteria[0]
                # L_wh_store_rel_ind = df_alloc_head.wh_store_rel_ind[0]
                # #status
                # O_status =3
                # mycursor.execute(Q_trunc_calc_allitemloc,(I_alloc,))
                # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                # #status
                # O_status =4
                # #executing destination
                # mycursor.execute(Q_insert_calc_allitemloc_temp,(I_alloc,L_alloc_criteria,L_alloc_criteria))
                # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                # #status
                # O_status =5

                # mycursor.execute(Q_delete_clear_1,(I_alloc,))
                # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                # #status
                # O_status =6

                # mycursor.execute(Q_delete_clear_2,(I_alloc,))
                # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                # #for sku
                # if L_alloc_criteria !='F':
                    # #status
                    # O_status =7
                    # mycursor.execute(Q_del_loc_status_ind1,(I_alloc,))
                    # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    # #status
                    # O_status =8
                    # mycursor.execute(Q_del_loc_status_ind2,(I_alloc,))
                    # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    # #status
                    # O_status =9
                    # mycursor.execute(Q_del_loc_status_ind3,(I_alloc,))
                    # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    # #status
                    # O_status =11
                    # mycursor.execute(Q_delete_clear_2,(I_alloc,))
                    # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                # else:
                    # #Q_del_loc_status_ind_wi2
                    # O_status =7
                    # mycursor.execute(Q_del_loc_status_ind_wi1,(I_alloc,))
                    # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    # #status
                    # O_status =8
                    # mycursor.execute(Q_del_loc_status_ind_wi2,(I_alloc,))
                    # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                # if L_alloc_level =='T':
                    # #status
                    # O_status =11.5
                    # mycursor.execute(Q_del_def_wh,(I_alloc,L_wh_store_rel_ind))
                    # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                # #status
                # O_status =12
                # mycursor.execute(Q_del_frozen,(I_alloc,I_alloc))
                # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                # O_status =14
                # mycursor.execute(Q_del_rec_allitemloc,(I_alloc,))
                # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount) #query added to delete duplicate reocrds
                # #status
                # O_status =13
                # mycursor.execute(Q_ins_calcallitem_main,(I_alloc,))
                # print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            # #commit
            # conn.commit()
            # #status
            # O_status =14
            # #returning inserted data
            # #df_calc_allitemloc = pd.read_sql(Q_fetch_calc_allitemloc,conn,params=(I_alloc,))
            # conn.cursor().close()
            # return True
                

    # except Exception as error:
        # if O_status==1:
            # print(L_func_name,":",O_status,":","Exception occured while creating temp table: ", error)
        # elif O_status==3:
            # print(L_func_name,":",O_status,":","Exception occured while deleting alloc_calc_allitemloc_temp: ", error)
        # elif O_status==4:
            # print(L_func_name,":",O_status,":","Exception occured while inserting alloc_calc_allitemloc_temp: ", error)
        # elif O_status>=5 and O_status<=12:
            # print(L_func_name,":",O_status,":","Exception occured while deleting alloc_calc_allitemloc_temp: ", error)
        # elif O_status==13:
            # print(L_func_name,":",O_status,":","Exception occured while inserting alloc_calc_allitemloc: ", error)
        # elif O_status==14:
            # print(L_func_name,":",O_status,":","Exception occured while deleting records from  alloc_calc_allitemloc: ", error)
        # else:
            # print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
        # conn.rollback()
        # conn.cursor().close()
        # return False

def setup_location (conn
                    ,I_alloc
                    ,O_status):

    L_func_name ="setup_location"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/inventory_setup_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)

            Q_create_allitemloc_tem       = queries['setup_location']['Q_create_allitemloc_tem']
            Q_create_allitemloc_temp_1    = queries['setup_location']['Q_create_allitemloc_temp_1']
            Q_drop_allitemloc_temp_1      = queries['setup_location']['Q_drop_allitemloc_temp_1']
            Q_insert_calc_allitemloc_temp = queries['setup_location']['Q_insert_calc_allitemloc_temp']
            Q_trunc_calc_allitemloc       = queries['setup_location']['Q_trunc_calc_allitemloc']
            Q_fetch_calc_allitemloc       = queries['setup_location']['Q_fetch_calc_allitemloc']
            Q_delete_clear_1              = queries['setup_location']['Q_delete_clear_1']
            Q_delete_clear_2              = queries['setup_location']['Q_delete_clear_2']
            Q_delete_clear_3              = queries['setup_location']['Q_delete_clear_3']
            Q_delete_clear_4              = queries['setup_location']['Q_delete_clear_4']
            Q_del_loc_status_ind1         = queries['setup_location']['Q_del_loc_status_ind1']
            Q_del_loc_status_ind2         = queries['setup_location']['Q_del_loc_status_ind2']
            Q_del_loc_status_ind3         = queries['setup_location']['Q_del_loc_status_ind3']
            Q_del_loc_status_ind4         = queries['setup_location']['Q_del_loc_status_ind4']
            Q_del_loc_status_ind5         = queries['setup_location']['Q_del_loc_status_ind5']
            Q_del_loc_status_ind6         = queries['setup_location']['Q_del_loc_status_ind6']
            Q_del_loc_status_ind7         = queries['setup_location']['Q_del_loc_status_ind7']
            Q_del_loc_status_ind8         = queries['setup_location']['Q_del_loc_status_ind8']
            Q_del_loc_status_ind_wi1      = queries['setup_location']['Q_del_loc_status_ind_wi1']
            Q_del_loc_status_ind_wi2      = queries['setup_location']['Q_del_loc_status_ind_wi2']
            Q_del_def_wh_1                = queries['setup_location']['Q_del_def_wh_1']
            Q_del_def_wh_2                = queries['setup_location']['Q_del_def_wh_2']
            Q_del_frozen                  = queries['setup_location']['Q_del_frozen']
            Q_del_rec_allitemloc          = queries['setup_location']['Q_del_rec_allitemloc']
            Q_ins_calcallitem_main        = queries['setup_location']['Q_ins_calcallitem_main']
            Q_chk_alloc                   = queries['setup_location']['Q_chk_alloc']
            Q_chk_allitemloc_table        = queries['setup_location']['Q_chk_allitemloc_table']
            
            mycursor = conn.cursor()
            mycursor.execute("SET SESSION sql_mode = '';")
            O_status =1
            df_chk = pd.read_sql(Q_chk_allitemloc_table,conn)
            L_chk = df_chk.chk[0]

            if L_chk == 1:
                O_status =2
                print("Please drop the table alloc_calc_allitemloc_temp")
                print(O_status,L_func_name)
                return False, L_func_name+": "+O_status+" : Please drop the table alloc_calc_allitemloc_temp."

            mycursor.execute(Q_create_allitemloc_tem)

            #fetching alloc header
            df_alloc_head = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))
            
            O_status =3
            if len(df_alloc_head)>0:
                O_status =4
                L_alloc_level      = df_alloc_head.alloc_level[0]
                L_alloc_criteria   = df_alloc_head.alloc_criteria[0]
                
                O_status =5
                if L_alloc_criteria == 'F':
                    L_wh_store_rel_ind = 'N'
                else:
                    L_wh_store_rel_ind = df_alloc_head.wh_store_rel_ind[0]
                    
                O_status =6
                mycursor.execute(Q_trunc_calc_allitemloc,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                O_status =7
                #executing destination
                mycursor.execute(Q_insert_calc_allitemloc_temp,(I_alloc,L_alloc_criteria,L_alloc_criteria))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                O_status =8
                mycursor.execute(Q_delete_clear_1,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                O_status =9
                mycursor.execute(Q_delete_clear_2,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                if L_alloc_level =='T':
                    O_status =10
                    mycursor.execute(Q_create_allitemloc_temp_1)               
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    O_status =11                    
                    mycursor.execute(Q_delete_clear_3,(I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount) 
                    O_status =12                 
                    mycursor.execute(Q_drop_allitemloc_temp_1)
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    O_status =13
                    mycursor.execute(Q_create_allitemloc_temp_1)
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    O_status =14
                    mycursor.execute(Q_delete_clear_4,(I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    O_status =15
                    mycursor.execute(Q_drop_allitemloc_temp_1)
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                if L_alloc_criteria !='F':
                    O_status =16
                    mycursor.execute(Q_del_loc_status_ind1,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    O_status =17
                    mycursor.execute(Q_del_loc_status_ind2,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    O_status =18
                    mycursor.execute(Q_del_loc_status_ind3,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    # O_status =11
                    if L_alloc_level =='T':
                        O_status =19
                        mycursor.execute(Q_create_allitemloc_temp_1)
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                        O_status =20
                        mycursor.execute(Q_del_loc_status_ind4,(I_alloc,I_alloc))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                        O_status =21                        
                        mycursor.execute(Q_drop_allitemloc_temp_1)
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                        O_status =22
                        mycursor.execute(Q_create_allitemloc_temp_1)
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                        O_status =23
                        mycursor.execute(Q_del_loc_status_ind5,(I_alloc,I_alloc))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                        O_status =24          
                        mycursor.execute(Q_drop_allitemloc_temp_1)
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                        
                        O_status =25
                        mycursor.execute(Q_create_allitemloc_temp_1)
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                        O_status =26
                        mycursor.execute(Q_del_loc_status_ind6,(I_alloc,I_alloc))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                        O_status =27
                        mycursor.execute(Q_drop_allitemloc_temp_1) 
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)                        
                        
                elif L_alloc_criteria =='F':
                    O_status =28
                    mycursor.execute(Q_del_loc_status_ind_wi1,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    O_status =29
                    mycursor.execute(Q_del_loc_status_ind_wi2,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    
                    if L_alloc_level =='T':
                        O_status =30
                        mycursor.execute(Q_create_allitemloc_temp_1)
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                        O_status =31
                        mycursor.execute(Q_del_loc_status_ind7,(I_alloc,I_alloc))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                        O_status =32 
                        mycursor.execute(Q_drop_allitemloc_temp_1)
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                        
                        O_status =33
                        mycursor.execute(Q_create_allitemloc_temp_1)
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                        O_status =34
                        mycursor.execute(Q_del_loc_status_ind8,(I_alloc,I_alloc))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                        O_status =35           
                        mycursor.execute(Q_drop_allitemloc_temp_1)
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                if L_alloc_level =='T':
                    O_status =36
                    mycursor.execute(Q_create_allitemloc_temp_1)
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    O_status =37
                    mycursor.execute(Q_del_def_wh_1,(I_alloc,L_wh_store_rel_ind,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    O_status =38
                    mycursor.execute(Q_drop_allitemloc_temp_1)
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                else:
                    O_status =39
                    mycursor.execute(Q_create_allitemloc_temp_1)
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    O_status =40
                    mycursor.execute(Q_del_def_wh_2,(I_alloc,L_wh_store_rel_ind,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    O_status =41
                    mycursor.execute(Q_drop_allitemloc_temp_1)
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status =42
                mycursor.execute(Q_del_frozen,(I_alloc,I_alloc))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status =43
                mycursor.execute(Q_del_rec_allitemloc,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount) #query added to delete duplicate reocrds
                O_status =44
                mycursor.execute(Q_ins_calcallitem_main,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            conn.commit()
            conn.cursor().close()       
            O_status =14          
            return True,""
                

    except Exception as error:
        err_return = ""
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while creating temp table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while creating temp table :"+ str(error)
        elif O_status==6:
            print(L_func_name,":",O_status,":","Exception occured while deleting alloc_calc_allitemloc_temp: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting alloc_calc_allitemloc_temp :"+ str(error)
        elif O_status==7:
            print(L_func_name,":",O_status,":","Exception occured while inserting alloc_calc_allitemloc_temp: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting alloc_calc_allitemloc_temp :"+ str(error)
        elif O_status>=8 and O_status<=41:
            print(L_func_name,":",O_status,":","Exception occured while deleting alloc_calc_allitemloc_temp: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting alloc_calc_allitemloc_temp :"+ str(error)
        elif O_status==42:
            print(L_func_name,":",O_status,":","Exception occured while deleting records from  alloc_calc_allitemloc: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting records from  alloc_calc_allitemloc :"+ str(error)
        elif O_status==43:
            print(L_func_name,":",O_status,":","Exception occured while inserting alloc_calc_allitemloc: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting alloc_calc_allitemloc :"+ str(error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(error)
        conn.rollback()
        conn.cursor().close()                     
        return False, err_return

#Changes by Shubham End#
#--------------------------------------------------------------
# Function to update latest inventory for WH type allocation
#--------------------------------------------------------------
#def update_wh_inv (conn
#                  ,I_alloc
#                  ,O_status):
#    L_func_name ="update_wh_inv"
#    O_status = 0
#    emp_list = list()
#    print("EXECUTING: ",L_func_name)
#    try:
#        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/inventory_setup_queries.yaml') as fh:
#            queries = yaml.load(fh, Loader=yaml.SafeLoader)
#            Q_fetch_item_sku = queries['update_wh_inv']['Q_fetch_item_sku']
#            Q_chk_alloc = queries['update_inv']['Q_chk_alloc']
#            mycursor = conn.cursor()
#            #status
#            O_status =1

#            #checking alloc head
#            df_alloc_head = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))
#            #status
#            O_status =2

#            if len(df_alloc_head)>0:
#                L_alloc_level = df_alloc_head.alloc_level[0]
#                #status
#                O_status =3

#                if L_alloc_level =='T':
#                    #status
#                    O_status =4
#                    #updating inventory
#                    df_search_data = pd.read_sql(Q_fetch_item_sku,conn,params=(I_alloc,))

#                    if len(df_search_data)>0:
#                        #status
#                        O_status =5
#                        return df_search_data
#                    else:
#                        #status
#                        O_status =404
#                        print("No data: ",L_func_name,O_status)
#                        return emp_list

#    except Exception as argument:
#        print(L_func_name,O_status)
#        print("Exception occured in: ",L_func_name,argument)        
#        conn.rollback()
#        return emp_list
            

#----------------------------------------------------------
# Function to update latest inventory 
#----------------------------------------------------------
def update_inv (conn
                ,I_alloc
                ,O_status):
    L_func_name ="update_inv"
    O_status = 0
    L_wh_source_type_ind  = 0
    L_wi_source_type_ind  = 0
    L_po_source_type_ind  = 0
    L_tsf_source_type_ind = 0
    L_asn_source_type_ind = 0
    L_clear_ind           = None
    #style/sku
    L_po  = list()
    L_tsf = list()
    L_asn = list()
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/inventory_setup_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)

            Q_chk_alloc                  = queries['update_inv']['Q_chk_alloc']
            Q_clear_ind                  = queries['update_inv']['Q_clear_ind']
            Q_chk_itm_srch               = queries['update_inv']['Q_chk_itm_srch']
            Q_cre_itm_loc_rfrh           = queries['update_inv']['Q_cre_itm_loc_rfrh']
            Q_del_rfrs_tmp               = queries['update_inv']['Q_del_rfrs_tmp']
            Q_ins_rfrs_tmp               = queries['update_inv']['Q_ins_rfrs_tmp']
            Q_merg_avail_qty             = queries['update_inv']['Q_merg_avail_qty']
            Q_create_wh_srch_table       = queries['update_inv']['Q_create_wh_srch_table']
            Q_del_whs_tmp                = queries['update_inv']['Q_del_whs_tmp']
            Q_ins_wh_tmp                 = queries['update_inv']['Q_ins_wh_tmp']
            Q_ins_wh_tmp_wif             = queries['update_inv']['Q_ins_wh_tmp_wif']
            Q_create_item_loc_srch_table = queries['update_inv']['Q_create_item_loc_srch_table']
            Q_del_itm_loc_srch_tbl       = queries['update_inv']['Q_del_itm_loc_srch_tbl']
            Q_ins_itm_loc_srch_sku       = queries['update_inv']['Q_ins_itm_loc_srch_sku']
            Q_ins_itm_loc_srch_wif_sku   = queries['update_inv']['Q_ins_itm_loc_srch_wif_sku']
            Q_create_item_srch_table     = queries['update_inv']['Q_create_item_srch_table']
            Q_ins_itm_src_tmp_sku        = queries['update_inv']['Q_ins_itm_src_tmp_sku']
            Q_upd_inact_qty_apv          = queries['update_inv']['Q_upd_inact_qty_apv']
            Q_del_doc_type               = queries['update_inv']['Q_del_doc_type']
            Q_upd_wi_avail_qty           = queries['update_inv']['Q_upd_wi_avail_qty']
            Q_upd_inact_qty              = queries['update_inv']['Q_upd_inact_qty']
            Q_delete_search_dtl          = queries['update_inv']['Q_delete_search_dtl']
            Q_insert_itm_search          = queries['update_inv']['Q_insert_itm_search']

            #style/diff
            Q_asn_id                     = queries['update_inv']['Q_asn_id']
            Q_tsf_no                     = queries['update_inv']['Q_tsf_no']
            Q_po_no                      = queries['update_inv']['Q_po_no']
            Q_ins_itm_loc_srch_style     = queries['update_inv']['Q_ins_itm_loc_srch_style']
            Q_ins_itm_loc_srch_wif_style = queries['update_inv']['Q_ins_itm_loc_srch_wif_style']
            Q_ins_itm_src_tmp_style      = queries['update_inv']['Q_ins_itm_src_tmp_style']
            Q_del_itm_src_tmp            = queries['update_inv']['Q_del_itm_src_tmp']

            mycursor = conn.cursor()
            mycursor.execute("SET sql_mode = '';")
            #status
            O_status =1
            #creating tables
            mycursor.execute(Q_cre_itm_loc_rfrh)
            mycursor.execute(Q_create_wh_srch_table)
            mycursor.execute(Q_create_item_loc_srch_table)
            mycursor.execute(Q_create_item_srch_table)

            df_clear_ind = pd.read_sql(Q_clear_ind,conn,params=(I_alloc,))
            if len(df_clear_ind)>0:
                L_clear_ind = df_clear_ind.clearance_ind[0]
            #status
            O_status =2
            mycursor.execute(Q_del_rfrs_tmp,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status =3
            mycursor.execute(Q_ins_rfrs_tmp,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status =4
            df_search_data = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))

            if len(df_search_data)>0:
                #status
                O_status =5
                L_alloc_criteria = df_search_data.alloc_criteria[0]
                L_status         = df_search_data.status[0]
                L_alloc_type     = df_search_data.alloc_type[0]
                L_alloc_level    = df_search_data.alloc_level[0]
                if L_alloc_criteria == 'W':
                    L_wh_source_type_ind = 1
                elif L_alloc_criteria =='F':
                    L_wi_source_type_ind = 1
                elif L_alloc_criteria =='P':
                    L_po_source_type_ind = 1
                elif L_alloc_criteria =='T':
                    L_tsf_source_type_ind = 1
                elif L_alloc_criteria =='A':
                    L_asn_source_type_ind = 1

                #style/diff
                if L_alloc_criteria == 'A':
                    mycursor.execute(Q_asn_id,(I_alloc,)) 
                    L_result = mycursor.fetchall()

                    if len(L_result) > 0:
                        for i in range(len(L_result)):
                            L_asn_id = L_result[i]
                            L_asn_id = L_asn_id[0]
                            L_asn.append(L_asn_id)
                elif L_alloc_criteria == 'T':
                    mycursor.execute(Q_tsf_no,(I_alloc,)) 
                    L_result = mycursor.fetchall()

                    if len(L_result) > 0:
                        for i in range(len(L_result)):
                            L_tsf_no = L_result[i]
                            L_tsf_no = L_tsf_no[0]
                            L_tsf.append(L_tsf_no)
                elif L_alloc_criteria == 'P':
                    mycursor.execute(Q_po_no,(I_alloc,)) 
                    L_result = mycursor.fetchall()

                    if len(L_result) > 0:
                        for i in range(len(L_result)):
                            L_po_no = L_result[i]
                            L_po_no = L_po_no[0]
                            L_po.append(L_po_no)
                
                if L_status in ('APV','RSV'): #change when code detail is updated
                    #status
                    O_status =6
                    mycursor.execute(Q_merg_avail_qty,(I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                #status
                O_status =7
                mycursor.execute(Q_del_whs_tmp)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                #status
                O_status =8
                if L_alloc_criteria !='F':
                    #status
                    O_status =9
                    mycursor.execute(Q_ins_wh_tmp,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                else:
                    #status
                    O_status =9
                    mycursor.execute(Q_ins_wh_tmp_wif)
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                #status
                O_status =10
                mycursor.execute(Q_del_itm_loc_srch_tbl)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                if L_alloc_criteria !='F':
                    #status
                    O_status =11
                    if L_alloc_level == 'T':
                        #status
                        O_status =12
                        mycursor.execute(Q_ins_itm_loc_srch_sku,(I_alloc,)) 
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    elif L_alloc_level == 'D':
                        #status
                        O_status =12
                        mycursor.execute(Q_ins_itm_loc_srch_style,(I_alloc,)) 
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                else:
                    if L_alloc_level == 'T':
                        #Q_ins_itm_loc_srch_wif_sku
                        #status
                        O_status =12
                        mycursor.execute(Q_ins_itm_loc_srch_wif_sku,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    elif L_alloc_level == 'D':
                        #Q_ins_itm_loc_srch_wif_sku
                        #status
                        O_status =12
                        mycursor.execute(Q_ins_itm_loc_srch_wif_style,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                #status
                O_status =13
                if L_alloc_level == 'T':
                    mycursor.execute(Q_ins_itm_src_tmp_sku)
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                elif L_alloc_level == 'D':
                    mycursor.execute(Q_ins_itm_src_tmp_style)
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                if L_alloc_level == 'D':
                    mycursor.execute(Q_del_itm_src_tmp)
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                print(pd.read_sql("select * from alloc_items_srch_temp;",conn))
                
                #status
                O_status =14
                mycursor.execute(Q_delete_search_dtl,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #status
                O_status =15
                
                I_search_criteria = {"WHATIF_SOURCE_TYPE_IND" :L_wi_source_type_ind,
                                     "WH_SOURCE_TYPE_IND"     :L_wh_source_type_ind,
                                     "TSF_SOURCE_TYPE_IND"    :L_tsf_source_type_ind,
                                     "PO_SOURCE_TYPE_IND"     :L_po_source_type_ind,
                                     "ASN_SOURCE_TYPE_IND"    :L_asn_source_type_ind,
                                     "PO"                     :L_po,
                                     "PO_TYPE"                :None,
                                     "ASN"                    :L_asn,
                                     "TSF"                    :L_tsf,
                                     "ALLOC_NO"               :I_alloc,
                                     "ALLOC_CRITERIA"         :L_alloc_criteria,
                                     "ALLOC_LEVEL"            :L_alloc_level,
                                     "ALLOC_TYPE"             :L_alloc_type,
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
                                     "ITEM_PARENT"            :[], 
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
                                     "EISD_END_DATE"          :None                       
                                    }

                conn.commit()
                #status
                O_status =16
                L_fetch_inventory = fetch_inventory(conn,
                                                  I_search_criteria,
                                                  O_status)
                if len(L_fetch_inventory) ==0:
                    print("No records found while fetching latest inventory")
                    return False,L_func_name+":"+str(O_status)+": "+"No records found while fetching latest inventory."
                conn.commit()
                
                if L_status in ('APV','RSV'):
                    #status
                    O_status =17
                    mycursor.execute(Q_upd_inact_qty_apv,(I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                else:    
                    #status
                    O_status =18
                    df_chk = pd.read_sql(Q_chk_itm_srch,conn,params=(I_alloc,))
                    if len(df_chk)==0:
                        #status
                        O_status =19
                        mycursor.execute(Q_del_rfrs_tmp,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    else:
                        #status
                        O_status =20
                        if L_alloc_criteria == 'F':
                            mycursor.execute(Q_upd_wi_avail_qty,(I_alloc,I_alloc))
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                        mycursor.execute(Q_del_doc_type,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    #status
                    O_status = 21
                    mycursor.execute(Q_upd_inact_qty,(I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #conn.commit()
                #status
                O_status = 22
                mycursor.execute(Q_delete_search_dtl,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #status
                O_status = 23
                mycursor.execute(Q_insert_itm_search,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()
                conn.cursor().close()
                return True, ""

    except Exception as error:
        err_return = ""
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while creating temp table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while creating temp table :"+ str(error)
        elif O_status==2:
            print(L_func_name,":",O_status,":","Exception occured while deleting alloc_itm_srch_rfrs_temp: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting alloc_itm_srch_rfrs_temp :"+ str(error)
        elif O_status==3:
            print(L_func_name,":",O_status,":","Exception occured while inserting alloc_itm_srch_rfrs_temp: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting alloc_itm_srch_rfrs_temp :"+ str(error)
        elif O_status>=4 and O_status<=5:
            print(L_func_name,":",O_status,":","Exception occured while fetching alloc_head data: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while fetching alloc_head data :"+ str(error)
        elif O_status==6:
            print(L_func_name,":",O_status,":","Exception occured while merging alloc_itm_srch_rfrs_temp: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while merging alloc_itm_srch_rfrs_temp :"+ str(error)
        elif O_status>=7 and O_status <=16:
            print(L_func_name,":",O_status,":","Exception occured while merging alloc_itm_srch_rfrs_temp: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while merging alloc_itm_srch_rfrs_temp :"+ str(error)
        elif O_status>=17 and O_status <=23:
            print(L_func_name,":",O_status,":","Exception occured while processing data into alloc_itm_search_dtl: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing data into alloc_itm_search_dtl :"+ str(error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False,err_return


