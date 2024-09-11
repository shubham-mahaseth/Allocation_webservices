from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
from ..CREATE_SCREEN.populate_search_result import *
import pandas as pd
import yaml

##################################################################################
#Created By - Priyanshu Pandey                                                   #
#File Name - setup_inventory.py                                                  #
#Purpose - Contains all the functions to setup inventory before calculation      #
##################################################################################

#--------------------------------------------------------------
# Function to setup destination item 
#--------------------------------------------------------------
def setup_item_location (conn
                         ,I_alloc
                         ,O_status):
    L_func_name ="setup_item_location"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_inventory_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_delete_calc_source      = queries['setup_item_location']['Q_delete_calc_source']
            Q_fetch_calc_like_source  = queries['setup_item_location']['Q_fetch_calc_like_source']
            Q_fetch_calc_item_source  = queries['setup_item_location']['Q_fetch_calc_item_source']
            Q_fetch_calc_source       = queries['setup_item_location']['Q_fetch_calc_source']
            Q_insert_calc_source      = queries['setup_item_location']['Q_insert_calc_source']
            Q_like_item_chk           = queries['setup_item_location']['Q_like_item_chk']
            Q_chk_alloc               = queries['setup_location']['Q_chk_alloc']

            #status
            O_status =1
            mycursor = conn.cursor()
            mycursor.execute(Q_delete_calc_source,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status =2

            mycursor.execute(Q_chk_alloc,(I_alloc,))
            my_alloc_head = mycursor.fetchall()

            df_alloc_head = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))
            #status
            O_status =3

            if len(my_alloc_head)>0:
                L_alloc_level = df_alloc_head.alloc_level[0]

                mycursor.execute(Q_like_item_chk,(I_alloc,))
                my_like_item_chk = mycursor.fetchall()
                #status
                O_status =4

                if L_alloc_level == 'T':
                    #status
                    O_status =5

                    if my_like_item_chk == 1:
                        df_calc_like_source = pd.read_sql(Q_fetch_calc_like_source,conn,params=(I_alloc,))
                        #status
                        O_status =6

                        if len(df_calc_like_source)>0:
                            #status
                            O_status =7

                            for i in range(len(df_calc_like_source)):
                                #status
                                O_status =8

                                L_insert_calc_source_temp = (df_calc_like_source.loc[i, "alloc_no"],             df_calc_like_source.loc[i, "item_source_id"],          df_calc_like_source.loc[i, "release_date"], 
                                                             df_calc_like_source.loc[i, "item_type"],            df_calc_like_source.loc[i, "source_item"],             df_calc_like_source.loc[i, "source_item_level"],
                                                             df_calc_like_source.loc[i, "source_tran_level"],    df_calc_like_source.loc[i, "source_pack_ind"],         df_calc_like_source.loc[i, "source_diff1_id"],
                                                             df_calc_like_source.loc[i, "source_diff2_id"],      df_calc_like_source.loc[i, "source_diff3_id"],         df_calc_like_source.loc[i, "source_diff4_id"], 
                                                             df_calc_like_source.loc[i, "tran_item"],            df_calc_like_source.loc[i, "tran_item_level"],         df_calc_like_source.loc[i, "tran_tran_level"],
                                                             df_calc_like_source.loc[i, "tran_pack_ind"],        df_calc_like_source.loc[i, "tran_diff1_id"],           df_calc_like_source.loc[i, "tran_diff2_id"], 
                                                             df_calc_like_source.loc[i, "tran_diff3_id"],        df_calc_like_source.loc[i, "tran_diff4_id"],           df_calc_like_source.loc[i, "hier1"], 
                                                             df_calc_like_source.loc[i, "hier2"],                df_calc_like_source.loc[i, "hier3"],                   df_calc_like_source.loc[i, "pack_no"], 
                                                             df_calc_like_source.loc[i, "like_source_item"],     df_calc_like_source.loc[i, "like_source_item_level"],  df_calc_like_source.loc[i, "like_source_tran_level"],
                                                             df_calc_like_source.loc[i, "like_source_pack_ind"], df_calc_like_source.loc[i, "like_source_diff1_id"],    df_calc_like_source.loc[i, "like_source_diff2_id"],
                                                             df_calc_like_source.loc[i, "like_source_diff3_id"], df_calc_like_source.loc[i, "like_source_diff4_id"],    df_calc_like_source.loc[i, "like_tran_item"],
                                                             df_calc_like_source.loc[i, "like_tran_item_level"], df_calc_like_source.loc[i, "like_tran_tran_level"],    df_calc_like_source.loc[i, "like_tran_pack_ind"],
                                                             df_calc_like_source.loc[i, "like_tran_diff1_id"],   df_calc_like_source.loc[i, "like_tran_diff2_id"],      df_calc_like_source.loc[i, "like_tran_diff3_id"],
                                                             df_calc_like_source.loc[i, "like_tran_diff4_id"],   df_calc_like_source.loc[i, "like_hier1"],              df_calc_like_source.loc[i, "like_hier2"],
                                                             df_calc_like_source.loc[i, "like_hier3"],           df_calc_like_source.loc[i, "like_pack_no"],            df_calc_like_source.loc[i, "like_item_weight"],
                                                             df_calc_like_source.loc[i, "like_size_prof_ind"],   df_calc_like_source.loc[i, "create_id"],               df_calc_like_source.loc[i, "create_datetime"],
                                                             df_calc_like_source.loc[i, "last_update_id"],       df_calc_like_source.loc[i, "last_update_datetime"],    df_calc_like_source.loc[i, "som_qty"])
                                #insert calc source temp
                                L_insert_calc_source_temp = convert_numpy(L_insert_calc_source_temp)
                                mycursor.execute(Q_insert_calc_source,L_insert_calc_source_temp)  
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                                #status
                                O_status =9
                    else:
                        #status
                        O_status =10

                        #mycursor.execute(Q_fetch_calc_item_source,(I_alloc,))
                        df_calc_item_source = pd.read_sql(Q_fetch_calc_item_source,conn,params=(I_alloc,))
                        #status
                        O_status =11

                        if len(df_calc_item_source)>0:
                            #status
                            O_status =12

                            for i in range(len(df_calc_item_source)):
                                L_insert_calc_source_temp = (df_calc_item_source.loc[i, "alloc_no"],             df_calc_item_source.loc[i, "item_source_id"],          df_calc_item_source.loc[i, "release_date"], 
                                                             df_calc_item_source.loc[i, "item_type"],            df_calc_item_source.loc[i, "source_item"],             df_calc_item_source.loc[i, "source_item_level"],
                                                             df_calc_item_source.loc[i, "source_tran_level"],    df_calc_item_source.loc[i, "source_pack_ind"],         df_calc_item_source.loc[i, "source_diff1_id"],
                                                             df_calc_item_source.loc[i, "source_diff2_id"],      df_calc_item_source.loc[i, "source_diff3_id"],         df_calc_item_source.loc[i, "source_diff4_id"], 
                                                             df_calc_item_source.loc[i, "tran_item"],            df_calc_item_source.loc[i, "tran_item_level"],         df_calc_item_source.loc[i, "tran_tran_level"],
                                                             df_calc_item_source.loc[i, "tran_pack_ind"],        df_calc_item_source.loc[i, "tran_diff1_id"],           df_calc_item_source.loc[i, "tran_diff2_id"], 
                                                             df_calc_item_source.loc[i, "tran_diff3_id"],        df_calc_item_source.loc[i, "tran_diff4_id"],           df_calc_item_source.loc[i, "hier1"], 
                                                             df_calc_item_source.loc[i, "hier2"],                df_calc_item_source.loc[i, "hier3"],                   df_calc_item_source.loc[i, "pack_no"], 
                                                             df_calc_item_source.loc[i, "like_source_item"],     df_calc_item_source.loc[i, "like_source_item_level"],  df_calc_item_source.loc[i, "like_source_tran_level"],
                                                             df_calc_item_source.loc[i, "like_source_pack_ind"], df_calc_item_source.loc[i, "like_source_diff1_id"],    df_calc_item_source.loc[i, "like_source_diff2_id"],
                                                             df_calc_item_source.loc[i, "like_source_diff3_id"], df_calc_item_source.loc[i, "like_source_diff4_id"],    df_calc_item_source.loc[i, "like_tran_item"],
                                                             df_calc_item_source.loc[i, "like_tran_item_level"], df_calc_item_source.loc[i, "like_tran_tran_level"],    df_calc_item_source.loc[i, "like_tran_pack_ind"],
                                                             df_calc_item_source.loc[i, "like_tran_diff1_id"],   df_calc_item_source.loc[i, "like_tran_diff2_id"],      df_calc_item_source.loc[i, "like_tran_diff3_id"],
                                                             df_calc_item_source.loc[i, "like_tran_diff4_id"],   df_calc_item_source.loc[i, "like_hier1"],              df_calc_item_source.loc[i, "like_hier2"],
                                                             df_calc_item_source.loc[i, "like_hier3"],           df_calc_item_source.loc[i, "like_pack_no"],            df_calc_item_source.loc[i, "like_item_weight"],
                                                             df_calc_item_source.loc[i, "like_size_prof_ind"],   df_calc_item_source.loc[i, "create_id"],               df_calc_item_source.loc[i, "create_datetime"],
                                                             df_calc_item_source.loc[i, "last_update_id"],       df_calc_item_source.loc[i, "last_update_datetime"],    df_calc_item_source.loc[i, "som_qty"])
                                #insert calc source temp
                                L_insert_calc_source_temp = convert_numpy(L_insert_calc_source_temp)
                                mycursor.execute(Q_insert_calc_source,L_insert_calc_source_temp)  
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #fetching inserted record
                #returning inserted data
                #status
                O_status =13

                #df_calc_source = pd.read_sql(Q_fetch_calc_source,conn,params=(I_alloc,))

                #if len(df_calc_source)>0:
                #    #status
                #    O_status =14
                
                #commit
                conn.commit()
                conn.cursor().close()
                return True

            else:
                #status
                O_status =404
                print("No record found: ",L_func_name)
                print(O_status)
                conn.cursor().close()
                return False

    except Exception as argument:
        print(L_func_name,O_status)
        print("Exception occured in: ",L_func_name,argument)
        conn.rollback()
        conn.cursor().close()
        return False



#--------------------------------------------------------------
# Function to setup destination 
#--------------------------------------------------------------
def setup_location (conn
                    ,I_alloc
                    ,O_status):

    L_func_name ="setup_location"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_inventory_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)

            Q_create_allitemloc_tem       = queries['setup_location']['Q_create_allitemloc_tem']
            Q_insert_calc_allitemloc_temp = queries['setup_location']['Q_insert_calc_allitemloc_temp']
            Q_trunc_calc_allitemloc       = queries['setup_location']['Q_trunc_calc_allitemloc']
            Q_fetch_calc_allitemloc       = queries['setup_location']['Q_fetch_calc_allitemloc']
            Q_delete_clear_1              = queries['setup_location']['Q_delete_clear_1']
            Q_delete_clear_2              = queries['setup_location']['Q_delete_clear_2']
            Q_del_loc_status_ind1         = queries['setup_location']['Q_del_loc_status_ind1']
            Q_del_loc_status_ind2         = queries['setup_location']['Q_del_loc_status_ind2']
            Q_del_loc_status_ind3         = queries['setup_location']['Q_del_loc_status_ind3']
            Q_del_loc_status_ind_wi1      = queries['setup_location']['Q_del_loc_status_ind_wi1']
            Q_del_loc_status_ind_wi2      = queries['setup_location']['Q_del_loc_status_ind_wi2']
            Q_del_def_wh                  = queries['setup_location']['Q_del_def_wh']
            Q_del_frozen                  = queries['setup_location']['Q_del_frozen']
            Q_del_rec_allitemloc        = queries['setup_location']['Q_del_rec_allitemloc']
            Q_ins_calcallitem_main        = queries['setup_location']['Q_ins_calcallitem_main']
            Q_chk_alloc                   = queries['setup_location']['Q_chk_alloc']
            Q_chk_allitemloc_table        = queries['setup_location']['Q_chk_allitemloc_table']
            
            mycursor = conn.cursor()
            mycursor.execute("SET SESSION sql_mode = '';")
            #status
            O_status =1
            #Q_chk_allitemloc_table
            df_chk = pd.read_sql(Q_chk_allitemloc_table,conn)
            L_chk = df_chk.chk[0]

            if L_chk == 1:
                #status
                O_status =3
                print(" Please drop the table alloc_calc_allitemloc_temp")
                print(O_status,L_func_name)
                conn.cursor().close()
                return False

            mycursor.execute(Q_create_allitemloc_tem)

            #fetching alloc header
            df_alloc_head = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))
            #status
            O_status =2
            
            if len(df_alloc_head)>0:
                L_alloc_level      = df_alloc_head.alloc_level[0]
                L_alloc_criteria   = df_alloc_head.alloc_criteria[0]
                L_wh_store_rel_ind = df_alloc_head.wh_store_rel_ind[0]
                #status
                O_status =3
                mycursor.execute(Q_trunc_calc_allitemloc,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #status
                O_status =4
                #executing destination
                mycursor.execute(Q_insert_calc_allitemloc_temp,(I_alloc,L_alloc_criteria,L_alloc_criteria))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                #status
                O_status =5

                mycursor.execute(Q_delete_clear_1,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #status
                O_status =6

                mycursor.execute(Q_delete_clear_2,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                #for sku
                if L_alloc_criteria !='F':
                    #status
                    O_status =7
                    mycursor.execute(Q_del_loc_status_ind1,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    #status
                    O_status =8
                    mycursor.execute(Q_del_loc_status_ind2,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    #status
                    O_status =9
                    mycursor.execute(Q_del_loc_status_ind3,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    #status
                    O_status =11
                    mycursor.execute(Q_delete_clear_2,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                else:
                    #Q_del_loc_status_ind_wi2
                    O_status =7
                    mycursor.execute(Q_del_loc_status_ind_wi1,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    #status
                    O_status =8
                    mycursor.execute(Q_del_loc_status_ind_wi2,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                if L_alloc_level =='T':
                    #status
                    O_status =11.5
                    mycursor.execute(Q_del_def_wh,(I_alloc,L_wh_store_rel_ind))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                #status
                O_status =12
                mycursor.execute(Q_del_frozen,(I_alloc,I_alloc))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status =14
                mycursor.execute(Q_del_rec_allitemloc,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount) #query added to delete duplicate reocrds
                #status
                O_status =13
                mycursor.execute(Q_ins_calcallitem_main,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #commit
            conn.commit()
            #status
            O_status =14
            #returning inserted data
            #df_calc_allitemloc = pd.read_sql(Q_fetch_calc_allitemloc,conn,params=(I_alloc,))
            conn.cursor().close()
            return True
                

    except Exception as error:
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while creating temp table: ", error)
        elif O_status==3:
            print(L_func_name,":",O_status,":","Exception occured while deleting alloc_calc_allitemloc_temp: ", error)
        elif O_status==4:
            print(L_func_name,":",O_status,":","Exception occured while inserting alloc_calc_allitemloc_temp: ", error)
        elif O_status>=5 and O_status<=12:
            print(L_func_name,":",O_status,":","Exception occured while deleting alloc_calc_allitemloc_temp: ", error)
        elif O_status==13:
            print(L_func_name,":",O_status,":","Exception occured while inserting alloc_calc_allitemloc: ", error)
        elif O_status==14:
            print(L_func_name,":",O_status,":","Exception occured while deleting records from  alloc_calc_allitemloc: ", error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
        conn.rollback()
        conn.cursor().close()
        return False

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
#        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_inventory_queries.yaml') as fh:
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
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_inventory_queries.yaml') as fh:
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
                    if L_alloc_level =='T':
                        #status
                        O_status =12
                        mycursor.execute(Q_ins_itm_loc_srch_sku,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                else:
                    if L_alloc_level =='T':
                        #Q_ins_itm_loc_srch_wif_sku
                        #status
                        O_status =12
                        mycursor.execute(Q_ins_itm_loc_srch_wif_sku,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                #status
                O_status =13
                mycursor.execute(Q_ins_itm_src_tmp_sku)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #status
                O_status =14
                mycursor.execute(Q_delete_search_dtl,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #status
                O_status =15

                I_search_criteria = {"WH_SOURCE_TYPE_IND"     :L_wh_source_type_ind,
                                     "WHATIF_SOURCE_TYPE_IND" :L_wi_source_type_ind,
                                     "PO_SOURCE_TYPE_IND"     :L_po_source_type_ind,
                                     "TSF_SOURCE_TYPE_IND"    :L_tsf_source_type_ind,
                                     "ASN_SOURCE_TYPE_IND"    :L_asn_source_type_ind,
                                     "ALLOC_NO"               :I_alloc,
                                     "ALLOC_CRITERIA"         :L_alloc_criteria,
                                     "ALLOC_LEVEL"            :L_alloc_level,
                                     "ALLOC_TYPE"             :L_alloc_type,
                                     "CLEARANCE_IND"          :L_clear_ind,
                                     "MIN_AVAIL_QTY"          :None,
                                     "MAX_AVAIL_QTY"          :None,
                                     "DIFF_ID"                :[],
                                     "VPN"                    :[],
                                     "ASN"                    :[],
                                     "TSF"                    :[],
                                     "PO"                     :[],
                                     "PO_TYPE"                :None,
                                     "START_DATE"             :None,
                                     "END_DATE"               :None,
                                     "EISD_START_DATE"        :None,
                                     "EISD_END_DATE"          :None,
                                     "SKU"                    :[]
                                     }
                #status
                O_status =16
                L_fetch_inventory=fetch_inventory(conn,
                                                  I_search_criteria,
                                                  O_status)
                if len(L_fetch_inventory) ==0:
                    print("No records found while fetching latest inventory")
                    return False
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
                return True

    except Exception as error:
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while creating temp table: ", error)
        elif O_status==2:
            print(L_func_name,":",O_status,":","Exception occured while deleting alloc_itm_srch_rfrs_temp: ", error)
        elif O_status==3:
            print(L_func_name,":",O_status,":","Exception occured while inserting alloc_itm_srch_rfrs_temp: ", error)
        elif O_status>=4 and O_status<=5:
            print(L_func_name,":",O_status,":","Exception occured while fetching alloc_head data: ", error)
        elif O_status==6:
            print(L_func_name,":",O_status,":","Exception occured while merging alloc_itm_srch_rfrs_temp: ", error)
        elif O_status>=7 and O_status <=16:
            print(L_func_name,":",O_status,":","Exception occured while merging alloc_itm_srch_rfrs_temp: ", error)
        elif O_status>=17 and O_status <=23:
            print(L_func_name,":",O_status,":","Exception occured while processing data into alloc_itm_search_dtl: ", error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
        conn.rollback()
        conn.cursor().close()
        return False

#--------------------------------------------------------------
# Function to load_item_source 
#--------------------------------------------------------------
def load_item(conn,
              L_alloc_no,
              O_status):
    O_status=(0)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/load_item_source_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_sel_record=queries['load_item_source']['Q_sel_record']
            Q_alloc_level=queries['load_item_source']['Q_alloc_level']
            Q_del_alloc_item_src_tmp=queries['load_item_source']['Q_del_alloc_item_src_tmp']
            Q_del_item_source=queries['load_item_source']['Q_del_item_source']
            Q_merge=queries['load_item_source']['Q_merge']
            Q_ins_item_source=queries['load_item_source']['Q_ins_item_source']
            Q_upd_pack_ind=queries['load_item_source']['Q_upd_pack_ind']
            Q_del_qty=queries['load_item_source']['Q_del_qty']

            I_input_data = list()
            mycursor = conn.cursor()
            mycursor.execute("SET sql_mode = ''; ")
            O_status=(10)
            L_item_found=0
            df_alloc_lvl = pd.read_sql(Q_alloc_level,conn,params=(L_alloc_no,))
            df_alloc_lvl=df_alloc_lvl["alloc_level"][0]

            O_status=(20)
            mycursor.execute(Q_del_alloc_item_src_tmp,(L_alloc_no,L_alloc_no,))

            mycursor.execute(Q_del_item_source,(L_alloc_no,L_alloc_no,))
            if mycursor.rowcount > 0:
                L_item_found=1

            O_status=(30)
            mycursor.execute("SET sql_mode = ''; ")
            mycursor.execute(Q_merge,(L_alloc_no,))
            if mycursor.rowcount > 0:
                L_item_found=1
                print("updated the data")

            O_status=(40)
            mycursor.execute("SET sql_mode = ''; ")
            mycursor.execute(Q_ins_item_source,(L_alloc_no,df_alloc_lvl,L_alloc_no,))
            if mycursor.rowcount > 0:
                L_item_found=1
                print("inserted the data")

            O_status=(50)
            if df_alloc_lvl=='T':
                mycursor.execute(Q_upd_pack_ind,(L_alloc_no,))
            O_status=(60)
            if L_item_found==1:
                L_fun_upd_alloc =update_alloc(conn,O_status,L_alloc_no,None,None,'Y',I_input_data)
                if L_fun_upd_alloc==False:
                    conn.rollback()
                    conn.cursor().close()
                    return False

                O_status=(70)
                L_fun_calc_source=setup_item_location(conn,L_alloc_no,O_status)
                if L_fun_calc_source==False:
                    conn.rollback()
                    conn.cursor().close()
                    return False

                O_status=(80)
                mycursor.execute(Q_sel_record,(L_alloc_no,))
                L_rule = mycursor.fetchall()
                if len(L_rule) > 0:
                    L_fun_setup_loc=setup_location(conn,L_alloc_no,O_status)
                    if L_fun_setup_loc==False:
                        conn.rollback()
                        conn.cursor().close()
                        return False
                O_status=(90)
                mycursor.execute(Q_del_qty,(L_alloc_no,L_alloc_no,L_alloc_no)) 
            conn.commit()
            conn.cursor().close()
            return True

    except Exception as argument:
        if O_status==10:
            print("load_item: Exception occured in selecting the alloc level: ",argument)
        elif O_status==20:
            print("load_item: Exception occured in deleting the data: ",argument)
        elif O_status==30:
            print("load_item:Exception occured while merging the data in alloc_item_source_dtl : ",argument)
        elif O_status==40:
            print("load_item:Exception occured while inserting the data in alloc_item_source_dtl ",argument)
        elif O_status==50:
            print("load_item:Exception occured while updating the pack indicator in alloc_item_source_dtl: ",argument)
        elif O_status==60:
            print("load_item:Exception occured while calling the update alloc ext function: ",argument)
        elif O_status==70:
            print("load_item:Exception occured while calling the setup_item_location function: ",argument)
        elif O_status==80:
            print("load_item:Exception occured while calling the setup_location function: ",argument)
        elif O_status==90:
            print("load_item:Exception occured while deleting the records from quantity limits: ",argument)
        else:
            print("load_item:",argument)
        conn.rollback() #handle run time exception completed
        conn.cursor().close()
        return False

#--------------------------------------------------------------
# Function to update_alc_alloc_ext 
#--------------------------------------------------------------
def update_alloc(conn,
                 O_status,
                 L_alloc_no,
                 L_level,
                 L_date,
                 L_recalc_ind,
                 I_input_data):
    O_status=(0)

    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/upd_alloc_ext_queries.yaml') as fh:
            queries            = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_sel_alloc_rule   = queries['update_alloc_ext']['Q_sel_alloc_rule']
            Q_sel_date_status  = queries['update_alloc_ext']['Q_sel_date_status']
            Q_upd_date         = queries['update_alloc_ext']['Q_upd_date']
            Q_upd_date_itemloc = queries['update_alloc_ext']['Q_upd_date_itemloc']
            Q_upd_alc_loc      = queries['update_alloc_ext']['Q_upd_alc_loc']
            Q_merge            = queries['update_alloc_ext']['Q_merge']
            Q_chck_calc_data   = queries['update_alloc_ext']['Q_chck_calc_data']
            mycursor=conn.cursor()
            mycursor.execute("SET sql_mode = ''; ")


            if len(I_input_data)>0:
                L_alloc_criteria  = I_input_data["ALLOC_CRITERIA"]
                L_status          = I_input_data["STATUS"]
                L_alloc_desc      = I_input_data["ALLOC_DESC"]
                L_alloc_type      = I_input_data["ALLOC_TYPE"]
                L_context         = I_input_data["CONTEXT"]
                L_promotion       = I_input_data["PROMOTION"]
                L_create_id       = I_input_data["CREATE_ID"]
                L_create_datetime = I_input_data["CREATE_DATETIME"]
            else:
                L_alloc_criteria  = None
                L_status          = None
                L_alloc_desc      = None
                L_alloc_type      = None
                L_context         = None
                L_create_id       = None
                L_promotion       = None
                L_create_datetime = None


            df_search_data = pd.read_sql(Q_sel_date_status,conn,params=(L_alloc_no,))
            if len(df_search_data)>0:
                df_rel_date=df_search_data["release_date"][0]
            else:
                df_rel_date=None

            if L_date!=None:
                O_status=(20)
                mycursor.execute(Q_upd_date,(L_date,L_alloc_no))
                mycursor.execute(Q_upd_date_itemloc,(L_date,L_alloc_no))
                mycursor.execute(Q_upd_alc_loc,(L_alloc_no,L_date))

            O_status=(30)
            if df_rel_date != None and df_rel_date != L_date:
                mycursor.execute(Q_sel_alloc_rule,(L_alloc_no,))
                fetch=mycursor.fetchall()
                if len(fetch)> 0:
                    df_chck_calc_data = pd.read_sql(Q_chck_calc_data,conn,params=(L_alloc_no,))
                    if len(df_chck_calc_data) > 0:
                        L_func= setup_location(conn,L_alloc_no,O_status)
                        if L_func==False:
                            conn.rollback()
                            conn.cursor().close()
                            return False

        O_status=(40)
        mycursor.execute(Q_merge,(L_alloc_no,L_alloc_criteria,L_status,L_alloc_desc,L_alloc_type,L_level,L_context,L_promotion,L_date,L_recalc_ind,L_create_id,L_create_datetime,L_alloc_criteria,L_context,L_alloc_desc,L_promotion,L_alloc_type,L_level,L_date,L_date,L_recalc_ind,))
        conn.commit()
        conn.cursor().close()
        return True


    except Exception as argument:
        if O_status==10:
            print("update_alloc:Exception occured in selecting release_date from alloc_head : ",update_alloc,argument)
        elif O_status==20:
            print("update_alloc: Exception occured while updating the query:",update_alloc,argument)
        elif O_status==30:
            print("update_alloc:Exception occured while calling the setup_location function ",update_alloc,argument)
        elif O_status==40:
            print("update_alloc:Exception occured in merge and insert query ",update_alloc,argument)
        else:
            print("load_item:",update_alloc,argument)
        conn.rollback()
        conn.cursor().close()
        return False