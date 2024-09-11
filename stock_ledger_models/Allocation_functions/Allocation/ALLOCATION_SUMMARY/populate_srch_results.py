
#from get_connection import get_mysql_conn
from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
#from get_connection import get_mysql_conn
from datetime import datetime
import pandas as pd
import numpy as np
import yaml

def populate_search_results(conn,
                            I_search,
                            O_status):
    L_func_name = "populate_search_results"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/ASQuery.yaml') as asq:
            queries               = yaml.load(asq, Loader=yaml.SafeLoader)
            Q_create_tmp_table    = queries['as_fun_search_results']['Q_create_tmp_table']
            Q_create_status_tbl   = queries['as_fun_search_results']['Q_create_status_tbl']
            #Q_chck_srch_table     = queries['as_fun_search_results']['Q_chck_srch_table']
            Q_delete_srch_results = queries['as_fun_search_results']['Q_delete_srch_results']
            Q_alloc_status        = queries['as_fun_search_results']['Q_alloc_status']
            Q_insert_srch_results = queries['as_fun_search_results']['Q_insert_srch_results']
            Q_insert_alloc_no     = queries['as_fun_search_results']['Q_insert_alloc_no']
            Q_update_srch_dtlcnt  = queries['as_fun_search_results']['Q_update_srch_dtlcnt']
            Q_update_srch_loccnt  = queries['as_fun_search_results']['Q_update_srch_loccnt']
            Q_search_results      = queries['as_fun_search_results']['Q_search_results']

            mycursor = conn.cursor()
            
            L_alloc_no          = I_search["I_alloc_no"]        
            L_alloc_desc        = I_search["I_alloc_desc"]       
            L_from_release_date = I_search["I_from_release_date"]
            L_to_release_date   = I_search["I_to_release_date"]  
            L_from_create_date  = I_search["I_from_create_date"] 
            L_to_create_date    = I_search["I_to_create_date"]   
            L_create_user       = I_search["I_create_user"]      
            #L_alloc_status      = I_search["I_alloc_status"]     
            L_po                = I_search["I_po"]               
            L_tst_no            = I_search["I_tst_no"]           
            L_context           = I_search["I_context"]          
            L_promotion         = I_search["I_promotion"]        
            L_dept              = I_search["I_dept"]             
            L_class             = I_search["I_class"]            
            L_subclass          = I_search["I_subclass"]         
            L_wh                = I_search["I_wh"]               
            L_item_parent       = I_search["I_item_parent"]      
            L_diffid            = I_search["I_diff_id"]          
            L_item_sku          = I_search["I_item_sku"]         
            L_vpn               = I_search["I_vpn"]              
            L_alloc_type        = I_search["I_alloc_type"]       
            L_source            = I_search["I_source"]           
            L_asn               = I_search["I_asn"]             
            L_packid            = I_search["I_pack_id"]          
            L_batch_calc_ind    = I_search["I_batch_calc_ind"]   

            if L_alloc_desc != 'NULL':
                L_alloc_desc = "'"+'%'+L_alloc_desc+'%'+"'"
                print(L_alloc_desc,"L_alloc_desc")

            mycursor.execute(Q_create_tmp_table)
            #mycursor.execute(Q_create_status_tbl)

            O_status = 1
            mycursor.execute(Q_delete_srch_results)
            print(L_func_name,O_status,"-","rows_affected: ",mycursor.rowcount)

            Q_cnt_status = pd.read_sql(Q_alloc_status,conn)
            
            O_status = 2
            Q_count_status = Q_cnt_status.cnt[0]
            np_int         = np.int64(Q_count_status)
            R_cnt_status   = np_int.item()

            O_status = 3            
            L_params = (L_alloc_type,L_alloc_type,L_alloc_type,L_create_user,L_alloc_no,R_cnt_status,R_cnt_status,L_alloc_type,L_alloc_desc,L_context,L_promotion,L_from_create_date,L_to_create_date,L_from_release_date,L_to_release_date,L_po,L_batch_calc_ind,L_asn,L_asn,L_asn,L_tst_no,L_tst_no,L_tst_no,L_wh,L_dept,L_class,L_subclass,L_item_parent,L_diffid,L_diffid,L_diffid,L_diffid,L_diffid,L_packid,L_item_sku,L_vpn,L_vpn,L_vpn,L_source,L_source,L_source,L_source,L_source,L_source,
                        L_alloc_type,L_alloc_type,L_alloc_type,L_create_user,R_cnt_status,R_cnt_status,L_alloc_type,L_alloc_no,L_alloc_desc,L_context,L_promotion,L_from_create_date,L_to_create_date,L_batch_calc_ind,L_alloc_type,L_alloc_type,L_po,L_po,L_asn,L_asn,L_tst_no,L_tst_no,L_wh,L_from_release_date,L_to_release_date,L_dept,L_class,L_subclass,L_item_parent,L_item_parent,L_diffid,L_item_sku,L_item_sku,L_item_sku,L_packid,L_packid,L_packid,L_vpn,L_vpn,L_source,L_source,L_source,L_source,
                        L_alloc_type,L_alloc_type,L_alloc_type,L_create_user,R_cnt_status,R_cnt_status,L_alloc_type,L_alloc_no,L_alloc_desc,L_context,L_promotion,L_from_create_date,L_to_create_date,L_batch_calc_ind,L_alloc_type,L_alloc_type,L_po,L_po,L_asn,L_asn,L_tst_no,L_tst_no,L_wh,L_from_release_date,L_to_release_date,L_dept,L_class,L_subclass,L_item_parent,L_item_parent,L_diffid,L_item_sku,L_item_sku,L_item_sku,L_packid,L_packid,L_packid,L_vpn,L_vpn,L_source,L_source,L_source,L_source,
                        L_alloc_type,L_alloc_type,L_alloc_type,L_create_user,L_alloc_no,R_cnt_status,R_cnt_status,L_alloc_type,L_alloc_desc,L_context,L_promotion,L_from_create_date,L_to_create_date,L_batch_calc_ind,L_from_release_date,L_to_release_date,L_po,L_dept,L_class,L_subclass,L_wh,L_item_parent,L_diffid,L_item_sku,L_packid,L_vpn,L_source,L_source,L_asn,L_tst_no)
            L_insert = Q_insert_srch_results % L_params
            mycursor.execute(L_insert) 
            print(O_status,"-","rows_affected: ",mycursor.rowcount)


            O_status = 4
            mycursor.execute(Q_insert_alloc_no)
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status = 5
            mycursor.execute(Q_update_srch_dtlcnt)
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status = 6
            mycursor.execute(Q_update_srch_loccnt)
            print(O_status,"-","rows_affected: ",mycursor.rowcount) 

            conn.commit()

            Q_srch_result = pd.read_sql(Q_search_results,conn)
            conn.cursor().close()
            return Q_srch_result,''

    except Exception as error:
        print("Failed to update table record: {}".format(error))  
        err_return= ''
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during execution of delete queries for alloc_no: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception raised during execution of delete queries for alloc_no:")
        elif O_status == 2:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during retreving the count of status from alloc_status: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception raised during retreving the count of status from alloc_status")
        elif O_status == 3:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during inserting records into alloc_summary: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception raised during inserting records into alloc_summary")
        elif O_status == 4:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during inserting records into alloc_no_as: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception raised during inserting records into alloc_no_as")
        elif O_status == 5:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during updating records in alloc_summary: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception raised during updating records in alloc_summary")
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured : "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured: ",error)
        conn.rollback()
        conn.cursor().close()
        return [], err_return


def refresh_search_results(conn,O_status):
    L_func_name = "refresh_search_results"
    no_data     = list()
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/ASQuery.yaml') as asq: 
            queries                      = yaml.load(asq, Loader=yaml.SafeLoader)
            Q_rfsh_summary_res           = queries['as_fun_refresh_results']['Q_rfsh_summary_res']
            Q_del_rfsh                   = queries['as_fun_refresh_results']['Q_del_rfsh']
            Q_sel_refresh_results        = queries['as_fun_refresh_results']['Q_sel_refresh_results']
            Q_del_alloc_summary          = queries['as_fun_refresh_results']['Q_del_alloc_summary']
            Q_delete_alloc_no            = queries['as_fun_refresh_results']['Q_delete_alloc_no']
            Q_ins_alloc_summary          = queries['as_fun_refresh_results']['Q_ins_alloc_summary']
            Q_ins_alloc_refresh_results  = queries['as_fun_refresh_results']['Q_ins_alloc_refresh_results']
            Q_update_refresh_dtlcnt      = queries['as_fun_refresh_results']['Q_update_refresh_dtlcnt']
            Q_update_refresh_loccnt      = queries['as_fun_refresh_results']['Q_update_refresh_loccnt']
            Q_refresh_results            = queries['as_fun_refresh_results']['Q_refresh_results']

            mycursor = conn.cursor()
                                        
            O_status = 1
            mycursor.execute(Q_del_rfsh)
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

                                                                                                 
            O_status = 2
            mycursor.execute(Q_sel_refresh_results)
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

                                                 
            O_status = 3
            mycursor.execute(Q_del_alloc_summary)
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

                                               
            O_status = 4
            mycursor.execute(Q_delete_alloc_no)
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

                                                                          
            O_status = 5
            mycursor.execute(Q_ins_alloc_summary)
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

                                                                                                          
            O_status = 6
            mycursor.execute(Q_ins_alloc_refresh_results)
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

                                                                              
            O_status = 7
            mycursor.execute(Q_update_refresh_dtlcnt)
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

                                                                              
            O_status = 8
            mycursor.execute(Q_update_refresh_loccnt)
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

            conn.commit() 
            Q_rfsh_result = pd.read_sql(Q_refresh_results,conn)
            conn.cursor().close()
            return Q_rfsh_result, ''

    except Exception as error:
        print("Failed to update table record: {}".format(error))
        err_return= ''
        if O_status == 0:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during execution of count of alloc summary : "+ str(error)
            #print(L_func_name,":",O_status,":","Exception raised during execution of count of alloc summary:")
        elif O_status == 1:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during execution of delete queries for alloc_no: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception raised during execution of delete queries for alloc_no:")
        elif O_status == 2:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during inserting records into alloc_sel_alloc_no: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception raised during inserting records into alloc_sel_alloc_no")
        elif O_status == 3:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during deleting records from alloc_summary: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception raised during deleting records from alloc_summary")
        elif O_status == 4:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during deleting records from Q_delete_alloc_no: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception raised during deleting records from Q_delete_alloc_no")
        elif O_status == 5:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during inserting records into alloc_summary: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception raised during inserting records into alloc_summary")
        elif O_status == 6:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during inserting records into Q_ins_alloc_no_refresh_results: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception raised during inserting records into Q_ins_alloc_no_refresh_results")
        elif O_status == 7:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during updating records dtlcount in alloc_summary: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception raised during updating records dtlcount in alloc_summary")
        elif O_status == 8:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during updating records loccount in alloc_summary: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception raised during updating records loccount in alloc_summary")
        else:            
            err_return = L_func_name+":"+str(O_status)+": Exception occured : "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured: ",error)
        conn.rollback()
        conn.cursor().close()
        return no_data, err_return

def populate_alert_results(conn,O_status):
    L_func_name = "populate_alert_results"

    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/ASQuery.yaml') as asq:
            queries               = yaml.load(asq, Loader=yaml.SafeLoader)
            Q_del_alloc_alert     = queries['as_fun_alert_results']['Q_del_alloc_alert']
            Q_ins_alloc_alert     = queries['as_fun_alert_results']['Q_ins_alloc_alert']
            Q_update_alert_dtlcnt = queries['as_fun_alert_results']['Q_update_alert_dtlcnt']
            Q_update_alert_loccnt = queries['as_fun_alert_results']['Q_update_alert_loccnt']
            Q_alert_results       = queries['as_fun_alert_results']['Q_alert_results']

            mycursor = conn.cursor()
            
            mycursor.execute(Q_del_alloc_alert)
            O_status = 1
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

            mycursor.execute(Q_ins_alloc_alert)
            O_status = 2
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

            mycursor.execute(Q_update_alert_dtlcnt)
            O_status = 3
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

            mycursor.execute(Q_update_alert_loccnt)
            O_status = 4
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

            conn.commit() 
            Q_alert_result = pd.read_sql(Q_alert_results,conn)
            conn.cursor().close()
            return Q_alert_result, ""

    except Exception as error:
        print("Failed to update table record: {}".format(error))   
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during execution of deleting records from alloc_summary: "+ str(error)
        elif O_status == 2:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during inserting records into from alloc_summaryd : "+ str(error)
        elif O_status == 3:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during updating records dtlcnt in alloc_summary : "+ str(error)
        elif O_status == 4:
            err_return = L_func_name+":"+str(O_status)+": Exception raised during deleting records loccnt in alloc_summary: "+ str(error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured : "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return
