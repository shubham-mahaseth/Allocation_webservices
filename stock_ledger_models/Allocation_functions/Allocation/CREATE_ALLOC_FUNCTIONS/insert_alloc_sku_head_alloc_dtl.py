
import pandas as pd
import yaml
from datetime import date
from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
import yaml

def insert_tables(conn,O_status,I_alloc_no):
    L_func_name = "insert_tables"
    try:
        I_get_mysql_conn = list()
        I_get_mysql_conn.append(0)
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/insert_alloc_hdr_alloc_dtl_queries.yaml') as fh:
           queries               = yaml.load(fh, Loader=yaml.SafeLoader)
           Q_sel_rec_sys_opt     = queries['insert_tables']['Q_sel_rec_sys_opt']
           Q_sel_alloc_level     = queries['insert_tables']['Q_sel_alloc_level']
           Q_sel_nextval         = queries['insert_tables']['Q_sel_nextval']
           Q_ins_hdr_tmp_tbl     = queries['insert_tables']['Q_ins_hdr_tmp_tbl']
           Q_ins_hdr_tmp_tbl_d   = queries['insert_tables']['Q_ins_hdr_tmp_tbl_d']
           Q_ins_dtl_tmp_tbl     = queries['insert_tables']['Q_ins_dtl_tmp_tbl']
           Q_sel_prcoess_id_hdr  = queries['insert_tables']['Q_sel_prcoess_id_hdr']
           Q_ins_alloc_sku_head  = queries['insert_tables']['Q_ins_alloc_sku_head']
           Q_sel_dtl_process_id  = queries['insert_tables']['Q_sel_dtl_process_id']
           Q_ins_alloc_dtl       = queries['insert_tables']['Q_ins_alloc_dtl']
           Q_ins_alloc_xref      = queries['insert_tables']['Q_ins_alloc_xref']
        
        
           GP_options_populated = False
        
           mycursor=conn.cursor()
        
           O_status = 1
           df_sel_rec_sys_opt = pd.read_sql(Q_sel_rec_sys_opt,conn)
           L_chck_cntry_id=df_sel_rec_sys_opt["BASE_COUNTRY_ID"][0] #incorporated POPULATE_SYSTEM_OPTIONS function validation

           
           if GP_options_populated == False:  #incorporated GET_DEFAULT_ORDER_TYPE function validation start
               if L_chck_cntry_id == None:    #incorporated POPULATE_SYSTEM_OPTIONS function validation start 
                   print("INV_CURSOR")
                   err_msg = "BASE_COUNTRY_ID IS NULL"
                   return False,err_msg
               GP_options_populated = True 
           L_sel_deflt_ord_type=df_sel_rec_sys_opt["DEFAULT_ORDER_TYPE"][0]
        
           #-----------POPULATE_SYSTEM_OPTIONS function end------------#
        
           O_status = 2 
           df_chck_alloc_lvl = pd.read_sql(Q_sel_alloc_level,conn,params=(I_alloc_no,))
           L_sel_alloc_ind = df_chck_alloc_lvl["alloc_level"][0]

           df_nextval_procs_id = pd.read_sql(Q_sel_nextval,conn)
           L_seq_process_id_int =df_nextval_procs_id.alloc_sync_process_id[0]
           L_seq_process_id = int(L_seq_process_id_int)
           print("L_seq_process_id")
        
           if L_sel_alloc_ind == 'T':
               print("L_sel_alloc_ind",L_sel_alloc_ind)
        
               O_status = 3
               
               #should lock the table and release the lock
               mycursor.execute("LOCK TABLES alloc_sync_header_temp WRITE,alloc_head a READ,alloc_item_location l READ,alloc_itm_search_dtl ast READ, alloc_item_source_dtl ais READ;")
               mycursor.execute(Q_ins_hdr_tmp_tbl,(L_seq_process_id,I_alloc_no,))
               print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
               mycursor.execute("UNLOCK TABLES;")
               #print("data inserted into alloc_sync_header_tmp",mycursor.rowcount)
           else:
               mycursor.execute(Q_ins_hdr_tmp_tbl_d,(L_seq_process_id,I_alloc_no,))
               print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
               #print("data inserted into alloc_sync_header_tmp for style diff",mycursor.rowcount)
        
           O_status = 4
           mycursor.execute(Q_ins_dtl_tmp_tbl,(L_seq_process_id,L_seq_process_id,))
           print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
           #print("data inserted into alloc_sync_detail_tmp table",mycursor.rowcount)
        
           O_status = 5
           df_sel_prc_ind_hdr = pd.read_sql(Q_sel_prcoess_id_hdr,conn,params=(I_alloc_no,))
           L_sel_proc_ind_hdr= int(df_sel_prc_ind_hdr["alloc_sync_process_id"][0])
           print("L_sel_proc_ind_hdr",L_sel_proc_ind_hdr)
        
           O_status = 6
           mycursor.execute(Q_ins_alloc_sku_head,(L_sel_proc_ind_hdr,))
           print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
           #print("data inserted into alloc_sku_head table",mycursor.rowcount)   
        
           O_status = 7
           df_sel_prc_ind_dtl = pd.read_sql(Q_sel_dtl_process_id,conn,params=(I_alloc_no,))
           L_sel_proc_ind_dtl = int(df_sel_prc_ind_dtl["alloc_sync_process_id"][0])
           print("L_sel_proc_ind_hdr",L_sel_proc_ind_dtl)
           
           O_status = 8
           mycursor.execute(Q_ins_alloc_dtl,(L_sel_proc_ind_dtl,))
           print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
           #print("data inserted into alloc_dtl table",mycursor.rowcount)
        
           O_status = 9
           mycursor.execute(Q_ins_alloc_xref,(L_sel_proc_ind_dtl,L_sel_proc_ind_hdr,))
           print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
           #print("data inserted into alloc_dtl table",mycursor.rowcount)
        
        conn.commit()
        conn.cursor().close()
        return True,""


    except Exception as error:
        err_return = ""
        if O_status == 1:
            print(L_func_name,":",O_status,":","Exception occured while selecting the status from alloc_head table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while selecting the status from alloc_head table :"+ str(error)
        elif O_status == 2:
            print(L_func_name,":",O_status,":","Exception occured while selecting alloc_level from alloc_head ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while selecting alloc_level from alloc_head :"+ str(error)
        elif O_status == 3:
            print(L_func_name,":",O_status,":","Exception occured while inserting data into alloc_sync_header_tmp table ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting data into alloc_sync_header_tmp table :"+ str(error)
        elif O_status == 4:
            print(L_func_name,":",O_status,":","Exception occured while inserting data into alloc_sync_detail_tmp table ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting data into alloc_sync_detail_tmp table :"+ str(error)
        elif O_status == 5:
            print(L_func_name,":",O_status,":","Exception occured while selecting alloc_sync_process_id from alloc_sync_header_tmp table ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while selecting alloc_sync_process_id from alloc_sync_header_tmp table :"+ str(error)
        elif O_status == 6:
            print(L_func_name,":",O_status,":","Exception occured while inserting data into alloc_sku_head ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting data into alloc_sku_head :"+ str(error)
        elif O_status == 7:
            print(L_func_name,":",O_status,":","Exception occured while selecting alloc_sync_process_id from alloc_sync_detail_tmp table ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while selecting alloc_sync_process_id from alloc_sync_detail_tmp table :"+ str(error)
        elif O_status == 8:
            print(L_func_name,":",O_status,":","Exception occured while inserting data into alloc_dtl table ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting data into alloc_dtl table :"+ str(error)
        elif O_status == 8:
            print(L_func_name,":",O_status,":","Exception occured while inserting data into alloc_xref table ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting data into alloc_xref table :"+ str(error)
        else:
            print(L_func_name,":",O_status,":","Exception occured: ", error)
            err_return = L_func_name+": "+"Exception occured :"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False,err_return




#if __name__ == "__main__":
#    I_alloc_no=332
#    I_status = 'A'
#    O_status=None
#    conn=None
#    daily_view = insert_tables(conn,O_status,I_alloc_no,I_status)  
#    print(daily_view);




#this function will insert header,detail temp table and alloc_header,alloc_dtl table incorporated GET_DEFAULT_ORDER_TYPE and POPULATE_SYSTEM_OPTIONS 



