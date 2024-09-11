from pickle import FALSE
import pandas as pd
import yaml
doc_type = None
order_no = None


def retreive_wisummary_details(conn,
                               I_search,
                               O_status): 
    L_func_name = "retreive_wisummary_details"
    no_data = list()
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/wisummary.yaml') as ws:
            queries            = yaml.load(ws,Loader=yaml.SafeLoader)
            Q_chck_wis_data    = queries['func_retreive_dtls']['Q_chck_wis_data']
            Q_crt_hdr_tmp_tbl  = queries['func_retreive_dtls']['Q_crt_hdr_tmp_tbl']
            Q_crt_dtl_tmp_tbl  = queries['func_retreive_dtls']['Q_crt_dtl_tmp_tbl']
            Q_chck_data        = queries['func_retreive_dtls']['Q_chck_data']
            Q_chck_cnt         = queries['func_retreive_dtls']['Q_chck_cnt']
            Q_del_wis_data     = queries['func_retreive_dtls']['Q_del_wis_data']
            Q_ins_wis_hdr      = queries['func_retreive_dtls']['Q_ins_wis_hdr']
            Q_ins_wis_dtl      = queries['func_retreive_dtls']['Q_ins_wis_dtl']
            Q_ins_dtl          = queries['func_retreive_dtls']['Q_ins_dtl']
            Q_ins_hdr          = queries['func_retreive_dtls']['Q_ins_hdr']
            Q_wis_hdr_data     = queries['func_retreive_dtls']['Q_wis_hdr_data']
            Q_wis_dtl_data     = queries['func_retreive_dtls']['Q_wis_dtl_data']
            #style/diff
            Q_alloc_level      = queries['func_retreive_dtls']['Q_alloc_level']
            Q_del_hdr_gtt      = queries['func_retreive_dtls']['Q_del_hdr_gtt']
            Q_del_dtl_gtt      = queries['func_retreive_dtls']['Q_del_dtl_gtt']

            mycursor = conn.cursor()

            I_alloc_no = I_search["I_alloc_no"]
            I_po_type  = I_search["I_po_type"]
            I_multi_wh = I_search["I_multi_wh"]

            mycursor.execute(Q_crt_hdr_tmp_tbl)
            mycursor.execute(Q_crt_dtl_tmp_tbl)
            
            df_alloc_level = pd.read_sql(Q_alloc_level,conn,params=(I_alloc_no,))
            L_alloc_level  = df_alloc_level.alloc_level[0]

            df_chck_wis_data = pd.read_sql(Q_chck_wis_data,conn,params=(I_po_type,I_alloc_no))
            if len(df_chck_wis_data) > 0:
                L_chck_wis_po_type = df_chck_wis_data.po_type[0]
                L_chck_wis_po_type1 = df_chck_wis_data.po_type1[0]
                if L_chck_wis_po_type != L_chck_wis_po_type1:
                    L_err_message = "PO type cannot be changed"
                    return [],[],L_err_message,False
            

            mycursor.execute(Q_del_hdr_gtt,(I_alloc_no,))
            print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

            mycursor.execute(Q_del_dtl_gtt,(I_alloc_no,))
            print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)


            #else:
            #    mycursor.execute(Q_crt_hdr_tmp_tbl)
            #    mycursor.execute(Q_crt_dtl_tmp_tbl)
            

            df_chck_cnt = pd.read_sql(Q_chck_cnt,conn,params=(I_po_type,I_alloc_no))
            if (len(df_chck_cnt) != 0) and (df_chck_cnt.count_of_rec[0] != 0 and df_chck_cnt.count_of_order_rec[0] == 0):
                O_status = 1
                mycursor.execute(Q_del_wis_data,(I_alloc_no,))
                print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

            df_chck_data = pd.read_sql(Q_chck_data,conn,params=(I_alloc_no,))
            if (len(df_chck_data) != 0) and (df_chck_data.chck[0] == 1):
                O_status = 2
                mycursor.execute(Q_ins_wis_hdr,(I_multi_wh,I_po_type,I_alloc_no))
                print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status = 3
                mycursor.execute(Q_ins_wis_dtl,(I_alloc_no,))
                print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)
            else:
                O_status = 4
                mycursor.execute(Q_ins_dtl,(I_po_type,I_po_type,I_po_type,I_po_type,I_po_type,I_po_type,I_po_type,I_alloc_no,L_alloc_level))
                print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status = 5
                mycursor.execute(Q_ins_hdr,(I_multi_wh,I_po_type,I_alloc_no))
                print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

            df_wis_hdr_data = pd.read_sql(Q_wis_hdr_data,conn,params=(I_alloc_no,))
            df_wis_dtl_data = pd.read_sql(Q_wis_dtl_data,conn,params=(I_alloc_no,))

            O_status = 6
                
            for index, row in df_wis_hdr_data.iterrows():
                L_alloc_no          = f"{row['ALLOC_NO']}"
                L_source_item       = f"{row['SOURCE_ITEM']}"
                L_diff_id           = f"{row['DIFF_ID']}"
                L_supplier          = f"{row['SUPPLIER']}"
                L_wh_id             = f"{row['WH_ID']}"
                L_origin_country_id = f"{row['ORIGIN_COUNTRY_ID']}"
                
                res_function,err_msg = update_supp_country_error(conn,
                                             O_status,
                                             L_alloc_no,         
                                             L_source_item,       
                                             L_diff_id,           
                                             L_supplier,          
                                             L_wh_id,             
                                             L_origin_country_id)
                
                if res_function == False:
                    conn.rollback()    
                    return [], [], str(err_msg), False
                    
            conn.commit()
            return df_wis_hdr_data,df_wis_dtl_data, '', True
    except Exception as error:
        print("Failed to update table record: {}".format(error))
        err_return = ""
        if O_status == 1:
            print(L_func_name,":",O_status,":","Exception raised while deleting data from alloc_whatif_summary table",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while deleting data from alloc_whatif_summary table: "+ str(error)
        elif O_status == 2:
            print(L_func_name,":",O_status,":","Exception raised while inserting in alloc_wisummary_hdr_tmp from wis table",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while inserting in alloc_wisummary_hdr_tmp from wis table: "+ str(error)
        elif O_status == 3:
            print(L_func_name,":",O_status,":","Exception raised while inserting in alloc_wisummary_dtl_tmp from wis table",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while inserting in alloc_wisummary_dtl_tmp from wis table: "+ str(error)
        elif O_status == 4:
            print(L_func_name,":",O_status,":","Exception raised while inserting in alloc_wisummary_hdr_tmp",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while inserting in alloc_wisummary_hdr_tmp: "+ str(error)
        elif O_status == 5:
            print(L_func_name,":",O_status,":","Exception raised while inserting in alloc_wisummary_dtl_tmp",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while inserting in alloc_wisummary_dtl_tmp: "+ str(error)
        elif O_status == 6:
            print(L_func_name,":",O_status,":","Exception raised while calling update_supp_country_error function",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while calling update_supp_country_error function: "+ str(error)
        else:
            print(L_func_name,":",O_status,":","Exception occured: ",error)
            err_return = L_func_name+": Exception occured: "+ str(error)
        conn.rollback()
        return [], [], err_return, False

#---------------------------------------------------------------------------------------------------------------#
def update_supp_country_error(conn,
                              O_status,
                              I_alloc_no,         
                              I_source_item,       
                              I_diff_id,           
                              I_supplier,          
                              I_wh_id,             
                              I_origin_country_id):
    L_func_name = "update_supp_country_error"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/wisummary.yaml') as ws:
            queries    = yaml.load(ws,Loader=yaml.SafeLoader)
            Q_dtl_msg  = queries['func_update_supp_country_error']['Q_dtl_msg']
            Q_dtl_msg1 = queries['func_update_supp_country_error']['Q_dtl_msg1']
            

            mycursor = conn.cursor()

            O_status = 1
            mycursor.execute(Q_dtl_msg,(I_wh_id,I_source_item,I_diff_id,I_alloc_no))
            print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status =2
            mycursor.execute(Q_dtl_msg1,(I_wh_id,I_source_item,I_diff_id,I_alloc_no,I_supplier,I_origin_country_id))
            print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

            conn.commit()
            return True,''
    except Exception as error:
        print("Failed to update table record: {}".format(error))
        err_return = ""
        if O_status == 1:
            print(L_func_name,":",O_status,":","Exception raised while updating data for Sizes under the item/ diff",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while updating data for Sizes under the item/ diff: "+ str(error)
        elif O_status == 2:
            print(L_func_name,":",O_status,":","Exception raised while updating data in alloc_wisummary_dtl_tmp",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while updating data in alloc_wisummary_dtl_tmp: "+ str(error)
        else:
            print(L_func_name,":",O_status,":","Exception occured: ",error)
            err_return = L_func_name+": Exception occured: "+ str(error)
        conn.rollback()
        return False,err_return
#---------------------------------------------------------------------------------------------------------------#
def retreive_wisummary_into_temp(conn,
                                 I_search,
                                 O_status): 
    L_func_name = "retreive_wisummary_into_temp"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/wisummary.yaml') as ws:
            queries            = yaml.load(ws,Loader=yaml.SafeLoader)
            Q_ins_wis_temp     = queries['func_retreive_wis_temp']['Q_ins_wis_temp']
            Q_upd_wis_temp     = queries['func_retreive_wis_temp']['Q_upd_wis_temp']
            Q_upd_dsd_wis_temp = queries['func_retreive_wis_temp']['Q_upd_dsd_wis_temp']
            Q_wis_temp_data    = queries['func_retreive_wis_temp']['Q_wis_temp_data']
            Q_upd_hdr_tmp2     = queries['func_retreive_wis_temp']['Q_upd_hdr_tmp2']
            Q_upd_dtl_tmp2     = queries['func_retreive_wis_temp']['Q_upd_dtl_tmp2']

            mycursor = conn.cursor()

            I_alloc_no = I_search["I_alloc_no"]
            I_multi_wh = I_search["I_multi_wh"]

            O_status = 1
            mycursor.execute(Q_upd_hdr_tmp2,(I_alloc_no,))
            print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status = 2
            mycursor.execute(Q_upd_dtl_tmp2,(I_alloc_no,))
            print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status = 3
            mycursor.execute(Q_upd_wis_temp,(I_multi_wh,I_alloc_no))
            print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status = 4
            mycursor.execute(Q_ins_wis_temp,(I_multi_wh,I_alloc_no))
            print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status = 5
            mycursor.execute(Q_upd_dsd_wis_temp,(I_alloc_no,I_alloc_no))
            print(L_func_name,": ",O_status,"-","rows_affected: ",mycursor.rowcount)

            conn.commit()
            return True,''
    except Exception as error:
        print("Failed to update table record: {}".format(error))
        err_return = ""
        if O_status == 1:
            print(L_func_name,":",O_status,":","Exception raised while updating data in alloc_wisummary_hdr_tmp",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while updating data in alloc_wisummary_dtl_tmp: "+ str(error)
        elif O_status == 2:
            print(L_func_name,":",O_status,":","Exception raised while merging data in alloc_wisummary_dtl_tmp",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while merging data in alloc_wisummary_dtl_tmp: "+ str(error)
        elif O_status == 3:
            print(L_func_name,":",O_status,":","Exception raised while merging data in alloc_whatif_summary",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while merging data in alloc_whatif_summary: "+ str(error)
        elif O_status == 4:
            print(L_func_name,":",O_status,":","Exception raised while merging data in alloc_whatif_summary for WH&CD orders",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while merging data in alloc_whatif_summary for WH&CD orders: "+ str(error)
        elif O_status == 5:
            print(L_func_name,":",O_status,":","Exception raised while merging data in alloc_whatif_summary for DSD orders",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while merging data in alloc_whatif_summary for DSD orders: "+ str(error)
        else:
            print(L_func_name,":",O_status,":","Exception occured: ",error)
            err_return = L_func_name+": Exception occured: "+ str(error)
        conn.rollback()
        return False,err_return

 #FUNCTION TO CHANGE PO QUANTITY  

def upd_po_qty(conn,O_status,I_alloc_no,I_wh_id,I_src_item,I_tran_item,I_diff_id,I_som_qty,po_qty):
    L_func_name = "upd_po_qty"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/wisummary.yaml') as ws:
            #queries            = yaml.load(ws,Loader=yaml.SafeLoader)
            #Q_chck_wis_data    = queries['func_retreive_dtls']['Q_chck_wis_data']
            #Q_crte_gtt_dtl_2 = "CREATE TEMPORARY TABLE IF NOT EXISTS alloc_wisummary_dtl_tmp_2 SELECT * FROM alloc_wisummary_dtl_tmp WHERE alloc_no = %s; "

            Q_sel_rec = "select alloc_no,wh_id,source_item,diff_id,tran_item,som_qty,po_qty from alloc_wisummary_dtl_tmp WHERE alloc_no = %s; "

            Q_upd_po_qty = """UPDATE alloc_wisummary_dtl_tmp
                                SET po_qty = %s
                                WHERE alloc_no = %s
                       AND wh_id = %s
                       AND source_item = %s
                       AND COALESCE(diff_id,'$')=COALESCE(%s,'$')
                       AND tran_item=%s;"""

            print(1)
            mycursor = conn.cursor()
            df_po_qty = pd.read_sql(Q_sel_rec,conn,params=(I_alloc_no,))

            L_alloc_no    = df_po_qty["alloc_no"][0]
            L_wh_id       = df_po_qty["wh_id"][0]
            L_source_item = df_po_qty["source_item"][0]
            L_diff_id     = df_po_qty["diff_id"][0]
            L_tran_item   = df_po_qty["tran_item"][0]
            L_som_qty     = df_po_qty["som_qty"][0]

            print("values of temp2",L_alloc_no,L_wh_id,L_source_item,L_diff_id,L_tran_item,L_som_qty,po_qty)
            O_status = 1
            po_qty = int(po_qty)
            if po_qty <= 0:
                po_qty = 0
           

            print("after if condition")
            print(po_qty,L_som_qty)
            L_po_qty = round(po_qty / (L_som_qty if L_som_qty is not None else 1)) * (L_som_qty if L_som_qty is not None else 1)
            print("changed L_po_qty",L_po_qty)

            O_status = 2
            mycursor.execute(Q_upd_po_qty,(L_po_qty,I_alloc_no,I_wh_id,I_src_item,I_diff_id,I_tran_item,))
           
            conn.commit()
            conn.cursor().close()
            return True, ""

    except Exception as error:
        err_return = ""
        if O_status == 1:
            print(L_func_name,":",O_status,":","Exception raised while creating alloc_wisummary_dtl_tmp_2 table:",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while creating alloc_wisummary_dtl_tmp_2 table: "+ str(error)
        elif O_status == 2:
            print(L_func_name,":",O_status,":","Exception raised while updating the po_qty in alloc_wisummary_dtl_tmp table:",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while updating the po_qty in alloc_wisummary_dtl_tmp table: "+ str(error)
        else:
            print(L_func_name,":",O_status,":","Exception occured: ",error)
            err_return = L_func_name+": Exception occured: "+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False,err_return