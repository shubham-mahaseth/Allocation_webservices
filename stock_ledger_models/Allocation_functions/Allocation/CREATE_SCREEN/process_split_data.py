
import pandas as pd
import yaml
from ..ALLOCATION_SUMMARY.copy_alloc import copy_alloc_data
from ..INVENTORY_SETUP.update_alloc_ext import update_alloc

#--------------------------------------------------------------------------------------------------------------------------------

def split_data(conn,
               I_alloc_no,I_create_id,
               O_status):
    L_func_name = "split_data"
    no_data = list()
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/process_split_data_queries.yaml') as fls:
            queries      = yaml.load(fls,Loader=yaml.SafeLoader)
            Q_chck_split = queries['func_load_split_data']['Q_chck_split']

            mycursor = conn.cursor()
            df_chck = pd.read_sql(Q_chck_split,conn,params=(I_alloc_no,))
            if len(df_chck) > 0:
                L_chck  = df_chck.chck[0]
                if L_chck == 1:
                    print("Allocation cannot be split since the PO is not in approved status.")
                    return no_data,L_func_name+":"+str(O_status)+": "+"Exception :Allocation cannot be split since the PO is not in approved status."
            else:
                O_status = 1
                result1,err_msg1 = load_split(conn,I_alloc_no,O_status)
                if result1 == True:
                    print(O_status,L_func_name,": ","Function load_split")
                else:
                    return no_data, err_msg1
                O_status = 2
                L_new_alloc_no, err_msg2 = copy_alloc_data (conn,I_alloc_no,I_create_id,O_status)
                print(O_status,L_func_name,": ","Func copy_alloc")
                if  len(err_msg2) > 0:
                    return no_data, err_msg2
                O_status = 3
                L_new_alloc_no,err_msg = clean_split_data(conn,I_alloc_no,L_new_alloc_no,O_status)
                print(O_status,L_func_name,": ","clean_split_data")
                if len(str(L_new_alloc_no)) == 0 and len(err_msg) > 0:
                    return no_data, err_msg

            conn.commit()
            conn.cursor().close()
            return L_new_alloc_no, ""
    except Exception as error:
        print("Failed to update table record: {}".format(error)) 
        err_return = ""
        if O_status == 1:
            print(L_func_name,":",O_status,":","Exception raised while calling Function load_split",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while calling Function load_split :"+ str(error)
        elif O_status == 2:
            print(L_func_name,":",O_status,":","Exception raised while calling Function copy_alloc",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while calling Function copy_alloc :"+ str(error)
        elif O_status == 3:
            print(L_func_name,":",O_status,":","Exception raised while calling Function clean_split_data",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while calling Function clean_split_data :"+ str(error)
        else:
            print(L_func_name,":",O_status,":","Exception occured: ",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured :"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return no_data, err_return
#------------------------------------------------------------------

def load_split(conn,
               I_alloc_no,
               O_status):
    L_func_name = "load_split"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/process_split_data_queries.yaml') as fls:
            queries      = yaml.load(fls,Loader=yaml.SafeLoader)
            Q_create_tbl = queries['func_load_split_data']['Q_create_tbl']
            Q_chck_tbl   = queries['func_load_split_data']['Q_chck_tbl']
            Q_del_split  = queries['func_load_split_data']['Q_del_split']
            Q_ins_split  = queries['func_load_split_data']['Q_ins_split']
            Q_update_dtl = queries['func_load_split_data']['Q_update_dtl']

            mycursor = conn.cursor()
            
            df_chck_tbl = pd.read_sql(Q_chck_tbl,conn)
            L_chck_tbl  = df_chck_tbl.chk[0]
            if L_chck_tbl == 1:
                print("alloc_item_loc_split_gtt already exists ")
                return False, L_func_name+":"+str(O_status)+": "+"Exception occured :"+ "alloc_item_loc_split_gtt already exists."
            else:
                mycursor.execute(Q_create_tbl)
                print("table created")
                    
                mycursor.execute(Q_del_split)
                print(O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status = 1
                mycursor.execute(Q_ins_split,(I_alloc_no,))
                O_status = 2
                print(O_status,"-","rows_affected: ",mycursor.rowcount)
                    
                mycursor.execute(Q_update_dtl,(I_alloc_no,))
                O_status = 3
                print(O_status,"-","rows_affected: ",mycursor.rowcount)

                conn.commit()
                conn.cursor().close()
                return True, ""

    except Exception as error:
        print("Failed to update table record: {}".format(error))   
        err_return = ""
        if O_status == 1:
            print(L_func_name,":",O_status,":","Exception raised during deleting records from alloc_item_loc_split",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised during deleting records from alloc_item_loc_split :"+ str(error)
        elif O_status == 2:
            print(L_func_name,":",O_status,":","Exception raised during insering records into alloc_item_loc_split",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised during insering records into alloc_item_loc_split :"+ str(error)
        elif O_status == 3:
            print(L_func_name,":",O_status,":","Exception raised during updating split_ind in alloc_itm_search_dtl",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised during updating split_ind in alloc_itm_search_dtl :"+ str(error)
        else:
            print(L_func_name,":",O_status,":","Exception occured: ",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured :"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

#-----------------------------------------------------------
def clean_split_data(conn,
                     I_alloc_no,
                     I_new_alloc_no,
                     O_status):
    L_func_name = "clean_split_data"
    I_schdl_ind = 'N'
    no_data     = list()
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/process_split_data_queries.yaml') as fls:
            queries                         = yaml.load(fls,Loader=yaml.SafeLoader)
            Q_upd_size_prf_ind              = queries['func_clean_split']['Q_upd_size_prf_ind']
            Q_chck_wif                      = queries['func_clean_split']['Q_chck_wif']
            Q_upd_alloc_head                = queries['func_clean_split']['Q_upd_alloc_head']
            Q_del_calc_src_tmp_y            = queries['func_clean_split']['Q_del_calc_src_tmp_y']
            Q_del_like_item_src_y           = queries['func_clean_split']['Q_del_like_item_src_y']
            Q_del_item_src_dtl_y            = queries['func_clean_split']['Q_del_item_src_dtl_y']
            Q_del_qnty_limits_n             = queries['func_clean_split']['Q_del_qnty_limits_n']
            Q_del_calc_allitem_n            = queries['func_clean_split']['Q_del_calc_allitem_n']
            Q_del_calc_src_tmp_n            = queries['func_clean_split']['Q_del_calc_src_tmp_n']
            Q_del_like_item_src_n           = queries['func_clean_split']['Q_del_like_item_src_n']
            Q_del_item_src_dtl_n            = queries['func_clean_split']['Q_del_item_src_dtl_n']
            Q_mrge_itm_srch_dtl_ca_sel_ind  = queries['func_clean_split']['Q_mrge_itm_srch_dtl_ca_sel_ind']
            Q_mrge_itm_srch_dtl_ca_spt_ind  = queries['func_clean_split']['Q_mrge_itm_srch_dtl_ca_spt_ind']
            Q_mrge_itm_srch_dtl_na_sel_ind  = queries['func_clean_split']['Q_mrge_itm_srch_dtl_na_sel_ind']
            Q_mrge_itm_srch_dtl_na_spt_ind  = queries['func_clean_split']['Q_mrge_itm_srch_dtl_na_spt_ind']
            Q_del_qnty_limits_wh_rel        = queries['func_clean_split']['Q_del_qnty_limits_wh_rel']
            Q_del_qnty_lmts_wi_wh_rel       = queries['func_clean_split']['Q_del_qnty_lmts_wi_wh_rel']
            Q_del_calc_allitemloc_wh_rel    = queries['func_clean_split']['Q_del_calc_allitemloc_wh_rel']
            Q_del_calc_allitemloc_wi_wh_rel = queries['func_clean_split']['Q_del_calc_allitemloc_wi_wh_rel']

            mycursor = conn.cursor()
            result1,err_msg1 = copy_alloc_schl(conn,I_alloc_no,I_new_alloc_no,I_schdl_ind,O_status)
            if result1 == False:
                print("copy_alloc_schl")
                conn.rollback()
                return no_data, err_msg1
            
            O_status = 1
            mycursor.execute(Q_upd_alloc_head,(I_alloc_no,I_new_alloc_no))
            print(L_func_name,":",O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status = 2
            mycursor.execute(Q_del_calc_src_tmp_y,(I_alloc_no,))      #deletes copied allocation data from alloc_calc_src_tmp
            print(L_func_name,":",O_status,"Q_del_calc_src_tmp_y -","rows_affected: ",mycursor.rowcount)

            O_status = 3
            mycursor.execute(Q_del_like_item_src_y,(I_alloc_no,))     #deletes copied allocation data from alloc_like_item_src    
            print(L_func_name,":",O_status,"Q_del_like_item_src_y -","rows_affected: ",mycursor.rowcount)

            O_status = 4
            mycursor.execute(Q_del_item_src_dtl_y,(I_alloc_no,))      #deletes copied allocation data from alloc_item_src_dtl
            print(L_func_name,":",O_status,"Q_del_item_src_dtl_y -","rows_affected: ",mycursor.rowcount)

            O_status = 5
            mycursor.execute(Q_del_qnty_limits_n,(I_alloc_no,))      #deletes copied allocation data from alloc_qnty_limits
            print(L_func_name,":",O_status,"Q_del_qnty_limits_n -","rows_affected: ",mycursor.rowcount)
            O_status = 6
            mycursor.execute(Q_del_calc_allitem_n,(I_alloc_no,I_alloc_no)) #deletes copied allocation data from alloc_calc_allitem_loc
            print(L_func_name,":",O_status,"Q_del_calc_allitem_n -","rows_affected: ",mycursor.rowcount)
            O_status = 7
            mycursor.execute(Q_del_calc_src_tmp_n,(I_new_alloc_no,))  #deletes new allocation data from alloc_calc_src_tmp(split_ind-Y)
            print(L_func_name,":",O_status,"Q_del_calc_src_tmp_n -","rows_affected: ",mycursor.rowcount)

            O_status = 8
            mycursor.execute(Q_del_like_item_src_n,(I_new_alloc_no,))  #deletes new allocation data from alloc_like_item_src(split_ind-Y)
            print(L_func_name,":",O_status,"Q_del_like_item_src_n -","rows_affected: ",mycursor.rowcount)

            O_status = 9
            mycursor.execute(Q_del_item_src_dtl_n,(I_new_alloc_no,))  #deletes new allocation data from alloc_item_src_dtl(split_ind-Y)
            print(L_func_name,":",O_status,"Q_del_item_src_dtl_n -","rows_affected: ",mycursor.rowcount)

            O_status = 10
            mycursor.execute(Q_mrge_itm_srch_dtl_ca_sel_ind,(I_alloc_no,I_alloc_no))      #updates sel_ind in item_srch_dtl 
            print(L_func_name,":",O_status,"Q_mrge_itm_srch_dtl_ca_sel_ind -","rows_affected: ",mycursor.rowcount)

            O_status = 11
            mycursor.execute(Q_mrge_itm_srch_dtl_ca_spt_ind,(I_alloc_no,I_alloc_no))  #deletes copied allocation data from item_srch_dtl 
            print(L_func_name,":",O_status,"Q_mrge_itm_srch_dtl_ca_spt_ind -","rows_affected: ",mycursor.rowcount)

            O_status = 12
            mycursor.execute(Q_mrge_itm_srch_dtl_na_sel_ind,(I_alloc_no,I_new_alloc_no))  #updates new allocation sel_ind in item_srch_dtl
            print(L_func_name,":",O_status,"Q_mrge_itm_srch_dtl_na_sel_ind -","rows_affected: ",mycursor.rowcount)

            O_status = 13
            mycursor.execute(Q_mrge_itm_srch_dtl_na_spt_ind,(I_alloc_no,I_new_alloc_no,))  #deletes new allocation data from item_srch_dtl(split_ind-Y) 
            print(L_func_name,":",O_status,"Q_mrge_itm_srch_dtl_na_spt_ind -","rows_affected: ",mycursor.rowcount)

            
            I_input_data = list()
            result2,err_msg2 = update_alloc(conn,O_status, I_alloc_no, None, None,'Y', I_input_data)
            if result2 == False:
                conn.cursor().close()
                print("Func update_alloc failed")
                return no_data, err_msg2
            
            O_status = 14
            mycursor.execute(Q_upd_size_prf_ind,(I_new_alloc_no,I_new_alloc_no))
            print(L_func_name,":",O_status,"-","rows_affected: ",mycursor.rowcount)

            df_chck_wif = pd.read_sql(Q_chck_wif,conn,params=(I_alloc_no,))
            if len(df_chck_wif) > 0:
                L_chck_wif  = df_chck_wif.chck[0]
                
                if L_chck_wif != 'F':
                    O_status = 15
                    alloc_list = (I_alloc_no,I_new_alloc_no)
                    mycursor.execute(Q_del_qnty_limits_wh_rel.format(alloc_list,I_alloc_no))  #deletes new alloc data from qnty_limts(split_ind-Y) if wh_store_rel-Y
                    print(L_func_name,":",O_status,"-","rows_affected: ",mycursor.rowcount)

                    O_status = 16
                    mycursor.execute(Q_del_calc_allitemloc_wh_rel.format(alloc_list,I_alloc_no))  #deletes new alloc data from calc_allitemloc(split_ind-Y) if wh_store_rel-Y
                    print(L_func_name,":",O_status,"-","rows_affected: ",mycursor.rowcount) 
                else:
                    O_status = 15
                    alloc_list = (I_alloc_no,I_new_alloc_no)
                    mycursor.execute(Q_del_qnty_lmts_wi_wh_rel.format(alloc_list,I_alloc_no))  #deletes new alloc data from qnty_limts(split_ind-Y) if wh_store_rel-Y
                    print(L_func_name,":",O_status,"-","rows_affected: ",mycursor.rowcount)

                    O_status = 16
                    mycursor.execute(Q_del_calc_allitemloc_wi_wh_rel.format(alloc_list,I_alloc_no))  #deletes new alloc data from calc_allitemloc(split_ind-Y) if wh_store_rel-Y
                    print(L_func_name,":",O_status,"-","rows_affected: ",mycursor.rowcount) 

            conn.commit()
            conn.cursor().close()
            return I_new_alloc_no, ""
    except Exception as error:
        print("Failed to update table record: {}".format(error))   
        err_return = ""
        if O_status == 1:
            print(L_func_name,":",O_status,":","Exception raised while executing Q_upd_alloc_head",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while executing Q_upd_alloc_head :"+ str(error)
        elif O_status == 2:
            print(L_func_name,":",O_status,":","Exception raised while executing Q_del_calc_src_tmp_y",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while executing Q_del_calc_src_tmp_y :"+ str(error)
        elif O_status == 3:
            print(L_func_name,":",O_status,":","Exception raised while executing Q_del_like_item_src_y",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while executing Q_del_like_item_src_y :"+ str(error)
        elif O_status == 4:
            print(L_func_name,":",O_status,":","Exception raised while executing Q_del_item_src_dtl_y",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while executing Q_del_item_src_dtl_y :"+ str(error)
        elif O_status == 5:
            print(L_func_name,":",O_status,":","Exception raised while executing Q_del_qnty_limits_n",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while executing Q_del_qnty_limits_n :"+ str(error)
        elif O_status == 6:
            print(L_func_name,":",O_status,":","Exception raised while executing Q_del_calc_allitem_n",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while executing Q_del_calc_allitem_n :"+ str(error)
        elif O_status == 7:
            print(L_func_name,":",O_status,":","Exception raised while executing Q_del_calc_src_tmp_n",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while executing Q_del_calc_src_tmp_n :"+ str(error)
        elif O_status == 8:
            print(L_func_name,":",O_status,":","Exception raised while executing Q_del_like_item_src_n",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while executing Q_del_like_item_src_n :"+ str(error)
        elif O_status == 9:
            print(L_func_name,":",O_status,":","Exception raised while executing Q_del_item_src_dtl_n",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while executing Q_del_item_src_dtl_n :"+ str(error)
        elif O_status == 10:
            print(L_func_name,":",O_status,":","Exception raised while executing Q_mrge_itm_srch_dtl_ca_sel_ind",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while executing Q_mrge_itm_srch_dtl_ca_sel_ind :"+ str(error)
        elif O_status == 11:
            print(L_func_name,":",O_status,":","Exception raised while executing Q_mrge_itm_srch_dtl_ca_spt_ind",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while executing Q_mrge_itm_srch_dtl_ca_spt_ind :"+ str(error)
        elif O_status == 12:
            print(L_func_name,":",O_status,":","Exception raised while executing Q_mrge_itm_srch_dtl_na_sel_ind",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while executing Q_mrge_itm_srch_dtl_na_sel_ind :"+ str(error)
        elif O_status == 13:
            print(L_func_name,":",O_status,":","Exception raised while executing Q_mrge_itm_srch_dtl_na_spt_ind",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while executing Q_mrge_itm_srch_dtl_na_spt_ind :"+ str(error)
        elif O_status == 14:
            print(L_func_name,":",O_status,":","Exception raised while executing Q_upd_size_prf_ind",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while executing Q_upd_size_prf_ind :"+ str(error)
        elif O_status == 15:
            print(L_func_name,":",O_status,":","Exception raised while executing Q_del_qnty_limits_wh_rel",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while executing Q_del_qnty_limits_wh_rel :"+ str(error)
        elif O_status == 16:
            print(L_func_name,":",O_status,":","Exception raised while executing Q_del_calc_allitemloc_wh_rel",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while executing Q_del_calc_allitemloc_wh_rel :"+ str(error)
        else:
            print(L_func_name,":",O_status,":","Exception occured: ",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured :"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return no_data,err_return


#-------------------------------------------------------------------------------
def copy_alloc_schl(conn,
                    I_alloc_no,
                    I_new_alloc_no,
                    I_schdl_ind,
                    O_status):

    L_func_name = "copy_alloc_schl"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/process_split_data_queries.yaml') as fls:
            queries                 = yaml.load(fls,Loader=yaml.SafeLoader)
            Q_merge_srch_dtl        = queries['copy_alloc_schl']['Q_merge_srch_dtl']
            Q_merge_schdl_srch_dtl  = queries['copy_alloc_schl']['Q_merge_schdl_srch_dtl']
            Q_ins_qty_limits        = queries['copy_alloc_schl']['Q_ins_qty_limits']

            mycursor = conn.cursor()

            if I_schdl_ind == 'N':
                O_status = 1
                mycursor.execute(Q_merge_srch_dtl,(I_alloc_no,I_new_alloc_no))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)
            else:
                O_status = 2
                mycursor.execute(Q_merge_schdl_srch_dtl,(I_alloc_no,I_new_alloc_no))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status = 3
            mycursor.execute(Q_ins_qty_limits,(I_new_alloc_no,I_alloc_no))
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

            conn.commit()
            conn.cursor().close()
            return True, ""

    except Exception as error:
        print("Failed to update table record: {}".format(error))   
        err_return = ""
        if O_status == 1:
            print(L_func_name,":",O_status,":","Exception raised while updating in Q_merge_srch_dtl",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while updating in Q_merge_srch_dtl :"+ str(error)
        elif O_status == 3:
            print(L_func_name,":",O_status,":","Exception raised during insering in Q_ins_qty_limits",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised during insering in Q_ins_qty_limits :"+ str(error)
        elif O_status == 2:
            print(L_func_name,":",O_status,":","Exception raised while updating in Q_merge_schdl_srch_dtl",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception raised while updating in Q_merge_schdl_srch_dtl :"+ str(error)
        else:
            print(L_func_name,":",O_status,":","Exception occured: ",error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured :"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False, err_return