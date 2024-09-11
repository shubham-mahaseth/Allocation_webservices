from ..INVENTORY_SETUP.load_item_source import load_item
from ..INVENTORY_SETUP.inventory_setup import update_inv,setup_item_location,setup_location
from ..GLOBAL_FILES.null_handler import null_py
from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy

import pandas as pd
import yaml


def calculate_validation (conn
                         ,I_alloc
                         ,O_status):
    L_func_name ="calculate_validation"

    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculate_validation_queries.yaml') as fh:
            queries                         = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_get_release_date              = queries['calculate_validate_new']['Q_get_release_date']
            Q_check_error                   = queries['calculate_validate_new']['Q_check_error']
            Q_get_rule_rec                  = queries['calculate_validate_new']['Q_get_rule_rec']
            Q_get_item_source_found         = queries['calculate_validate_new']['Q_get_item_source_found']
            Q_get_item_loc_found            = queries['calculate_validate_new']['Q_get_item_loc_found']
            Q_upd_itm_search_dtl            = queries['calculate_validate_new']['Q_upd_itm_search_dtl']    #Changes by Shubham
            Q_po_alloc                      = queries['calculate_validate_new']['Q_po_alloc']
            Q_get_days                      = queries['calculate_validate_new']['Q_get_days']
            Q_sp_week                       = queries['calculate_validate_new']['Q_sp_week']
            Q_del_itm_srch_tmp              = queries['calculate_validate_new']['Q_del_itm_srch_tmp']
            Q_cre_alloc_po_eisd_temp        = queries['calculate_validate_new']['Q_cre_alloc_po_eisd_temp']
            Q_del_po_eisd_gtt               = queries['calculate_validate_new']['Q_del_po_eisd_gtt']
            Q_ins_po_eisd_gtt_p             = queries['calculate_validate_new']['Q_ins_po_eisd_gtt_p']
            Q_ins_po_eisd_gtt_a             = queries['calculate_validate_new']['Q_ins_po_eisd_gtt_a']    
            Q_mer_1                         = queries['calculate_validate_new']['Q_mer_1']  
            Q_mer_2                         = queries['calculate_validate_new']['Q_mer_2']  
            Q_get_rec                       = queries['calculate_validate_new']['Q_get_rec']  
            Q_get_count_total_skus          = queries['calculate_validate_new']['Q_get_count_total_skus']  
            Q_get_count_total_locs          = queries['calculate_validate_new']['Q_get_count_total_locs']
            Q_get_count_total_whs           = queries['calculate_validate_new']['Q_get_count_total_whs']  
            Q_upd_err_1                     = queries['calculate_validate_new']['Q_upd_err_1']
            Q_upd_err_2                     = queries['calculate_validate_new']['Q_upd_err_2']
            Q_upd_err_3                     = queries['calculate_validate_new']['Q_upd_err_3']
            Q_upd_err_4                     = queries['calculate_validate_new']['Q_upd_err_4']
            Q_upd_err_5                     = queries['calculate_validate_new']['Q_upd_err_5']
            Q_upd_err_6                     = queries['calculate_validate_new']['Q_upd_err_6']
            Q_get_wh_str_rel_ind            = queries['calculate_validate_new']['Q_get_wh_str_rel_ind']
            Q_mer_3                         = queries['calculate_validate_new']['Q_mer_3']
            Q_mer_4                         = queries['calculate_validate_new']['Q_mer_4']
            Q_mer_5                         = queries['calculate_validate_new']['Q_mer_5']
            Q_upd_err_7                     = queries['calculate_validate_new']['Q_upd_err_7']
            Q_upd_err_8                     = queries['calculate_validate_new']['Q_upd_err_8']
            Q_upd_err_9                     = queries['calculate_validate_new']['Q_upd_err_9']
            Q_get_rec_2                     = queries['calculate_validate_new']['Q_get_rec_2']
            Q_upd_err_10                    = queries['calculate_validate_new']['Q_upd_err_10']
            Q_upd_err_11                    = queries['calculate_validate_new']['Q_upd_err_11']
            Q_upd_err_12                    = queries['calculate_validate_new']['Q_upd_err_12']
            Q_upd_err_13                    = queries['calculate_validate_new']['Q_upd_err_13']
            Q_upd_err_14                    = queries['calculate_validate_new']['Q_upd_err_14']
            Q_upd_err_15                    = queries['calculate_validate_new']['Q_upd_err_15']
            Q_upd_err_16                    = queries['calculate_validate_new']['Q_upd_err_16']
            Q_upd_err_17                    = queries['calculate_validate_new']['Q_upd_err_17']
            Q_upd_err_18                    = queries['calculate_validate_new']['Q_upd_err_18']
            Q_upd_err_19                    = queries['calculate_validate_new']['Q_upd_err_19']
            Q_upd_err_20                    = queries['calculate_validate_new']['Q_upd_err_20']
            Q_upd_err_21                    = queries['calculate_validate_new']['Q_upd_err_21']
            Q_upd_err_22                    = queries['calculate_validate_new']['Q_upd_err_22']
            Q_upd_err_23                    = queries['calculate_validate_new']['Q_upd_err_23']
            Q_upd_err_24                    = queries['calculate_validate_new']['Q_upd_err_24']
            Q_upd_err_25                    = queries['calculate_validate_new']['Q_upd_err_25']
            Q_upd_err_26                    = queries['calculate_validate_new']['Q_upd_err_26']
            Q_upd_err_27                    = queries['calculate_validate_new']['Q_upd_err_27']
            Q_upd_err_28                    = queries['calculate_validate_new']['Q_upd_err_28']
            Q_upd_err_29                    = queries['calculate_validate_new']['Q_upd_err_29']
            Q_upd_err_30                    = queries['calculate_validate_new']['Q_upd_err_30']
            Q_upd_err_31                    = queries['calculate_validate_new']['Q_upd_err_31']
            
            
            
            mycursor=conn.cursor()
            O_status = 1
            mycursor.execute(Q_cre_alloc_po_eisd_temp)
            df_get_release_date = pd.read_sql(Q_get_release_date,conn, params=(I_alloc,))
            print(df_get_release_date)
            L_release_date = df_get_release_date.release_date[0]
            L_alloc_level = df_get_release_date.alloc_level[0]
            print("L_release_date is:",L_release_date)
            print("L_alloc_level is:",L_alloc_level)
            O_status = 2
            check_UInv, err_msg = update_inv(conn,I_alloc,O_status)
            if check_UInv == False:
                return False, L_func_name+":"+str(O_status)+": "+ err_msg
            
            mycursor.execute(Q_del_itm_srch_tmp,(I_alloc,))
            
            conn.commit()
            O_status = 3
            check_LI , err_msg = load_item(conn,I_alloc,O_status)
            if check_LI == False:
                return False, L_func_name+":"+str(O_status)+": "+ err_msg

            conn.commit()
            O_status = 4
            L_found =0
            df_get_item_source_found = pd.read_sql(Q_get_item_source_found, conn, params=(I_alloc,))
            if len(df_get_item_source_found) >0:
                L_found= df_get_item_source_found.L_found[0]
            O_status = 5
            if null_py(L_found,0) == 0:
                check_LI , err_msg = load_item(conn,I_alloc,O_status)
                if check_LI == False:
                    return False,L_func_name+":"+str(O_status)+": "+ err_msg
                O_status = 6
                L_found1=0
                df_get_item_source_found = pd.read_sql(Q_get_item_source_found, conn, params=(I_alloc,))
                if len(df_get_item_source_found) >0:
                    L_found1= df_get_item_source_found.L_found[0]
                if null_py(L_found1,0) == 0:
                    O_status = 7
                    check_SIL, err_msg =  setup_item_location (conn,I_alloc,O_status)
                    if check_SIL == False:
                        return False, L_func_name+":"+str(O_status)+": "+ err_msg
            
            O_status = 8
            df_get_item_loc_found = pd.read_sql(Q_get_item_loc_found, conn, params=(I_alloc,))
            if len(df_get_item_loc_found) >0:
                L_found = df_get_item_loc_found.L_found[0]
            else:
                L_found =0

            O_status = 9
            if null_py(L_found,0) == 0:
                df_get_rule_rec = pd.read_sql(Q_get_rule_rec, conn, params=(I_alloc,))
                if len(df_get_rule_rec) >0:
                    check_SL, err_msg = setup_location (conn,I_alloc,O_status)
                    if check_SL == False:
                        return False, L_func_name+":"+str(O_status)+": "+ err_msg
            conn.commit()

            L_found=0
            O_status = 10
            mycursor.execute(Q_upd_itm_search_dtl,(I_alloc,))                                                    #Changes by Shubham
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_itm_search_dtl")       #Changes by Shubham
            conn.commit()                                                                                        #Changes by Shubham

            df_po_alloc = pd.read_sql(Q_po_alloc, conn, params=(I_alloc,))
            L_alloc_doc_type = df_po_alloc.alloc_criteria[0]
            L_storm_wh_chk = df_po_alloc.loc_config[0]

            df_sp_week = pd.read_sql(Q_sp_week, conn)
            L_sp_week = df_sp_week.code_desc[0]
            O_status = 11
            if L_alloc_doc_type in ('A','P'):
                mycursor.execute(Q_del_po_eisd_gtt)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_del_po_eisd_gtt")
                O_status = 12
                if L_alloc_doc_type== 'P':
                    mycursor.execute(Q_ins_po_eisd_gtt_p,(I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_ins_po_eisd_gtt_p")
                    
                elif L_alloc_doc_type== 'A':
                    mycursor.execute(Q_ins_po_eisd_gtt_a,(I_alloc,))
                O_status = 13
                L_params=(I_alloc,L_sp_week,L_sp_week,L_sp_week,L_sp_week)
                L_params= convert_numpy(L_params)
                mycursor.execute(Q_mer_1,L_params)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_mer_1")

            else:
                O_status = 13.1
                L_params= (I_alloc,L_sp_week,L_sp_week)
                L_params=convert_numpy(L_params)
                mycursor.execute(Q_mer_2,L_params)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_mer_2")

            conn.commit()
            O_status = 14
            if L_alloc_doc_type=='F':
                df_rec = pd.read_sql(Q_get_rec, conn, params=(I_alloc,))
                if len(df_rec) >0:
                    print("df_rec is :",df_rec)
                    for i in range(len(df_rec)):
                        if L_alloc_level=='T':
                            L_total_skus =1
                            print("L_total_skus inside if is:", L_total_skus)
                        else:
                            df_total_skus = pd.read_sql(Q_get_count_total_skus, conn, params=(df_rec.item[i],df_rec.diff_id[i]))
                            L_total_skus= df_total_skus.total_skus[0]
                            print("L_total_skus inside else is:", L_total_skus)
                        print("df_rec.alloc_no[i] is:", df_rec.alloc_no[i])
                        L_alloc_no = (df_rec.alloc_no[i],)
                        L_alloc_no= convert_numpy(L_alloc_no)
                        print("L_alloc_no is:", L_alloc_no)
                        df_total_locs = pd.read_sql(Q_get_count_total_locs, conn, params=L_alloc_no)
                        L_total_locs = df_total_locs.total_locs[0]
                        print("L_total_locs is :", L_total_locs)
                        df_total_whs = pd.read_sql(Q_get_count_total_whs,conn,params=L_alloc_no)
                        L_total_whs = df_total_whs.total_whs[0]
                        print("L_total_whs is :", L_total_whs)
                        O_status = 15
                        if L_alloc_level== 'T':
                            #Checking all skus are not active in some selected locations ( or all skus are active in atleast one selected locations)
                            L_params_err1 = (I_alloc,df_rec.item[i],I_alloc,df_rec.item[i],df_rec.alloc_no[i],I_alloc,df_rec.item[i],df_rec.alloc_no[i],L_total_locs)
                            L_params_err1 = convert_numpy(L_params_err1)
                            mycursor.execute(Q_upd_err_1,L_params_err1)
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_1")
                            conn.commit()

                            #All the items are inactive/discontinued at some warehouse.
                            L_params_err2= (I_alloc,df_rec.item[i],df_rec.diff_id[i],df_rec.alloc_no[i],L_total_whs)
                            L_params_err2 = convert_numpy(L_params_err2)
                            mycursor.execute(Q_upd_err_2, L_params_err2)
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_2")
                            conn.commit()
                        else:
                            #Checking all skus are not active in some selected locations ( or all skus are active in atleast one selected locations)
                            L_params_err3 = (I_alloc,df_rec.item[i],df_rec.diff_id[i],df_rec.item[i],df_rec.diff_id[i],df_rec.diff_id[i],df_rec.alloc_no[i],L_total_locs,L_total_skus)
                            L_params_err3 = convert_numpy(L_params_err3)
                            mycursor.execute(Q_upd_err_3,L_params_err3)
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_3")
                            conn.commit()

                            #Checking all skus are not active in some wharehouse
                            L_params_err4 =(I_alloc,df_rec.item[i],df_rec.diff_id[i],df_rec.item[i],df_rec.diff_id[i],df_rec.diff_id[i],df_rec.alloc_no[i],L_total_whs,L_total_skus)
                            L_params_err4 = convert_numpy(L_params_err4)
                            mycursor.execute(Q_upd_err_4,L_params_err4)
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_4")
                            conn.commit()
                        O_status = 16
                        if L_alloc_level =='D':
                            #Checking some skus are not active in all selected locations (or atleast one sku is active in all selected locations)
                            L_params_err5 =(I_alloc,df_rec.item[i],df_rec.diff_id[i],df_rec.item[i],df_rec.diff_id[i],df_rec.diff_id[i],df_rec.alloc_no[i],L_total_locs,L_total_skus)
                            L_params_err5 = convert_numpy(L_params_err5)
                            mycursor.execute(Q_upd_err_5,L_params_err5)
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_5")
                            conn.commit()
                            #Checking some skus are not active in all wharehouse
                            L_params_err6 = (I_alloc,df_rec.item[i],df_rec.diff_id[i],df_rec.item[i],df_rec.diff_id[i],df_rec.diff_id[i],df_rec.alloc_no[i],L_total_whs,L_total_skus)
                            L_params_err6 = convert_numpy(L_params_err6)
                            mycursor.execute(Q_upd_err_6,L_params_err6 )
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_6")
                            conn.commit()
            O_status = 17
            if L_alloc_level=='T':
                df_wh_str_ind = pd.read_sql(Q_get_wh_str_rel_ind, conn, params=(I_alloc,))
                L_enforce_wh_store_rel_ind = df_wh_str_ind.WH_STORE_REL_IND[0]
                print("L_enforce_wh_store_rel_ind is:",L_enforce_wh_store_rel_ind)

                if L_enforce_wh_store_rel_ind=='Y':
                    mycursor.execute(Q_mer_3,(I_alloc,I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_mer_3")
                    conn.commit()
                    mycursor.execute(Q_mer_4,(I_alloc,I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_mer_4")
                    conn.commit()
                else:
                    mycursor.execute(Q_mer_5,(I_alloc,I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_mer_5")
            conn.commit()
            O_status = 18
            df_wh_str_ind = pd.read_sql(Q_get_wh_str_rel_ind, conn, params=(I_alloc,))
            L_enforce_wh_store_rel_ind = df_wh_str_ind.WH_STORE_REL_IND[0]
            if L_alloc_doc_type!='F' and L_enforce_wh_store_rel_ind=='N':
                if L_alloc_level=='T':
                    #Checking all skus are not active in some selected locations ( or all skus are active in atleast one selected locations)
                    mycursor.execute(Q_upd_err_7,(I_alloc,I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_7")
                    conn.commit()
            O_status = 19
            if L_alloc_level=='T':
                mycursor.execute(Q_upd_err_8,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_8")
            mycursor.execute(Q_upd_err_9,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_9")
            O_status = 20
            if L_alloc_doc_type=='F':
                df_rec_2 = pd.read_sql(Q_get_rec_2, conn, params=(I_alloc,))
                if len(df_rec_2) >0:
                    for i in range(len(df_rec_2)):
                        L_rec_item=df_rec_2.item[i]
                        L_rec_alloc_id= df_rec_2.alloc_no[i]
                        L_rec_diff_id= df_rec_2.diff_id[i]
                        if L_alloc_level=='T':
                            #checking all skus are not active in all selected locations
                            L_params_err10= (I_alloc,L_rec_item,L_rec_alloc_id)
                            L_params_err10 = convert_numpy(L_params_err10)
                            mycursor.execute(Q_upd_err_10,L_params_err10)
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_10")
                            conn.commit()
                            mycursor.execute(Q_upd_err_11,L_params_err10)
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_11")
                            conn.commit()
                        else:
                            #checking all skus are not active in all selected locations
                            L_params_err12= (I_alloc,L_rec_item,L_rec_diff_id,L_rec_item,L_rec_diff_id,L_rec_diff_id,L_rec_alloc_id)
                            L_params_err12 = convert_numpy(L_params_err12)
                            mycursor.execute(Q_upd_err_12,L_params_err12 )
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_12")
                            conn.commit()
                            #checking all skus are not active in all wharehouse
                            mycursor.execute(Q_upd_err_13,L_params_err12)
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_13")
                            conn.commit()
            mycursor.execute(Q_upd_err_14,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_14")
            conn.commit()
            O_status = 21
            if L_alloc_doc_type =='F':
                L_enforce_wh_store_rel_ind = 'N'
            else:
                df_wh_str_ind = pd.read_sql(Q_get_wh_str_rel_ind, conn, params=(I_alloc,))
                L_enforce_wh_store_rel_ind = df_wh_str_ind.WH_STORE_REL_IND[0]
            O_status = 22
            if L_enforce_wh_store_rel_ind=='Y':
                mycursor.execute(Q_upd_err_15,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_15")
            else:
                mycursor.execute(Q_upd_err_16,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_16")
            conn.commit()
            O_status = 23
            if L_alloc_level=='T':
                mycursor.execute(Q_upd_err_17, (I_alloc,I_alloc))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_17")
                conn.commit()
                mycursor.execute(Q_upd_err_18, (I_alloc,I_alloc,I_alloc))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_18")
                conn.commit()
                mycursor.execute(Q_upd_err_19, (I_alloc, I_alloc))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_19")
                conn.commit ()
                mycursor.execute(Q_upd_err_20, (I_alloc,I_alloc,I_alloc))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_20")
                conn.commit()
                mycursor.execute(Q_upd_err_21, (I_alloc, I_alloc))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_21")
                conn.commit()
                mycursor.execute(Q_upd_err_22, (I_alloc,I_alloc,I_alloc))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_22")
                conn.commit()
                if L_alloc_doc_type=='W' and null_py(L_storm_wh_chk,0) ==1:
                    mycursor.execute(Q_upd_err_23, (I_alloc, I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_23")
                    conn.commit()
                    mycursor.execute(Q_upd_err_24, (I_alloc,I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_24")
                    conn.commit()
            else:
                mycursor.execute(Q_upd_err_25,(L_alloc_level,I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_25")
                conn.commit()
                mycursor.execute(Q_upd_err_26,(L_alloc_level,I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_26")
                conn.commit()
                mycursor.execute(Q_upd_err_27,(L_alloc_level,I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_27")
                conn.commit()
                if L_alloc_doc_type=='W' and null_py(L_storm_wh_chk,0) ==1:
                    mycursor.execute(Q_upd_err_28,(L_alloc_level,I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_28")
                    conn.commit()
            mycursor.execute(Q_upd_err_29,(I_alloc,L_release_date))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_29")
            conn.commit
            O_status = 24
            df_get_days = pd.read_sql(Q_get_days, conn)
            L_days= df_get_days.code_desc[0]
            
            L_params1=(L_days,I_alloc,L_days)
            L_params1= convert_numpy(L_params1)
            mycursor.execute(Q_upd_err_30,L_params1)
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_30")
            conn.commit()
            mycursor.execute(Q_upd_err_31,(I_alloc,I_alloc))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_err_31")
            conn.commit()
            O_status = 25
            df_check_error = pd.read_sql(Q_check_error, conn, params=(I_alloc,))
            if len(df_check_error) >0:
                L_error_found= df_check_error.error_found[0]
            else:
                L_error_found =0

            if L_error_found ==1:
                print("Errors encountered during Calculation Validation")
                conn.commit()
                conn.cursor().close()
                return False, L_func_name+":"+str(O_status)+": Errors encountered during Calculation Validation"

            else:
                print("Calculation Validation Successful without hard stop errors")
                conn.cursor().close()
                return True,""

    except Exception as error:
        err_return = ""
        if O_status<=5:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while processing fetch_inventory:  "+ str(error)
        elif O_status>=6 and O_status<=9:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while processing load_item: "+ str(error)
        elif O_status<=11 and O_status>=10:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while processing setup_item_location: "+ str(error)
        elif O_status>=12 and O_status<=14:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while processing setup_location: "+ str(error)
        elif O_status==24:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while processing update_alloc: "+ str(error)
        elif O_status>14 and O_status<24:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while executing validations: "+ str(error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured:  "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return
