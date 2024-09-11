from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
from ..GLOBAL_FILES.null_handler import null_py
from datetime import datetime,date, timedelta
import pandas as pd
import numpy as np
import yaml



#############################################################
# Created By - Himanshu Maheshwari                          #                
# File Name - pack_priority_need.py                         #
# Purpose - Pack prioritization                             #
#############################################################

def pack_priority_need(conn
              ,I_alloc
              ,O_status):
    L_func_name ="pack_priority_need"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        print("inside try")
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/pack_priority_need_queries.yaml') as fh:
            queries                                         = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_cre_st_wh_avail_qty                           = queries['pack_priority_need']['Q_cre_st_wh_avail_qty']
            Q_doc_type                                      = queries['pack_priority_need']['Q_doc_type']
            Q_mer_1                                         = queries['pack_priority_need']['Q_mer_1']
            Q_mer_2                                         = queries['pack_priority_need']['Q_mer_2']
            Q_wh_str_rel_ind                                = queries['pack_priority_need']['Q_wh_str_rel_ind']
            Q_del_st_wh_avail_qty                           = queries['pack_priority_need']['Q_del_st_wh_avail_qty']
            Q_ins_st_wh_avail_qty                           = queries['pack_priority_need']['Q_ins_st_wh_avail_qty']
            Q_rec_priority                                  = queries['pack_priority_need']['Q_rec_priority']
            Q_del_1                                         = queries['pack_priority_need']['Q_del_1']
            Q_rec_get_loc                                   = queries['pack_priority_need']['Q_rec_get_loc']
            Q_ins_df_get_loc1                               = queries['pack_priority_need']['Q_ins_df_get_loc1']
            Q_get_rec_alloc_qty                             = queries['pack_priority_need']['Q_get_rec_alloc_qty']
            Q_get_wh_avail_qty                              = queries['pack_priority_need']['Q_get_wh_avail_qty']
            Q_pack_allocated                                = queries['pack_priority_need']['Q_pack_allocated']
            Q_mer_3                                         = queries['pack_priority_need']['Q_mer_3']
            Q_get_som_qty                                   = queries['pack_priority_need']['Q_get_som_qty']
            Q_get_loop_rec                                  = queries['pack_priority_need']['Q_get_loop_rec']
            Q_upd_pck_alloc_temp                            = queries['pack_priority_need']['Q_upd_pck_alloc_temp']
            Q_mer_4                                         = queries['pack_priority_need']['Q_mer_4']
            Q_mer_5                                         = queries['pack_priority_need']['Q_mer_5']
            Q_mer_6                                         = queries['pack_priority_need']['Q_mer_6']
            Q_mer_7                                         = queries['pack_priority_need']['Q_mer_7']
            Q_mer_8                                         = queries['pack_priority_need']['Q_mer_8']
            Q_mer_9                                         = queries['pack_priority_need']['Q_mer_9']

            O_status = 1
            mycursor = conn.cursor()
            
            mycursor.execute(Q_cre_st_wh_avail_qty)
            O_status = 2
            df_alloc_critera = pd.read_sql(Q_doc_type, conn, params=(I_alloc,))
            L_doc_type= df_alloc_critera.alloc_criteria[0]
            O_status = 3
            mycursor.execute(Q_mer_1, (I_alloc,I_alloc))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_mer_1")
            O_status = 4
            mycursor.execute(Q_mer_2, (I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_mer_2")

            conn.commit()
            O_status = 5
            if L_doc_type =='F':
                L_enforce_wh_store_rel_ind = 'N'
            else:
                O_status = 6
                df_wh_str_rel_ind= pd.read_sql(Q_wh_str_rel_ind, conn, params=(I_alloc,))
                if len(df_wh_str_rel_ind)>0:
                    L_enforce_wh_store_rel_ind =df_wh_str_rel_ind.wh_store_rel_ind[0] 
            O_status = 7
            mycursor.execute(Q_del_st_wh_avail_qty, (I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_del_st_wh_avail_qty")
            O_status = 8
            mycursor.execute(Q_ins_st_wh_avail_qty, (L_doc_type,I_alloc,L_enforce_wh_store_rel_ind,L_enforce_wh_store_rel_ind))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_ins_st_wh_avail_qty")
            O_status = 9
            df_rec_priority = pd.read_sql(Q_rec_priority, conn, params=(I_alloc,I_alloc))

            if len(df_rec_priority) >0:
                O_status = 10
                for i in range(len(df_rec_priority)):
                    mycursor.execute(Q_del_1,(I_alloc,))
                    L_rec1_tran_item = df_rec_priority.tran_item[i]
                    O_status = 11
                    df_get_loc = pd.read_sql(Q_rec_get_loc, conn, params=(I_alloc,L_rec1_tran_item))
                    if len(df_get_loc) >0:
                        for i in range(len(df_get_loc)):
                            O_status = 12
                            L_tran_item= df_get_loc.tran_item[i]
                            L_to_loc = df_get_loc.to_loc[i]
                            O_status = 13
                            mycursor.execute(Q_ins_df_get_loc1,(I_alloc,L_to_loc,L_tran_item,L_tran_item,I_alloc))
                            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_ins_df_get_loc1")
                            conn.commit()
                    L_assign_default_wh = None
                    df_rec_alloc_qty = pd.read_sql(Q_get_rec_alloc_qty, conn, params=(L_enforce_wh_store_rel_ind,I_alloc))
                    if len(df_rec_alloc_qty) >0:
                        O_status = 14
                        for i in range(len(df_rec_alloc_qty)):
                            L_A_pack_no = df_rec_alloc_qty.pack_no[i]
                            O_status = 15
                            if L_enforce_wh_store_rel_ind=='N':
                                L_A_to_loc = df_rec_alloc_qty.wh_loc[i]
                            else:
                                O_status = 16
                                L_assign_default_wh = df_rec_alloc_qty.wh_loc[i]
                                print("L_assign_default_wh:" ,L_assign_default_wh)
                            O_status = 17
                            df_get_wh_avail_qty = pd.read_sql(Q_get_wh_avail_qty, conn, params=(L_A_pack_no,L_enforce_wh_store_rel_ind,L_assign_default_wh,L_enforce_wh_store_rel_ind))
                            L_wh_id = df_get_wh_avail_qty.wh_id[0]
                            L_avail_qty =df_get_wh_avail_qty.avail_qty[0]
                            O_status = 18
                            df_pack_allocated = pd.read_sql(Q_pack_allocated, conn, params=(I_alloc,L_enforce_wh_store_rel_ind,L_assign_default_wh,L_enforce_wh_store_rel_ind))
                            L_total_pack_allocated = df_pack_allocated.total_pack_allocated[0]
                            O_status = 19
                            L_avail_qty=null_py(L_avail_qty,0)
                            L_total_pack_allocated =null_py(L_total_pack_allocated,0)
                            if L_avail_qty< L_total_pack_allocated :
                                mycursor.execute(Q_mer_3, (L_avail_qty,L_total_pack_allocated,I_alloc,L_enforce_wh_store_rel_ind,L_assign_default_wh,L_enforce_wh_store_rel_ind))
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_mer_3")
                                conn.commit()
                            O_status = 20
                            df_pack_allocated = pd.read_sql(Q_pack_allocated, conn, params=(I_alloc,L_enforce_wh_store_rel_ind,L_assign_default_wh,L_enforce_wh_store_rel_ind))
                            L_total_pack_allocated = df_pack_allocated.total_pack_allocated[0]
                            O_status = 21
                            df_get_som_qty = pd.read_sql(Q_get_som_qty, conn, params=(I_alloc,))
                            L_som_qty= df_get_som_qty.som_qty[0]
                            O_status = 22
                            if L_avail_qty < L_total_pack_allocated :
                                L_adj_units= max((L_total_pack_allocated - L_avail_qty),0)
                                L_adj_qty =1
                                O_status = 23
                                df_get_loop_rec = pd.read_sql(Q_get_loop_rec, conn, params=(L_adj_units,I_alloc))
                                if len(df_get_loop_rec) >0:
                                    for i in range(len(df_get_loop_rec)):
                                        O_status = 24
                                        if L_adj_units ==0:
                                            break
                                        O_status = 25
                                        L_adj_som_qty = round(L_adj_qty/L_som_qty,0)*L_som_qty
                                        I_pack_no= df_get_loop_rec.pack_no[i]
                                        I_to_loc = df_get_loop_rec.to_loc[i]
                                        O_status = 26
                                        mycursor.execute(Q_upd_pck_alloc_temp,(L_adj_som_qty,I_alloc,I_pack_no,I_to_loc))
                                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_upd_pck_alloc_temp")
                                        conn.commit()
                                        O_status = 27
                                        L_adj_units = max((L_adj_units - L_adj_som_qty),0)
                                        L_total_pack_allocated =0
                    O_status = 28                   
                    mycursor.execute(Q_mer_4,(I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_mer_4")
                    O_status = 29
                    mycursor.execute(Q_mer_5, (I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_mer_5")
                    O_status = 30
                    mycursor.execute(Q_mer_6, (I_alloc,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_mer_6")

                    conn.commit()
            O_status = 31
            mycursor.execute(Q_mer_7,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_mer_7")
            O_status = 32
            mycursor.execute(Q_mer_8,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_mer_8")
            O_status = 33
            mycursor.execute(Q_mer_9,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_mer_9")

            conn.commit()
            conn.cursor().close()
            return True,""

    except Exception as error:
        err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(error)

        #if O_status==1:
        #    print(L_func_name,":",O_status,":","Exception occured while executing ", L_func_name," function: ", error)
        #elif O_status==2:
        #    print(L_func_name,":",O_status,":","Exception occured while executing ", L_func_name," function: ", error)
        #elif O_status>=3 and O_status<=7:
        #    print(L_func_name,":",O_status,":","Exception occured while executing ", L_func_name," function: ", error)
        #elif O_status>=8 and O_status<=9:
        #    print(L_func_name,":",O_status,":","Exception occured while executing ", L_func_name," function: ", error)
        #elif O_status<=10 and O_status>=24:
        #    print(L_func_name,":",O_status,":","Exception occured while executing ", L_func_name," function: ", error)
        #elif O_status==25:
        #    print(L_func_name,":",O_status,":","Exception occured while executing ", L_func_name," function: ", error)
        #else:
        #    print("Exception occured in: ",L_func_name,O_status,error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False,err_return