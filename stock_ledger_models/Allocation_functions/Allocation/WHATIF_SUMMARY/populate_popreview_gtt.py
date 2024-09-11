import pandas as pd
import yaml

def populate_popreview_gtt(conn,O_status,I_alloc_no):
    L_func_name = "populate_popreview_gtt"
    O_status = 0
    try:
        L_func_name = "populate_popreview_gtt"
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/populate_popreview_gtt_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            print(0)
            Q_create_wi_summary_tbl  = queries['populate_popreview_gtt']['Q_create_wi_summary_tbl']
            Q_del_whatif_temp        = queries['populate_popreview_gtt']['Q_del_whatif_temp']
            Q_ins_temp               = queries['populate_popreview_gtt']['Q_ins_temp']
            Q_sel_rec                = queries['populate_popreview_gtt']['Q_sel_rec']
            Q_upd_records            = queries['populate_popreview_gtt']['Q_upd_records']
            Q_fetch_gtt              = queries['populate_popreview_gtt']['Q_fetch_gtt']

            print(1)
            O_status = 1
            L_count = 1

            mycursor = conn.cursor()
            mycursor.execute(Q_create_wi_summary_tbl)

            mycursor.execute(Q_del_whatif_temp)
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            
            O_status = 2
            mycursor.execute(Q_ins_temp,(I_alloc_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status = 3
            df_sel_rec_gtt = pd.read_sql(Q_sel_rec,conn)

            if len(df_sel_rec_gtt) > 0:
                for index, row in df_sel_rec_gtt.iterrows():
                    print("df_sel_rec_gtt::::",df_sel_rec_gtt)
                    L_supplier      = f"{row['supplier']}"
                    L_release_date =  f"{row['release_date']}"
                    L_orig_country  = f"{row['orig_country']}"
                    L_loc          =  f"{row['loc']}"
                    L_eff_loc      =  f"{row['eff_loc']}"

                    O_status = 4
                    mycursor.execute(Q_upd_records,(L_count,L_supplier,L_release_date,L_orig_country,L_eff_loc,L_loc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                    if mycursor.rowcount > 0:
                        L_count = L_count + 1
                        print("l_count")

            df_fetch_wi_po  =  pd.read_sql(Q_fetch_gtt,conn,params=(I_alloc_no,))

            conn.commit()
            conn.cursor().close()
            return df_fetch_wi_po,""

    except Exception as error:
        err_return = ""
        if O_status == 1:
            print(L_func_name,":",O_status,":","Exception occured while deleting records from alloc_whatif_summary_temp table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting records from alloc_whatif_summary_temp table: "+ str(error)
        elif O_status == 2:
            print(L_func_name,":",O_status,":","Exception occured while inserting reocrds into alloc_whatif_summary_temp table : ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting reocrds into alloc_whatif_summary_temp table: "+ str(error)
        elif O_status == 3:
            print(L_func_name,":",O_status,":","Exception occured while selecting records alloc_whatif_summary_temp table ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while selecting records alloc_whatif_summary_temp table: "+ str(error)
        elif O_status==4 :
            print(L_func_name,":",O_status,":","Exception occured while updating data from alloc_whatif_summary_temp  table ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while updating data from alloc_whatif_summary_temp table: "+ str(error)
        else:
            print("Exception occured in: ",L_func_name,error)
            err_return = L_func_name+": "+"Exception occured: "+ str(error)
        conn.rollback()
        return [],err_return

