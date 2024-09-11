import pandas as pd
import yaml


def alloc_pack_store_dtl(conn,O_status,I_alloc_no,I_pack_no,I_wh):  
    L_func_name = "alloc_pack_store_dtl"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/alloc_pack_comp_queries.yaml') as fh:
            queries         = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_crete_temp    = queries['alloc_pack_store_dtl']['Q_crete_temp']
            Q_sel_rec       = queries['alloc_pack_store_dtl']['Q_sel_rec']
            Q_del_gtt       = queries['alloc_pack_store_dtl']['Q_del_gtt']
            Q_ins_gtt       = queries['alloc_pack_store_dtl']['Q_ins_gtt']
            Q_sel_rec_gtt       = queries['alloc_pack_store_dtl']['Q_sel_rec_gtt']

            


            mycursor=conn.cursor()
            O_status = 1
            mycursor.execute(Q_crete_temp)

            O_status = 2
            L_doc_type = None
            df_doc_type = pd.read_sql(Q_sel_rec,conn,params=(I_alloc_no,))
            if len(df_doc_type) >0:
                L_doc_type = int(df_doc_type.alloc_criteria[0])

            O_status = 3
            mycursor.execute(Q_del_gtt,(I_alloc_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status = 4
            mycursor.execute(Q_ins_gtt,(I_alloc_no,I_pack_no,L_doc_type,I_wh,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            df_middle_pack_dtl = pd.read_sql(Q_sel_rec_gtt,conn,params=(I_alloc_no,))
            conn.cursor().close()
            return df_middle_pack_dtl,""


    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while creating temp table:  "+ str(error)
        elif O_status == 2:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while selecting doc_type from alloc_itm_search_dtl table: "+ str(error)
        elif O_status == 3:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while deleting records from alloc_sku_dtl_temp table:  "+ str(error)
        elif O_status == 4:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting data into alloc_sku_dtl_temp table :  "+ str(error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured:  "+ str(error)
        print(err_return)
        conn.rollback()
        return [],err_return


#if __name__ == "__main__":
#    I_alloc_no=1700
#    I_pack_no = '200085229'
#    I_wh = 1
#    conn=None
#    O_status=None
#    daily_view = alloc_pack_store_dtl(conn,O_status,I_alloc_no,I_pack_no,I_wh)  
#    print(daily_view);