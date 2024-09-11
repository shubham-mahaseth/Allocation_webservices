#from GLOBAL_FILES.get_connection import get_mysql_conn
import pandas as pd
import yaml

def alloc_pack_comp_dtl(conn,O_status,I_alloc_no,I_pack_no):  
    L_func_name = "alloc_pack_comp_dtl"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/alloc_pack_comp_queries.yaml') as fh:
            queries           = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_crete_tbl       = queries['alloc_pack_comp_dtl']['Q_crete_tbl']
            Q_del_gtt         = queries['alloc_pack_comp_dtl']['Q_del_gtt']
            Q_ins_pck_temp    = queries['alloc_pack_comp_dtl']['Q_ins_pck_temp']
            Q_sel_rec    = queries['alloc_pack_comp_dtl']['Q_sel_rec']
            #Q_sel_rec = "SELECT * FROM alloc_pack_comp_dtl_temp "
            mycursor=conn.cursor()
            O_status = 1
            mycursor.execute(Q_crete_tbl)

            O_status = 2
            mycursor.execute(Q_del_gtt,(I_alloc_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            mycursor.execute(Q_ins_pck_temp,(I_alloc_no,I_pack_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                
            df_top_pack_dtl = pd.read_sql(Q_sel_rec,conn,params=(I_alloc_no,))

            conn.cursor().close()
            return df_top_pack_dtl, ""
                

    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while creating temp table:  "+ str(error)
        elif O_status == 2:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting data into alloc_pack_comp_dtl_temp table : "+ str(error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception Occured: "+ str(error)
        print(err_return)
        conn.rollback()
        return [], err_return



# aso_alc_sku_details_gtt table getting created & inserted in aso_alc_sku_details function
#if __name__ == "__main__":
#    I_alloc_no=2351
#    I_pack_no = '200094733'
#    conn=None
#    O_status=None
#    daily_view = alloc_pack_comp_dtl(conn,O_status,I_alloc_no,I_pack_no)  
#    print(daily_view); 






