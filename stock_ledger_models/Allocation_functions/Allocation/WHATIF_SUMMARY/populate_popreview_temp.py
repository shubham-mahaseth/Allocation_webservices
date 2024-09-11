import pandas as pd
import yaml

def populate_popreview_temp(conn,O_status,I_alloc_no):
    L_func_name = "populate_popreview_temp"
    O_status = 0
    try:
        L_func_name = "populate_popreview_temp"
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/populate_popreview_gtt_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_upd_wi_summary          = queries['populate_popreview_temp']['Q_upd_wi_summary']
            Q_upd_wi_summary_hdr_gtt  = queries['populate_popreview_temp']['Q_upd_wi_summary_hdr_gtt']

            O_status = 1
            mycursor = conn.cursor()

            mycursor.execute(Q_upd_wi_summary,(I_alloc_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            
            O_status = 2
            mycursor.execute(Q_upd_wi_summary_hdr_gtt,(I_alloc_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            conn.commit()
            conn.cursor().close()
            return True, ""

    except Exception as error:
        err_return = ""
        if O_status == 1:
            print(L_func_name,":",O_status,":","Exception occured while updating records in alloc_whatif_summary table ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while updating records in alloc_whatif_summary table :"+ str(error)
        elif O_status == 2:
            print(L_func_name,":",O_status,":","Exception occured while updating records in aso_alloc_wisummary_hdr_gtt table  ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while updating records in aso_alloc_wisummary_hdr_gtt table :"+ str(error)
        else:
            print("Exception occured in: ",L_func_name,error)
            err_return = L_func_name+": "+"Exception occured:"+ str(error)
        conn.rollback()
        return False, err_return


