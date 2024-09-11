
import pandas as pd
import yaml
from ..CREATE_ALLOC_FUNCTIONS.complete_create import upd_resv_exp


def remove_alloc(conn,O_status,I_alloc_no):  
    L_func_name = "remove_alloc"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/remove_alloc.yaml') as fh:
            queries         = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_sel_alloc_no    = queries['remove_alloc']['Q_sel_alloc_no']
            Q_sel_records    = queries['remove_alloc']['Q_sel_records']
            Q_del_rcd_dtl    = queries['remove_alloc']['Q_del_rcd_dtl']
            Q_del_rcd_hdr    = queries['remove_alloc']['Q_del_rcd_hdr']
            Q_del_rcd_xref    = queries['remove_alloc']['Q_del_rcd_xref']

            
            mycursor=conn.cursor()
            O_status = 1
            df_get_allocno = pd.read_sql(Q_sel_alloc_no,conn,params=(I_alloc_no,))
            L_alloc_no     = df_get_allocno["xref_alloc_no"][0]

            O_status = 2
            if len(str(L_alloc_no)) == 0:
                return False,"Alloc_no not found"


            O_status = 3
            L_func,err = upd_resv_exp(conn,O_status,I_alloc_no,'D')
            if L_func == False:
                return False,err

            O_status = 4
            df_get_allocno = pd.read_sql(Q_sel_records,conn,params=(I_alloc_no,))
            
            O_status = 5
            mycursor.execute(Q_del_rcd_dtl,(I_alloc_no,))
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status = 6
            mycursor.execute(Q_del_rcd_hdr,(I_alloc_no,))
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status = 7
            mycursor.execute(Q_del_rcd_xref,(I_alloc_no,))
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

            conn.commit()
            return True,""


    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while selecting the alloc_no's from alloc_xref table: "+ error
            #print(L_func_name,":",O_status,":","Exception occured while selecting the alloc_no's from alloc_xref table: ", error)
        elif O_status == 2:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while checking the length for L_alloc_no dataframe: "+ error
            #print(L_func_name,":",O_status,":","Exception occured while checking the length for L_alloc_no dataframe: ", error)
        elif O_status == 3:
            err_return = L_func_name+":"+O_status+":"+"Exception occured before calling function upd_resv_exp: "+ error
            #print(L_func_name,":",O_status,":","Exception occured before calling function upd_resv_exp: ", error)
        elif O_status == 4:
            err_return = L_func_name+":"+O_status+":"+"Exception occured before calling function upd_resv_exp: "+ error
            #print(L_func_name,":",O_status,":","Exception occured before calling function upd_resv_exp: ", error)
        elif O_status == 5:
            err_return = L_func_name+":"+O_status+":"+"Exception occured while deleting records from alloc_sku_head table: "+ error
            #print(L_func_name,":",O_status,":","Exception occured while deleting records from alloc_sku_head table: ", error)
        elif O_status == 6:
            err_return = L_func_name+":"+O_status+":"+"Exception occured while deleting records from alloc_dtl table: "+ error
            #print(L_func_name,":",O_status,":","Exception occured while deleting records from alloc_dtl table: ", error)
        elif O_status == 7:
            err_return = L_func_name+":"+O_status+":"+"Exception occured while deleting records from alloc_xref table: "+ error
            #print(L_func_name,":",O_status,":","Exception occured while deleting records from alloc_xref table: ", error)
        else:
            err_return = L_func_name+":"+O_status+":"+"Exception occured: "+ error
            #print(L_func_name,":",O_status,":","Exception occured: ", error)
        print(err_return)
        conn.rollback()
        return False,err_return
