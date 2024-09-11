
import pandas as pd
import yaml
from ..ALLOCATION_STATUS.remove_alloc import remove_alloc

def cancel(conn,O_status,I_alloc_no):  
    L_func_name = "cancel"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/remove_alloc.yaml') as fh:
            queries         = yaml.load(fh, Loader=yaml.SafeLoader)
            
            Q_sel_status    = queries['cancel']['Q_sel_status']
            Q_sel_alloc_id    = queries['cancel']['Q_sel_alloc_id']
            Q_upd_status    = queries['cancel']['Q_upd_status']
            

            #Q_sel_status ="SELECT status FROM alloc_head WHERE alloc_no = %s;"
            #Q_sel_alloc_id = "select alloc_no from alloc_head where alloc_no = %s;"
            #Q_upd_status = "UPDATE alloc_head SET status = CASE WHEN status = 'RSV' THEN 'WS'ELSE 'CNL'END WHERE alloc_no = %s;"

            mycursor=conn.cursor()
            O_status = 1
            df_get_allocno_status = pd.read_sql(Q_sel_status,conn,params=(I_alloc_no,))
            L_alloc_status=df_get_allocno_status["status"][0]

            if L_alloc_status in ('APV','RSV'):
                #should call UPDATE_PROGRAM_AUDIT_ENTRY
                O_status = 2
                df_get_allocno = pd.read_sql(Q_sel_alloc_id,conn,params=(I_alloc_no,))
                L_alloc_status=df_get_allocno["alloc_no"][0]

                O_status = 3
                L_fun,err = remove_alloc(conn,O_status,I_alloc_no)
                if L_fun == False:
                    conn.rollback()
                    return False,err

                O_status = 4
                mycursor.execute(Q_upd_status,(I_alloc_no,))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)

                #should call UPDATE_PROGRAM_AUDIT_ENTRY
            else:
                print(I_alloc_no,"can not be cancelled.")
                return False,L_func_name+": "+I_alloc_no+" can not be cancelled."
            
            conn.commit()
            return True,""

                
    except Exception as error:
        err_return = ""
        if O_status == 1:            
            err_return = L_func_name+":"+str(O_status)+":Exception occured while selecting the status from alloc_head table: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while selecting the status from alloc_head table: ", error)
        elif O_status == 2:
            err_return = L_func_name+":"+str(O_status)+":Exception occured while selecting the alloc_no from alloc_head table: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while selecting the alloc_no from alloc_head table: ", error)
        elif O_status == 3:
            err_return = L_func_name,":"+str(O_status)+":Exception occured while calling remove_alloc function "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while calling remove_alloc function ", error)
        elif O_status == 4:
            err_return = L_func_name,":"+str(O_status)+":Exception occured while updating status in alloc_head table: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while updating status in alloc_head table: ", error)
        else:
            err_return = L_func_name,":"+str(O_status)+":Exception occured: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured: ", error)
        print(err_return)
        conn.rollback()
        return False,err_return