import mysql.connector
import pandas as pd
import yaml


def worksheet(conn,O_status,I_alloc_no):  
    L_func_name = "worksheet"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/worksheet_queries.yaml') as fh:
            queries         = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_sel_status    = queries['worksheet']['Q_sel_status']
            Q_upd_ind       = queries['worksheet']['Q_upd_ind']
            
            mycursor = conn.cursor()
            O_status = 1
            df_get_allocno_status = pd.read_sql(Q_sel_status,conn,params=(I_alloc_no,))
            L_alloc_status = df_get_allocno_status["status"][0]
            
            print("after selecting status",L_alloc_status)

            O_status = 2
            if L_alloc_status in ('APV','CNL','RSV','WS'): #kept 'ws' because in cancel function 'rsv' is updating to 'ws'
                print("L_alloc_status:::::::::",L_alloc_status )
                mycursor.execute(Q_upd_ind,(I_alloc_no,))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)
                if mycursor.rowcount > 0:
                    print("mycursor.rowcount::::::::",mycursor.rowcount)
                    conn.commit()
                    conn.cursor().close()
                    return True,""
                else:
                    #print("Allocation is not in approved,cancel or reserve status.")
                    err_message = "Allocation is not in approved,cancel or reserve status."
                    conn.cursor().close()
                    return False,err_message
            else:
                #print("alloc status is incorrect.")
                err_message = "alloc status is incorrect."
                conn.cursor().close()
                return False,err_message
        

    except Exception as error:
        err_return = ""
        if O_status == 1:
            print(L_func_name,":",O_status,":","Exception occured while selecting the status from alloc_head table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while selecting the status from alloc_head table :"+ str(error)
        elif O_status == 2:
            print(L_func_name,":",O_status,":","Exception occured while updating status,recalc_ind in alloc_head table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while updating status,recalc_ind in alloc_head table :"+ str(error)
        else:
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(error)
            print(L_func_name,":",O_status,":","Exception occured: ", error)
        conn.rollback()
        conn.cursor().close()
        return False,err_return




#if __name__ == "__main__":
#    I_alloc_no=103
#    O_status=None
#    conn=None
#    daily_view = worksheet(conn,O_status,I_alloc_no)  
#    print(daily_view);





