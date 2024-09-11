
from ..GLOBAL_FILES.get_vdate import get_vdate
from ..ALLOCATION_STATUS.cancel import cancel
from ..ALLOCATION_STATUS.approve import approve
import pandas as pd
import yaml

def sel_approve(conn,O_status):
    O_status = 0
    L_func_name = "sel_approve"
    L_action    = "APPROVE"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/sel_approve_queries.yaml') as fh:
            queries             = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_create_tmp        = queries['sel_approve']['Q_create_tmp']
            Q_sel_rec           = queries['sel_approve']['Q_sel_rec']
            Q_process_rec       = queries['sel_approve']['Q_process_rec']
            Q_chck_wi           = queries['sel_approve']['Q_chck_wi']
            Q_ins_values        = queries['sel_approve']['Q_ins_values']
            Q_chck_data         = queries['sel_approve']['Q_chck_data']
            Q_upd_alloc_head    = queries['sel_approve']['Q_upd_alloc_head']
            Q_df_temp_data      = queries['sel_approve']['Q_df_temp_data']
            Q_fetch_alloc_type  = queries['sel_approve']['Q_fetch_alloc_type']
            Q_alloc_type        = queries['sel_approve']['Q_alloc_type']
            Q_del_temp          = queries['sel_approve']['Q_del_temp']


            O_status = 1
            mycursor=conn.cursor()
            mycursor.execute(Q_create_tmp)
            mycursor.execute(Q_del_temp)
            L_vdate = get_vdate(conn)

            O_status = 2
            df_sel_alloc_type = pd.read_sql(Q_fetch_alloc_type,conn)
            print(df_sel_alloc_type,df_sel_alloc_type["alloc_no"])
            L_alloc  = list(df_sel_alloc_type["alloc_no"])
            print(L_alloc)

            for i in L_alloc:
                O_status = 3
                L_alloc_i = str(int(i))
                df_sel_alloc_type = pd.read_sql(Q_alloc_type,conn,params=(L_alloc_i,))
                print("df_sel_alloc_type:",df_sel_alloc_type)
                L_alloc_type = df_sel_alloc_type["alloc_type"][0]
                if L_alloc_type == 'SCHD':
                    err_message = "Parent Allocation cannot be approved."
                    return False,err_message

            O_status = 4
            df_sel_alloc_no = pd.read_sql(Q_sel_rec,conn,)
            L_count     = df_sel_alloc_no.count()[0]
            L_max_alloc = df_sel_alloc_no.alloc_no[0]
            print("selected rec",L_max_alloc)


            if L_count == 0 or L_max_alloc == None:
                err_message = "No records to process."
                return False,err_message

            O_status = 5
            df_loop_rec = pd.read_sql(Q_process_rec,conn,)
            print("df frame",df_loop_rec)
            for i in range(len(df_loop_rec)):
                L_alloc_no    = int(df_loop_rec["alloc_no"][i])
                L_status_code = df_loop_rec["status_code"][i]

                df_get_wi = pd.read_sql(Q_chck_wi,conn,params=(L_alloc_no,))

                O_status = 6
                if len(df_get_wi) > 0:
                    err_message = "What if allocation cannot be approved."
                    return False, err_message

                O_status = 7
                if L_status_code == 'CNL':
                    err_message = "Cancelled Allocation cannot be approved."
                    mycursor.execute(Q_ins_values,(L_alloc_no,L_action,err_message,))
                elif  L_status_code in ('APV','EXT','COLS','CNL'):
                    err_message = "Allocation has already been approved."
                    mycursor.execute(Q_ins_values,(L_alloc_no,L_action,err_message,))

                #need to add VALIDATE_ALLOC_LOCKED function.
                else:
                    O_status = 8
                    if L_status_code == 'RSV':
                        df_chck_date = pd.read_sql(Q_chck_data,conn,params=(L_alloc_no))
                        L_date       = df_chck_date["release_date"][0]

                        O_status = 9
                        if L_date < L_vdate:
                            err_message = "Release date cannot be older than the current date."
                            mycursor.execute(Q_ins_values,(L_alloc_no,L_action,err_message,))
                        else:
                            O_status = 10
                            L_cancel,err = cancel(conn,O_status,L_alloc_no)
                            if L_cancel == False:
                                mycursor.execute(Q_ins_values,(L_alloc_no,L_action,err,))
                            else:
                                mycursor.execute(Q_upd_alloc_head,(L_alloc_no,))

                    L_apv_func,err = approve(conn,O_status,L_alloc_no)

                    O_status = 11
                    if L_apv_func == False:
                        mycursor.execute(Q_ins_values,(L_alloc_no,L_action,err,))
                    else:
                        message = "SUCCESSFUL"
                        mycursor.execute(Q_ins_values,(L_alloc_no,L_action,message,))

            df_return_data = pd.read_sql(Q_df_temp_data,conn,)

            conn.commit()
            return df_return_data,""

    except Exception as error:  
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while creating temp tables: "+ str(error)
        elif O_status==2:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while selecting alloc_no from alloc_summary temp table: "+ str(error)
        elif O_status==3:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured in the for loop while fetching alloc_type: "+ str(error)
        elif O_status==4:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured in the for loop while checking for alloc_no's: "+ str(error)
        elif O_status==5:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured in the for loop while fetching records to process: "+ str(error)
        elif O_status==6:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while checking the what if allocation: "+ str(error)
        elif O_status==7:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while checking the status is in cancel or not: "+ str(error)
        elif O_status==8:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while checking the allocation status: "+ str(error)
        elif O_status==9:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while comparing the release date and vdate: "+ str(error)
        elif O_status==10:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while calling the cancel function: "+ str(error)
        elif O_status==11:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while inserting the data into temp table: "+ str(error)
        else:
            err_return =L_func_name+":"+str(O_status)+":"+"Exception occured in : "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False,err_return




#if __name__ == "__main__":
#    #L_alloc_no=6734
#    O_status=None
#    daily_view,err = sel_approve(O_status)  
#    print(daily_view,err);

