
from ..ALLOCATION_SUMMARY.sel_approve import sel_approve


def sel_approve_wrapper(conn):
    L_func_name="sel_approve_wrapper"
    O_status =list()
    try:
        mycursor=conn.cursor()
        mycursor.execute("SET sql_mode = ''; ")
        L_sel_apv,err_message = sel_approve(conn,O_status)
        return L_sel_apv,err_message
        #print("cancel function",err_message)

    except Exception as argument:
        err_return = L_func_name+"- Exception occured in: "+ str(argument)
        #print("Exception occured in: ",L_func_name,argument)
        return False,err_return


#if __name__ == "__main__":
#    O_status = None
#    daily_view,err = sel_approve_wrapper(O_status) 
#    print(daily_view,err);
