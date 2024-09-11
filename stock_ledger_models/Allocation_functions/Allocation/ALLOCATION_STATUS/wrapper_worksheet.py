
from ..ALLOCATION_STATUS.worksheet import worksheet
from ..ALLOCATION_STATUS.cancel import cancel
from ..INVENTORY_SETUP.inventory_setup import update_inv


def worksheet_wrapper(conn,I_alloc_no):
    L_func_name="worksheet_wrapper"
    O_status =list()
    try:
        mycursor=conn.cursor()
        mycursor.execute("SET sql_mode = ''; ")
        L_cancel,err_message = cancel(conn,O_status,I_alloc_no)
        print("cancel function",L_cancel)
        if L_cancel == True:
            L_worksheet_func,err_msg = worksheet(conn,O_status,I_alloc_no)
            if L_worksheet_func == True:
                mycursor.execute("SET sql_mode = ''; ")
                Lupd_iv,err_msg1 = update_inv(conn,I_alloc_no,O_status)
                if Lupd_iv == True: 
                    return True,""
                else:
                    return False,err_msg1
            else:
                return False, err_msg
        else:
            return False,err_message
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        return False,L_func_name+": Exception occured: "+str(argument)


#if __name__ == "__main__":
#    I_alloc_no=2351
#    daily_view = worksheet_wrapper(I_alloc_no) 
#    print(daily_view);

