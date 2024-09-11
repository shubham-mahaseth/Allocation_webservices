from ..ALLOCATION_DETAILS.restore_pck_alloc_qty import restore_pck_alloc_qty


def restore_pck_wrapper(conn,I_alloc_no,I_wh):
    L_func_name="alloc_pck_com_wrapper"
    O_status =list()
    try:
        mycursor=conn.cursor()
        mycursor.execute("SET sql_mode = ''; ")
        L_func, err_msg = restore_pck_alloc_qty(conn,O_status,I_alloc_no,I_wh)
        return L_func,err_msg
                
    except Exception as argument:
        err_return = L_func_name+": "+"Exception occured: "+ str(argument)
        print(err_return)
        return False, err_return


#if __name__ == "__main__":
#    I_alloc_no=1700
#    I_wh = 1
#    daily_view = restore_pck_wrapper(I_alloc_no,I_wh)  
#    print(daily_view);
