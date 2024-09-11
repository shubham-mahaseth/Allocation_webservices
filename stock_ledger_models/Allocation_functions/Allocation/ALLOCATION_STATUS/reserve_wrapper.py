from ..ALLOCATION_STATUS.reserve import reserve


def reserve_wrapper(conn,I_alloc_no):
    L_func_name="reserve_wrapper"
    O_status =list()
    try:
        mycursor=conn.cursor()
        mycursor.execute("SET sql_mode = ''; ")
        L_reserve_func,err_msg = reserve(conn,O_status,I_alloc_no)
        return L_reserve_func,err_msg

    except Exception as argument:
        err_return = L_func_name+": "+"Exception :"+ str(argument)
        print("Exception occured in: ",L_func_name,argument)
        return False,err_return


#if __name__ == "__main__":
#    I_alloc_no=2034
#    daily_view = reserve_wrapper(I_alloc_no) 
#    print(daily_view);



