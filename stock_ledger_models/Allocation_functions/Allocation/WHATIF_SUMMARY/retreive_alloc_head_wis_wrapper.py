from ..WHATIF_SUMMARY.retreive_alloc_head_wis import retreive_alloc_head


def retreive_alloc_head_wrapper(conn,I_alloc_no):
    L_func_name="retreive_alloc_head_wrapper"
    O_status =list()
    try:
        mycursor=conn.cursor()
        mycursor.execute("SET sql_mode = ''; ")
        L_function,err_msg = retreive_alloc_head(conn,O_status,I_alloc_no)
        return L_function,err_msg
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured:"+ str(argument)
        return False, err_return


#if __name__ == "__main__":
#    I_alloc_no=4945 #2060 #1951
#    daily_view = retreive_alloc_head_wrapper(I_alloc_no) 
#    print(daily_view);