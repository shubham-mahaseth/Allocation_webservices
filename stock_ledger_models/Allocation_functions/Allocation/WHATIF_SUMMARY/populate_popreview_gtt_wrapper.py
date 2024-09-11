from ..WHATIF_SUMMARY.populate_popreview_gtt import populate_popreview_gtt


def populate_popreview_wrapper(conn, I_alloc_no):
    L_func_name="populate_popreview_wrapper"
    O_status =list()
    try:
        mycursor=conn.cursor()
        mycursor.execute("SET sql_mode = ''; ")
        L_function,err_msg = populate_popreview_gtt(conn,O_status,I_alloc_no)
        return L_function,err_msg

    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured: "+ str(argument)
        return [],err_return


#if __name__ == "__main__":
#    I_alloc_no=4945 #2060
#    daily_view = populate_popreview_wrapper(I_alloc_no) 
#    print(daily_view);