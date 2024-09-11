from .alloc_size_header import retreive_alloc_size_header

def to_call_ls_fun(conn,I_alloc_no,
                   I_wh_id,
                   I_order_no,
                   I_source_item,
                   I_diff_id):
    L_func_name = "to_call_ls_fun"
    O_status =list()
    try:
        L_func_call = retreive_alloc_size_header(conn,I_alloc_no,I_wh_id,I_order_no,I_source_item,I_diff_id,O_status)
        #print("retreive_alloc_size_header: ",L_func_call)
        return L_func_call
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": Exception occured in: "+ str(argument)
        conn.rollback()
        return [],err_return

#to_call_ls_fun('5')




