
from .copy_alloc import copy_alloc_data

def to_call_ca_fun(conn,
                   I_copy_alloc_no,
                   I_create_id
                   ):
    L_func_name = "to_call_ca_fun"
    O_status =list()
    try:
        L_func_call,err_msg = copy_alloc_data(conn,
                                        I_copy_alloc_no,
                                        I_create_id,
                                        O_status)
        return L_func_call,err_msg
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+"- Exception occured in: "+ str(argument)
        conn.rollback()
        return False,err_return

#to_call_ca_fun(2074) 




