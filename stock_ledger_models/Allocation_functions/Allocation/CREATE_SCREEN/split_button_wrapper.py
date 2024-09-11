
from ..CREATE_SCREEN.process_split_data import split_data

def split_func(conn,I_alloc_no,I_create_id):
    L_func_name = "split_func"
    O_status =list()
    try:
        L_func_call,err_msg = split_data(conn,
                                    I_alloc_no,I_create_id,
                                    O_status)
        return L_func_call,err_msg
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured:"+ str(argument)
        return [],err_return

#split_func('12345678')
