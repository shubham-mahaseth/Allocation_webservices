
from ..CREATE_SCREEN.process_split_data import load_split

def to_call_ls_fun(conn,I_alloc_no):
    L_func_name = "to_call_ls_fun"
    O_status =list()
    try:
        L_func_call,err_msg = load_split(conn,I_alloc_no,O_status)
        return L_func_call,err_msg
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception :"+ str(argument)
        conn.rollback()
        return False, err_return

#to_call_ls_fun('12345678')




