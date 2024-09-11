from .setup_qty_limits import P360_INSERT_QTY_LIMITS

#----------------------------------------------------------
# Function to call other fuctions for testing
#----------------------------------------------------------
def ins_qty_limits(conn,
                   I_alloc_no):
    L_func_name="ins_qty_limits"
    O_status =list()
    try:
        L_func_call,err_msg = P360_INSERT_QTY_LIMITS(conn,
                                             I_alloc_no)
        return L_func_call,err_msg
    except Exception as argument:
        err_return = L_func_name+": "+"Exception occured:"+ str(argument)
        return False,err_return




#if __name__ == "__main__":
#    I_alloc_no = '1540'
#    conn = None
#    L_func_call = ins_qty_limits(conn,I_alloc_no)    
#    print(L_func_call)
