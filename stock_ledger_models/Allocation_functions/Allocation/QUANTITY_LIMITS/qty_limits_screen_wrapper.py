from .setup_qty_limits import P360_RETREIVE_QUANTITY_LIMITS
#from GLOBAL_FILES.get_connection import get_mysql_conn

#----------------------------------------------------------
# Function to call other fuctions for testing
#----------------------------------------------------------
def rtv_qty_limits(conn,I_alloc_no,I_mode):
    L_func_name="rtv_qty_limits"
    O_status =list()
    try:
        L_func_call,err_msg = P360_RETREIVE_QUANTITY_LIMITS(conn,I_alloc_no,I_mode)
        return L_func_call,err_msg
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured:"+ str(argument)
        return [],err_return




#if __name__ == "__main__":
#    I_alloc = 1540
#    mode = 'NEW'
#    L_func_call = rtv_qty_limits(I_alloc,mode)    
#    print(L_func_call)
