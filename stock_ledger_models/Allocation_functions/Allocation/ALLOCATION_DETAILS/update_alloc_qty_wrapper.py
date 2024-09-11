from ..GLOBAL_FILES.get_connection import get_mysql_conn
from .setup_alloc_details import update_alloc_qty_dtl

#----------------------------------------------------------
# Function to call update_alloc_qty_dtl
#----------------------------------------------------------
def upd_sku_calc(conn,I_alloc_no):
    L_func_name="upd_sku_calc"
    try:
        L_func_call,err_msg = update_alloc_qty_dtl(conn,
                                            I_alloc_no)
        return L_func_call,err_msg
    except Exception as argument:
        err_return = L_func_name+": "+"Exception occured in "+ str(argument)        
        print(err_return)
        return False,err_return,err_return




#if __name__ == "__main__":
#    I_alloc = '1918351'
#    L_func_call = upd_sku_calc(I_alloc)    
#    print(L_func_call)
