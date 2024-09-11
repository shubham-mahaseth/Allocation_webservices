from ..GLOBAL_FILES.get_connection import get_mysql_conn
from .setup_alloc_details import spread_alloc_loc_dtl

#----------------------------------------------------------
# Function to call spread_alloc_loc_dtl
#----------------------------------------------------------
def sprd_alloc_loc(conn,I_alloc_no):
    L_func_name="sprd_alloc_loc"
    O_status =list()
    try:
        L_func_call,err_msg = spread_alloc_loc_dtl(conn,
                                            I_alloc_no,
                                            O_status)
        return L_func_call,err_msg
    except Exception as argument:
        err_return = L_func_name+": Exception occured in "+ str(argument)
        print(err_return)
        return False,err_return




if __name__ == "__main__":
    I_alloc = '12345678'
    L_func_call = sprd_alloc_loc(I_alloc)    
    print(L_func_call)