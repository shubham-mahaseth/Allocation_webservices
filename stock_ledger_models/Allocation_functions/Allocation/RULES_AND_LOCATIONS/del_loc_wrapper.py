from ..GLOBAL_FILES.get_connection import get_mysql_conn
from .setup_rules_locations import DELETE_LOCATIONS

#----------------------------------------------------------
# Function to call other fuctions for testing
#----------------------------------------------------------
def DEL_LOCS(conn,I_alloc_no):
    L_func_name="DEL_LOCS"
    #O_status =list()
    emp = list()
    try:
        print("CONNECTION SUCCESS")
        I_get_mysql_conn = list()
        I_get_mysql_conn.append(0)
        #with get_mysql_conn (I_get_mysql_conn) as conn:
        L_func_call,err_msg = DELETE_LOCATIONS(conn,I_alloc_no)
        return L_func_call,err_msg
    except Exception as argument:
        err_return = L_func_name+": "+"Exception occured:"+ str(argument)
        print("Exception occured in: ",L_func_name,argument)
        #conn.rollback()
        return False,err_return




#if __name__ == "__main__":
#    I_alloc = 409
#    L_func_call = DEL_LOCS(I_alloc)    
#    print(L_func_call)
