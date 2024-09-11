
from .setup_rules_locations import RETREIVE_LOCATIONS

#----------------------------------------------------------
# Function to call other fuctions for testing
#----------------------------------------------------------
def RTV_LOCS(conn,I_alloc_no):
    L_func_name="RTV_LOCS"
    #O_status =list()
    emp = list()
    try:
        print("CONNECTION SUCCESS")
        L_func_call, err_msg = RETREIVE_LOCATIONS(conn,I_alloc_no)
        #print(L_func_call)
        return L_func_call,err_msg
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception Occured :"+ str(argument)
        #conn.rollback()
        return emp,err_return




#if __name__ == "__main__":
#    I_alloc = '1557'
#    L_func_call = RTV_LOCS(I_alloc)    
#    print(L_func_call)


