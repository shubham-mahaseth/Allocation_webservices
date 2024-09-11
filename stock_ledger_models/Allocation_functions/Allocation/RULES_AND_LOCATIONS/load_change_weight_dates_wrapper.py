from ..RULES_AND_LOCATIONS.change_weight import load_change_weight_dates


#----------------------------------------------------------
# Function to call other fuctions fro testing
#----------------------------------------------------------
def fetch_chng_wt(conn,I_alloc):
    L_func_name="fetch_chng_wt"
    O_status =list()
    try:
        L_func_call,err_msg = load_change_weight_dates(conn,I_alloc,O_status)
        return L_func_call,err_msg
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured :"+ str(argument)
        return False,err_return

#if __name__ == "__main__":
#    I_alloc = '1741'
#    L_func_call = fetch_chng_wt(I_alloc)    
#    print(L_func_call)