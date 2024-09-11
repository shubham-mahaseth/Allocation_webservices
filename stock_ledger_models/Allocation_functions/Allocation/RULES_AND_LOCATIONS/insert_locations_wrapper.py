from .setup_rules_locations import INSERT_LOCATIONS

#----------------------------------------------------------
# Function to call other fuctions for testing
#----------------------------------------------------------
def INS_LOCS(conn,I_alloc_no):
    L_func_name="INS_LOCS"
    O_status =list()
    try:
        L_func_call,err_msg = INSERT_LOCATIONS(conn,I_alloc_no)
        return L_func_call,err_msg
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured:"+ str(argument)
        conn.rollback()
        return False,err_return




#if __name__ == "__main__":
#    I_alloc = 409
#    L_func_call = INS_LOCS(I_alloc)    
#    print(L_func_call)




