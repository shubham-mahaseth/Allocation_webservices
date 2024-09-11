from .setup_rules_locations import UPDATE_SIZE_PROFILE_IND

#----------------------------------------------------------
# Function to call other fuctions for testing
#----------------------------------------------------------
def SIZE_PROF(conn,I_alloc_no,I_size_prof_ind):
    L_func_name = "SIZE_PROF"
    try:
        L_func_call,err_msg = UPDATE_SIZE_PROFILE_IND(conn,I_alloc_no,I_size_prof_ind)
        return L_func_call,err_msg
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured:"+ str(argument)
        conn.rollback()
        return False,err_return




#if __name__ == "__main__":
#    I_alloc = '12345678'
#    I_size_prof_ind = 'N'
#    L_func_call = SIZE_PROF(I_alloc,I_size_prof_ind)    
#    print(L_func_call)




