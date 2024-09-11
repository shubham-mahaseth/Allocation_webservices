from .setup_rules_locations import UPDATE_STORE_WH_REL

#----------------------------------------------------------
# Function to call other fuctions for testing
#----------------------------------------------------------
def STORE_WH(conn,I_alloc_no,I_store_wh_rel_ind):
    L_func_name="STORE_WH"
    #O_status =list()
    #emp = list()
    try:
        L_func_call,err_msg = UPDATE_STORE_WH_REL(conn,I_alloc_no,I_store_wh_rel_ind)
        #if L_func_call == False:
        #    return False
        #print(L_func_call)
        return L_func_call,err_msg
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured:"+ str(argument)
        conn.rollback()
        return False, err_return




#if __name__ == "__main__":
#    I_alloc = '111222'
#    I_store_wh_rel_ind = 'N'
#    L_func_call = STORE_WH(I_alloc,I_store_wh_rel_ind)    
#    print(L_func_call)





