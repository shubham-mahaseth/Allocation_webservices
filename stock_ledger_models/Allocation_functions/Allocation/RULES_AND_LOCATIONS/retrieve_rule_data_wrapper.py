from ..RULES_AND_LOCATIONS.retrieve_rule_data import retrieve_rule

#----------------------------------------------------------
# Function to call other fuctions for testing
#----------------------------------------------------------
def rtv_rule(conn,I_alloc_no):
    L_func_name="test_func"
    O_status =list()
    try:
        L_func_call,err_msg = retrieve_rule(conn,
                                    I_alloc_no,
                                    O_status)
        return L_func_call,err_msg
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured :"+ str(argument)
        emp =list()
        return emp,err_return




#if __name__ == "__main__":

#    I_alloc = '1557'
#    L_func_call = rtv_rule(I_alloc)    
#    print(L_func_call)



