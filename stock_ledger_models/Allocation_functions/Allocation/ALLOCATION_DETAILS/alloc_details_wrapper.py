from .setup_alloc_details import retreive_alloc_details

#----------------------------------------------------------
# Function to call other fuctions for testing
#----------------------------------------------------------
def RTV_ALLOC_DTL(conn,
                  I_alloc_no):
    L_func_name="RTV_ALLOC_DTL"
    O_status =list()
    try:        
        L_func_call, err_msg = retreive_alloc_details(conn,
                                             I_alloc_no)
        return L_func_call,err_msg
    except Exception as argument:        
        err_return = L_func_name+": Exception occured: "+ str(argument)
        conn.cursor().close()
        print(err_return)
        return [],err_return




#if __name__ == "__main__":
#    I_alloc = '1472'
#    L_func_call = RTV_ALLOC_DTL(I_alloc)    
#    print(L_func_call)
