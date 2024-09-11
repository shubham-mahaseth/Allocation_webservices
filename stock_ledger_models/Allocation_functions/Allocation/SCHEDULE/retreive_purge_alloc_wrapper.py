from .retreive_purge_alloc import retreive_purge_alloc


#----------------------------------------------------------
# Wrapper for function retreive_purge_alloc
#----------------------------------------------------------
def retreive_purge_alloc_wrapper( conn,alloc_id):

    L_func_name="retreive_purge_alloc_wrapper"
    O_status =list()
    try:
            L_func_call, err_msg = retreive_purge_alloc(conn
                                                ,alloc_id)
                                                
            return L_func_call, err_msg

    except Exception as argument:
        err_return = L_func_name+": Exception occured: "+ str(argument)
        print(err_return)
        return O_status, err_return





#if __name__ == "__main__":
#    alloc_id= "1001"
#    L_func_call = retreive_purge_alloc_wrapper(alloc_id)
#    print(L_func_call)




