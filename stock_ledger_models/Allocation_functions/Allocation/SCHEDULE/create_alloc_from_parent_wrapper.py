from ..SCHEDULE.create_alloc_from_parent import create_alloc_from_parent


#----------------------------------------------------------
# Wrapper for function aso_update_alloc_type
#----------------------------------------------------------
def create_alloc_from_parent_wrapper(conn,I_thread_value):

    L_func_name="create_alloc_from_parent_wrapper"
    O_status =list()
    try:
        L_func_call, err_msg = create_alloc_from_parent(conn
                                            ,I_thread_value)
                                                
        return L_func_call, err_msg

    except Exception as argument:
        err_return = L_func_name+": Exception occured: "+ str(argument)
        print(err_return)
        return False, err_return





#if __name__ == "__main__":
#    alloc_id= "2729"
#    I_thread_value="2"
#    L_func_call = create_alloc_from_parent_wrapper(alloc_id,I_thread_value)
#    print(L_func_call)



