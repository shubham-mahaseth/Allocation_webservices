from ..SCHEDULE.setup_schedule_alloc import delete_nonavail_from_parent


#----------------------------------------------------------
# Wrapper for function delete_nonavail_from_parent
#----------------------------------------------------------
def delete_nonavail_from_parent_wrapper(conn,
                                        alloc_id):

    L_func_name="delete_nonavail_from_parent_wrapper"
    O_status =list()
    try:
        L_func_call, err_msg = delete_nonavail_from_parent(conn
                                            ,alloc_id)
                                                
        return L_func_call, err_msg

    except Exception as argument:
        err_return = L_func_name+": Exception occured: "+ str(argument)
        print(err_return)
        return False, err_return





#if __name__ == "__main__":
#    alloc_id= "1823"
#    L_func_call = delete_nonavail_from_parent_wrapper(alloc_id)
#    print(L_func_call)



