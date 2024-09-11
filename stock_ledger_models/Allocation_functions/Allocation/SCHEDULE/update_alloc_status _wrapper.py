from ..SCHEDULE.setup_schedule_alloc import update_alloc_status


#----------------------------------------------------------
# Wrapper for function aso_update_alloc_status
#----------------------------------------------------------
def update_alloc_status_wrapper(conn, 
                                I_status, 
                                I_alloc_id):

    L_func_name="aso_update_alloc_status_wrapper"
    O_status =list()
    try:
        L_func_call, err_msg = update_alloc_status(conn
                                          ,I_status
                                          ,I_alloc_id)
        return L_func_call, err_msg
    except Exception as argument:
        err_return = L_func_name+": Exception occured: "+ str(argument)
        print(err_return)
        return False, err_return





#if __name__ == "__main__":
#    I_alloc_id= 12345678
#    I_status="WS"
#    L_func_call = aso_update_alloc_status_wrapper(I_status, I_alloc_id)
#    print(L_func_call)
