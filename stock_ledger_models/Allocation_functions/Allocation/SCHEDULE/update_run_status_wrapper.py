from ..SCHEDULE.setup_schedule_alloc import update_run_status
#from GLOBAL_FILES.get_connection import get_mysql_conn


#----------------------------------------------------------
# Wrapper for function aso_update_run_status
#----------------------------------------------------------
def update_run_status_wrapper(conn,
                              I_alloc_id_child,
                              O_error_message,
                              I_alloc_id_parent):

    L_func_name="update_run_status_wrapper"
    O_status =list()
    try:
        L_func_call, err_msg = update_run_status(conn
                                        ,I_alloc_id_child
                                        ,O_error_message
                                        ,I_alloc_id_parent)
        return L_func_call, err_msg

    except Exception as argument:
        err_return = L_func_name+": Exception occured: "+ str(argument)
        print(err_return)
        return False, err_return





#if __name__ == "__main__":
#    I_alloc_id_child= 7464949
#    O_error_message="INVALID ERRORS"
#    I_alloc_id_parent="12345678"
#    L_func_call = aso_update_run_status_wrapper(I_alloc_id_child,O_error_message,I_alloc_id_parent)
#    print(L_func_call)





