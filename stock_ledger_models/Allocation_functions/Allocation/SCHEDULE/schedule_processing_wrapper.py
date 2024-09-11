from ..SCHEDULE.schedule_alloc_threading_process import run_schedule


#----------------------------------------------------------
# Wrapper for processing schedule allocations
#----------------------------------------------------------
def schedule_wrapper(conn):

    L_func_name="SCHEDULE_WRAPPER"
    O_status =list()
    try:
        L_func_call, err_msg = run_schedule(conn)                                                
        return L_func_call, err_msg
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        return False, L_func_name + " - EXCEPTION OCCURED : " +str(argument)

