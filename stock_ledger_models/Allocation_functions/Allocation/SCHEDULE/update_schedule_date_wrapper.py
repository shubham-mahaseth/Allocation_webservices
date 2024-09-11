from ..SCHEDULE.setup_schedule_alloc import update_schedule_date

#----------------------------------------------------------
# Wrapper for function aso_update_alloc_type
#----------------------------------------------------------
def update_schedule_date_wrapper(conn
                                 ):

    L_func_name="update_schedule_date_wrapper"
    O_status =list()
    try:
        L_func_call, err_msg = update_schedule_date(conn)
                                                
        return L_func_call, err_msg

    except Exception as argument:
        err_return = L_func_name+": Exception occured: "+ str(argument)
        print(err_return)
        return False, err_return





#if __name__ == "__main__":
#    alloc_id= ""
#    L_func_call = aso_update_schedule_date_wrapper(alloc_id)
#    print(L_func_call)


