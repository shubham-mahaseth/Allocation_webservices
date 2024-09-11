from ..SCHEDULE.setup_schedule_alloc import retreive_schedule_data

#----------------------------------------------------------
# Wrapper for function retreive_schedule_data
#----------------------------------------------------------
def retreive_schedule_data_wrapper(conn,
                                 I_alloc_id):

    L_func_name="retreive_schedule_data"
    O_status =list()
    try:
        L_func_call, err_msg = retreive_schedule_data(conn
                                           ,I_alloc_id)
                                                
        return L_func_call, err_msg

    except Exception as argument:
        err_return = L_func_name+": Exception occured: "+ str(argument)
        print(err_return)
        return O_status, err_return





#if __name__ == "__main__":
#    I_alloc_id= 1928
#    L_func_call = retreive_schedule_data_wrapper(I_alloc_id)
#    print(L_func_call)


