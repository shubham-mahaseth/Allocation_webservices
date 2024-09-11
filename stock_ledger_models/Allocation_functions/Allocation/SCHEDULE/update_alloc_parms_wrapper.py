from .setup_schedule_alloc import update_alloc_parms


#----------------------------------------------------------
# Wrapper for function aso_update_alloc_parms
#----------------------------------------------------------
def update_alloc_parms_wrapper(conn,
                               I_alloc_id, 
                               I_child_alloc_status, 
                               I_start_date,
                               I_end_date, 
                               I_frequency, 
                               I_days_of_week, 
                               I_next_schedule_run,
                               I_alloc_type, 
                               I_create_id, 
                               I_last_update_id):

    L_func_name="update_alloc_parms_wrapper"
    O_status =list()
    try:
        L_func_call, err_msg = update_alloc_parms(conn, 
                                         I_alloc_id, 
                                         I_child_alloc_status, 
                                         I_start_date,
                                         I_end_date, 
                                         I_frequency, 
                                         I_days_of_week, 
                                         I_next_schedule_run,
                                         I_alloc_type,
                                         I_create_id, 
                                         I_last_update_id)
        return L_func_call, err_msg

    except Exception as argument:
        err_return = L_func_name+": Exception occured: "+ str(argument)

        print(err_return)
        return False, err_return





#if __name__ == "__main__":
#    I_alloc_id = "12345678"
#    I_child_alloc_status= "0"
#    I_start_date = "2023-03-09"
#    I_end_date = "2023-03-16"
#    I_frequency = "M"
#    I_days_of_week = "0000111"
#    I_next_schedule_run = "2023-03-15"
#    I_alloc_type = "S"
#    I_create_id = "Akhil"
#    I_last_update_id = "chandan"
#    L_func_call = aso_update_alloc_parms_wrapper(I_alloc_id, I_child_alloc_status, I_start_date,
#                           I_end_date, I_frequency, I_days_of_week, I_next_schedule_run,
#                          I_alloc_type, I_create_id, I_last_update_id)
#    print(L_func_call)






