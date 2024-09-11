from ..SCHEDULE.setup_schedule_alloc import update_alloc_type


#----------------------------------------------------------
# Wrapper for function aso_update_alloc_type
#----------------------------------------------------------
def update_alloc_type_wrapper(conn,
                              I_alloc_type, 
                              USER, 
                              I_alloc_id):

    L_func_name="update_alloc_type_wrapper"
    O_status =list()
    try:
        L_func_call, err_msg = update_alloc_type(conn
                                        ,I_alloc_type
                                        ,USER
                                        ,I_alloc_id)                                                
        return L_func_call, err_msg
    except Exception as argument:
        err_return = L_func_name+": Exception occured: "+ str(argument)
        print(err_return)
        return False, err_return

#if __name__ == "__main__":
#    I_alloc_id= 1001
#    I_alloc_type="T"
#    USER="Akhil"
#    #SYSDATE='2022-02-25'
#    L_func_call = aso_update_alloc_type_wrapper(I_alloc_type, USER, I_alloc_id)
#    print(L_func_call)

