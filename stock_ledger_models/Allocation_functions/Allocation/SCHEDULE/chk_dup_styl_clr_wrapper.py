from ..SCHEDULE.setup_schedule_alloc import chk_dup_styl_clr

#----------------------------------------------------------
# Wrapper for function aso_chk_dup_styl_clr
#----------------------------------------------------------
def chk_dup_styl_clr_wrapper(conn,
                             I_alloc_id,
                             L_alloc_level):

    L_func_name="chk_dup_styl_clr_wrapper"
    O_status =list()
    try:
        L_func_call, err_msg = chk_dup_styl_clr(conn
                                            ,I_alloc_id
                                            ,L_alloc_level)
                                                
        return L_func_call, err_msg
    except Exception as argument:
        err_return = L_func_name+": Exception occured: "+ str(argument)
        print(err_return)
        return False, err_return





#if __name__ == "__main__":
#    I_alloc_id= 8
#    L_alloc_level= 'T'
#    L_func_call = aso_chk_dup_styl_clr_wrapper(I_alloc_id,L_alloc_level)
#    print(L_func_call)




