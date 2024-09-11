from ..CREATE_SCREEN.on_click_process import populate_error


#----------------------------------------------------------
# Function to populate allocation header
#----------------------------------------------------------
def pop_error(conn,I_alloc,I_err1,I_err2,I_err3,I_err4,I_err5,I_to_date,I_from_date):

    L_func_name="pop_error"
    O_status =list()
    try:
        L_func_call,err_msg =populate_error (conn,
                                        I_alloc,
                                        I_err1,
                                        I_err2,
                                        I_err3,
                                        I_err4,
                                        I_err5,
                                        I_to_date,
                                        I_from_date,
                                        O_status)
        return L_func_call,err_msg

    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured in "+ str(argument)
        return [],err_return




#if __name__ == "__main__":
#    I_alloc = 6
#    I_err1 = ''
#    I_err2 = ''
#    I_err3 = ''
#    I_err4 = ''
#    I_err5 = ''
#    I_to_date = ''
#    I_from_date = ''

#    L_func_call = pop_error(I_alloc,I_err1,I_err2,I_err3,I_err4,I_err5,I_to_date,I_from_date)
#    print(L_func_call)

