from ..CREATE_SCREEN.on_click_process import populate_header


#----------------------------------------------------------
# Function to populate allocation header
#----------------------------------------------------------
def pop_alloc_head(conn, I_alloc):

    L_func_name="pop_alloc_head"
    O_status = list()
    emp_list = list()
    try:
        L_func_call,err_msg = populate_header(conn
                                        ,I_alloc
                                        ,O_status)
        return L_func_call,err_msg
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception :"+ str(argument)
        return emp_list,err_return




#if __name__ == "__main__":
#    I_alloc = 330

#    L_func_call = pop_alloc_head(I_alloc)
#    print(L_func_call)
