from ..CREATE_SCREEN.on_click_process import refresh_grid


#----------------------------------------------------------
# Function to refresh all screen
#----------------------------------------------------------
def refresh_all(conn,I_alloc):

    L_func_name="Refresh_Grid_Wrap"
    O_status =list()
    try:
        L_func_call, err_msg  = refresh_grid(conn
                                    ,I_alloc
                                    ,O_status)
        return L_func_call, err_msg

    except Exception as argument:
        err_return = L_func_name+": "+"Exception occured in "+ str(argument)
        print("Exception occured in: ",L_func_name,argument)
        return False,err_return




#if __name__ == "__main__":
#    I_alloc = 111222

#    L_func_call = refresh_all(I_alloc)
#    print(L_func_call)

