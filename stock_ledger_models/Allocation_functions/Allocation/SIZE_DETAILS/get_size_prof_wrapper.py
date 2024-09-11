
from .setup_size_detail import get_alloc_size_profile

#----------------------------------------------------------
# Function to call other fuctions for testing
#----------------------------------------------------------
def get_size_prf(conn,I_alloc,
                   I_wh,
                   I_order_no,
                   I_item_id,
                   I_diff_id,
                   I_location):
    L_func_name="get_size_prf"
    O_status =list()
    try:
        L_func_call, err_msg = get_alloc_size_profile(conn,
                                                I_alloc,
                                                I_wh,
                                                I_order_no,
                                                I_item_id,
                                                I_diff_id,
                                                I_location)
        return L_func_call,err_msg
    except Exception as argument:
        err_return = L_func_name+": "+"Exception occured: "+ str(argument)

        #print("Exception occured in: ",L_func_name,argument)
        return O_status, err_return




#if __name__ == "__main__":
#    I_alloc = '12345678'
#    I_wh = 0
#    I_order_no = 0
#    I_item_id = 0
#    I_diff_id = 0
#    I_location = 0



#    L_func_call = sprd_alloc_loc(I_alloc,
#                                 I_wh,
#                                 I_order_no,
#                                 I_item_id,
#                                 I_diff_id,
#                                 I_location)   
#    print(L_func_call)
