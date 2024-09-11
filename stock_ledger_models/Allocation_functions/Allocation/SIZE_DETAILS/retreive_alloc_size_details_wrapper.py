
from .setup_size_detail import retreive_alloc_size_details

#----------------------------------------------------------
# Function to call other fuctions for testing
#----------------------------------------------------------
def rtv_alloc_size_dtl(conn,I_alloc,
                       I_wh,
                       I_order_no,
                       I_item_id,
                       I_diff_id,
                       I_location):
    L_func_name="rtv_alloc_size_dtl"
    O_status =list()
    try:
        L_func_call = retreive_alloc_size_details (conn,
                                                    I_alloc,
                                                    I_wh,
                                                    I_order_no,
                                                    I_item_id,
                                                    I_diff_id,
                                                    I_location)
        return L_func_call
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": Exception occured in: "+ str(argument)
        return [],[],err_return




#if __name__ == "__main__":
#    I_alloc = 3000
#    I_wh = '1'
#    I_order_no = 'NULL'
#    I_item_id  = '105364038'
#    I_diff_id  = 'MADISON'
#    I_location = 'NULL'

#    L_func_call = rtv_alloc_size_dtl(I_alloc,
#                                     I_wh,
#                                     I_order_no,
#                                     I_item_id,
#                                     I_diff_id,
#                                     I_location)
#    print(L_func_call)

