from .setup_like_item import RETREIVE_LIKE_ITEM_MAP

#----------------------------------------------------------
# Function to call other fuctions for testing
#----------------------------------------------------------
def rtv_like_item(conn,I_alloc_no): #conn has to be passed when merging with REACT.
    L_func_name="test_func"
    O_status =list()
    try:
        leftDf,rightDf,err_msg = RETREIVE_LIKE_ITEM_MAP(conn,I_alloc_no)
        return leftDf,rightDf,err_msg
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        return [], L_func_name+": "+"Exception occured in "+ str(argument)




#if __name__ == "__main__":
#    I_alloc = 2074
#    mode = 'NEW'
#    L_func_call = rtv_like_item(I_alloc)    
#    print(L_func_call)
