from ..LIKE_ITEM.setup_like_item import insert_like_item_map



#----------------------------------------------------------
# Function to insert mapped items for allocation into DB
#----------------------------------------------------------
def insert_mapped_item(conn,I_alloc):

    L_func_name="insert_mapped_item"
    O_status =list()
    try:
        L_func_call,err_msg = insert_like_item_map(conn
                                            ,I_alloc
                                            ,O_status)
        return L_func_call,err_msg

    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured in "+ str(argument)
        return False, err_return




#if __name__ == "__main__":
#    I_alloc = 7

#    L_func_call = insert_mapped_item(I_alloc)
#    print(L_func_call)



