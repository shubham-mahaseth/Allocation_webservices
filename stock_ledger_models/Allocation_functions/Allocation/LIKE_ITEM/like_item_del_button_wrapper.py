from ..LIKE_ITEM.setup_like_item import delete_like_item_map


#----------------------------------------------------------
# Function to delete mapped items for allocation
#----------------------------------------------------------
def delete_mapped_item(conn,I_alloc):

    L_func_name="delete_mapped_item"
    O_status =list()
    try:
        L_func_call,err_msg = delete_like_item_map(conn
                                            ,I_alloc
                                            ,O_status)
        return L_func_call,err_msg

    except Exception as argument:
        err_return = L_func_name+": "+"Exception occured in "+ str(argument)
        #print("Exception occured in: ",L_func_name,argument)
        return [],err_return




#if __name__ == "__main__":
#    I_alloc = 123456789

#    L_func_call = delete_mapped_item(I_alloc)
#    print(L_func_call)




