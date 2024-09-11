from ..INVENTORY_SETUP.load_item_source import load_item

def wrapper_load_source(conn,L_alloc_no,O_status):
    L_func_name="wrapper_load_source"
    O_status =list()
    try:
        print("Load Item : Inside")
        L_insert_alloc_dtl,err_msg = load_item(conn,L_alloc_no,O_status)
        print("Load Item : Outside")
        return L_insert_alloc_dtl,err_msg
    except Exception as argument:
        err_return = L_func_name+": "+"Exception :"+ str(argument)
        print("Exception occured in: ",L_func_name,argument)
        return False,err_return


#if __name__ == "__main__":
#    L_alloc = 1601
#    O_status = None
#    L_insert_alloc_dtl = wrapper_load_source(L_alloc,O_status)    
#    print(L_insert_alloc_dtl)

