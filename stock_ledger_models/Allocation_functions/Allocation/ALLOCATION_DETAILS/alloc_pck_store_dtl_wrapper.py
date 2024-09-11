from ..ALLOCATION_DETAILS.alloc_pack_store_dtl import alloc_pack_store_dtl


def alloc_pck_store_wrapper(conn,I_alloc_no,I_pack_no,I_wh):
    L_func_name="alloc_pck_store_wrapper"
    O_status =list()
    try:
        mycursor=conn.cursor()
        mycursor.execute("SET sql_mode = ''; ")
        L_func, err_msg = alloc_pack_store_dtl(conn,O_status,I_alloc_no,I_pack_no,I_wh)
        return L_func, err_msg
                
    except Exception as argument:
        err_return = L_func_name+": Exception occured: "+ str(argument)
        print(err_return)
        return [],err_return


#if __name__ == "__main__":
#    I_alloc_no=1700
#    I_pack_no = '200085229'
#    I_wh = 117
#    daily_view = alloc_pck_store_wrapper(I_alloc_no,I_pack_no,I_wh)  
#    print(daily_view);


