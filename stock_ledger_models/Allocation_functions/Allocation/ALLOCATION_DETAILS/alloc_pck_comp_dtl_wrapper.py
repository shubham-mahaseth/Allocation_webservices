
from ..ALLOCATION_DETAILS.alloc_pack_comp_dtls import alloc_pack_comp_dtl


def alloc_pck_com_wrapper(conn,I_alloc_no,I_pack_no):
    L_func_name="alloc_pck_com_wrapper"
    O_status =list()
    try:
        mycursor=conn.cursor()
        mycursor.execute("SET sql_mode = ''; ")
        L_func,err_msg = alloc_pack_comp_dtl(conn,O_status,I_alloc_no,I_pack_no)
        return L_func, err_msg
                
    except Exception as argument:        
        err_return = L_func_name+": Exception occured: "+ str(argument)
        print(err_return)
        return [],err_return


#if __name__ == "__main__":
#    I_alloc_no=1700
#    I_pack_no = '200085229'
#    daily_view = alloc_pck_com_wrapper(I_alloc_no,I_pack_no)  
#    print(daily_view);