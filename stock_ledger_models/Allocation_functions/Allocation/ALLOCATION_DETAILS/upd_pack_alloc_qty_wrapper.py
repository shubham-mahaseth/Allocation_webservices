from ..ALLOCATION_DETAILS.setup_alloc_details import update_alloc_qty_dtl
from ..ALLOCATION_DETAILS.upd_pack_alloc_qty import upd_pack_alloc_qty
import pandas as pd


def upd_pck_alloc_qty(conn,I_alloc_no):
    L_func_name="rtv_pack_dtl"
    O_status =list()
    try:
            mycursor=conn.cursor()
            mycursor.execute("SET sql_mode = ''; ")
            L_fun1,err_msg = update_alloc_qty_dtl(conn,I_alloc_no)
            if L_fun1 == True:
                L_func_call, err_msg = upd_pack_alloc_qty(conn,O_status,I_alloc_no)
                return L_func_call, err_msg
            else:
                return False, err_msg


    except Exception as argument:
        err_return = L_func_name+": "+"Exception occured: "+ str(argument)
        print(err_return)
        conn.rollback()
        return False, err_return




#if __name__ == "__main__":
#    I_alloc =2554   #2499,2500,1773461
#    L_func_call = rtv_pack_dtl(I_alloc)    
#    print(L_func_call)


