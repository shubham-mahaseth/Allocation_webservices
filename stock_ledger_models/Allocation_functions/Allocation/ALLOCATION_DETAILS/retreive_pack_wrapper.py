from ..ALLOCATION_DETAILS.retreive_pck_alloc_dtl import retreive_pack_alloc_dtl
import pandas as pd


def rtv_pack_dtl(conn,I_alloc_no):
    L_func_name="rtv_pack_dtl"
    O_status =list()
    try:        
        print("rtv_pack_dtl wrapper \n")
        L_func_call,err_msg = retreive_pack_alloc_dtl(conn,I_alloc_no,O_status)
        return L_func_call,err_msg

    except Exception as argument:
        err_return = L_func_name+": "+"Exception occured: "+ str(argument)
        print(err_return)
        conn.rollback()
        return [],err_return




#if __name__ == "__main__":
#    I_alloc =2554   #2499,2500,1773461
#    L_func_call = rtv_pack_dtl(I_alloc)    
#    print(L_func_call)

