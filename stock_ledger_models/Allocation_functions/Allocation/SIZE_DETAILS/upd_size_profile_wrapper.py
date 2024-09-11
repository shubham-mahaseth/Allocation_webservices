#from GLOBAL_FILES.get_connection import get_mysql_conn
from ..SIZE_DETAILS.upd_size_profile_dtl import upd_size_profile




#----------------------------------------------------------
# Function to call calculation process for allocation
#----------------------------------------------------------
def do_upd_size_profile(conn,I_alloc,I_source_item,I_diff_ID,I_order,I_wh_id,I_loc,I_tran_item_value,I_input_qty):
    L_func_name="do_upd_size_profile"
    O_status =list()
    L_func_call = True
    try:
        #I_get_mysql_conn = list()
        #I_get_mysql_conn.append(0)
        #with get_mysql_conn (I_get_mysql_conn) as conn:
        L_func_call = upd_size_profile(conn,O_status,I_alloc,I_source_item,I_diff_ID,I_order,I_wh_id,I_loc,I_tran_item_value,I_input_qty)
        print("upd_size_profile: ",L_func_call)
        return L_func_call
    except Exception as argument:
        err_return = L_func_name+": Exception occured in: "+ str(argument)
        print("Exception occured in: ",L_func_name,argument)
        err_msg = (err_return,False)
        return err_msg




#if __name__ == "__main__":
#    I_alloc = 1409   #alloc_no
#    I_source_item= '021391412'
#    I_diff_ID = 'WHITE'
#    I_order = None
#    I_wh_id = 1
#    I_loc= 27
#    I_tran_item_value= '021391412'
#    I_input_qty= 50
#    L_func_call = do_upd_size_profile(I_alloc,I_source_item,I_diff_ID,I_order,I_wh_id,I_loc,I_tran_item_value,I_input_qty)    
#    print(L_func_call)

