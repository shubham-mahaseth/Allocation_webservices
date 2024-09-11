
from ..RULES_AND_LOCATIONS.change_weight import retrieve_rule_dates_weight


#----------------------------------------------------------
# Function to call other fuctions fro testing
#----------------------------------------------------------
def fetch_retrieve_chng_wt(conn,I_alloc_no):
    L_func_name="fetch_retrieve_chng_wt"
    O_status =list()
    try:
        L_func_call,err_msg = retrieve_rule_dates_weight(conn,I_alloc_no,O_status)
        return L_func_call,err_msg
    except Exception as argument:
        err_return = L_func_name+": "+"Exception occured:"+ str(argument)
        print("Exception occured in: ",L_func_name,argument)
        conn.rollback()
        return False, err_return

#if __name__ == "__main__":
#    I_alloc = '1741'
#    L_func_call = fetch_retrieve_chng_wt(I_alloc)    
#    print(L_func_call)