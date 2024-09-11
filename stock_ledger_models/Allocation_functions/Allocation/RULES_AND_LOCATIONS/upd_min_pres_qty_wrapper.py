from .setup_rules_locations import UPDATE_MIN_PRESENTATION_QTY
from ..QUANTITY_LIMITS.setup_qty_limits import P360_RETREIVE_QUANTITY_LIMITS

#----------------------------------------------------------
# Function to call other fuctions for testing
#----------------------------------------------------------
def MIN_PRES(conn,I_alloc_no):
    L_func_name="test_func"
    try:
        print("CONNECTION SUCCESS")
        L_func_call = P360_RETREIVE_QUANTITY_LIMITS(conn,
                                                    I_alloc_no,
                                                    I_mode)
        if len(L_func_call)>0:
            L_func_call = UPDATE_MIN_PRESENTATION_QTY(conn,
                                                        I_alloc_no)
            return L_func_call
        else:
            print("There is no record in alloc_quantity_limits table.")
            return False
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        return False




#if __name__ == "__main__":
#    I_alloc = 1070
#    L_func_call = MIN_PRES(I_alloc)    
#    print(L_func_call)





