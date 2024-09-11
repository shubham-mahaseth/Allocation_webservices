from ..GLOBAL_FILES.get_connection import get_mysql_conn
from ..INVENTORY_SETUP.inventory_setup import setup_location,setup_item_location,update_inv
from ..INVENTORY_SETUP.load_item_source import load_item
from ..INVENTORY_SETUP.update_alloc_ext import update_alloc
from ..CALCULATION.calculation_setup import pop_calc_destination,seed_calc_need,setup_need,setup_sales_hist_need,setup_sku_sales_hist_need,calculation_setup
from ..CALCULATION.calculate_validation import calculate_validation
from ..CALCULATION.calculate import calculate
from ..RULES_AND_LOCATIONS.change_weight import load_change_weight_dates
#from RULES_AND_LOCATIONS.change_weight import load_change_weight_dates



#----------------------------------------------------------
# Function to call other fuctions fro testing
#----------------------------------------------------------
def test_func(I_alloc):
    L_func_name="test_func"
    O_status =list()
    try:
        I_get_mysql_conn = list()
        I_get_mysql_conn.append(0)
        with get_mysql_conn (I_get_mysql_conn) as conn:
            L_func_call = load_change_weight_dates(conn,I_alloc,O_status)
            return L_func_call
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        conn.rollback()
        return False




if __name__ == "__main__":
    I_alloc = 1702
    #O_status = list()
    L_func_call = test_func(I_alloc)    
    print(L_func_call) 