
from ..CALCULATION.calculate_validation import calculate_validation
from ..CALCULATION.calculate import calculate
from ..CALCULATION.calculate_whatif import wif_calculate


#----------------------------------------------------------
# Function to call calculation process for allocation
#----------------------------------------------------------
def do_whatif_calc(conn,I_alloc):
    L_func_name="do_whatif_calc"
    print("do_whatif_calc : ")
    O_status =list()
    try:
        L_func_call,err_msg = calculate_validation(conn,I_alloc,O_status)
        if L_func_call ==True:
            L_func_call = calculate(conn,I_alloc,O_status)
            print("calculate: ",L_func_call)
            if L_func_call ==True:
                L_func_call = wif_calculate(conn,I_alloc,O_status)
                print("whatif_calculate: ",L_func_call)
                return L_func_call, err_msg
            else:
                print("calculate: ",L_func_call)
                return L_func_call, err_msg 
        else:
            print("calculate_validation: ",L_func_call)
            return L_func_call, err_msg
    except Exception as argument:
        err_return = L_func_name+":"+str(O_status)+": "+ str(argument)
        print(err_return)
        return False, err_return




#if __name__ == "__main__":
#    I_alloc = 1295    #alloc_no
#    L_func_call = do_whatif_calc(I_alloc)    
#    print(L_func_call)

