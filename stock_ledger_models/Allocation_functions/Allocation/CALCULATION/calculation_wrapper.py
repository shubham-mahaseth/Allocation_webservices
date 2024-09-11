
from ..CALCULATION.calculate_validation import *
from ..CALCULATION.calculate import *



#----------------------------------------------------------
# Function to call calculation process for allocation
#----------------------------------------------------------
def do_calculation(conn,I_alloc):
    L_func_name="do_calculation"
    O_status =list()
    L_func_call = True
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/calculate_validation_queries.yaml') as fh:
            queries                         = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_error_ind              = queries['do_calculation']['Q_error_ind']
            mycursor = conn.cursor()

            from ..CALCULATION.calculate_validation import calculate_validation
            L_func_call,err_msg = calculate_validation(conn,I_alloc,O_status)
            print("calculate_validate: ",L_func_call)
            if L_func_call ==True:
                from ..CALCULATION.calculate import calculate
                L_func_call,err_msg = calculate(conn,I_alloc,O_status)
                print("calculate: ",L_func_call)
                return L_func_call,err_msg
            else:
                mycursor.execute(Q_error_ind,(I_alloc,))
                return L_func_call, err_msg

    except Exception as argument:
        err_return = L_func_name+":"+str(O_status)+": "+ str(argument)
        print(err_return)
        return False,err_return


#if __name__ == "__main__":
#    I_alloc = 3261   #alloc_no
#    L_func_call = do_calculation(I_alloc)    
#    print(L_func_call)
