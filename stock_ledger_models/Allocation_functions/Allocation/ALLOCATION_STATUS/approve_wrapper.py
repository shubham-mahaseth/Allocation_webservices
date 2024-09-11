
from ..ALLOCATION_STATUS.approve import approve


def approve_wrapper(conn,I_alloc_no):
    L_func_name="approve_wrapper"
    O_status =list()
    try:
            mycursor=conn.cursor()
            mycursor.execute("SET sql_mode = ''; ")
            L_approve_func,err_msg = approve(conn,O_status,I_alloc_no)
            return L_approve_func,err_msg
                
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        return False, L_func_name+"- Exception occured in: "+str(argument)


#if __name__ == "__main__":
#    I_alloc_no=2034
#    daily_view = approve_wrapper(I_alloc_no) 
#    print(daily_view);


