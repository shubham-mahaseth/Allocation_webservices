from ..WHATIF_SUMMARY.create_whatif_po import create_whatif_po


def create_whatif_po_wrapper(conn,I_alloc_no,I_create_id):
    L_func_name="create_whatif_po_wrapper"
    O_status =list()
    emp_list = list()
    try:
        mycursor=conn.cursor()
        mycursor.execute("SET sql_mode = ''; ")
        print(1)
        L_function,err_msg = create_whatif_po(conn,O_status,I_alloc_no,I_create_id)
        return L_function,err_msg
        #if L_function==True:
        #    return True
    except Exception as argument:
        err_return = L_func_name+": "+"Exception occured :"+ str(argument)
        print("Exception occured in: ",L_func_name,argument)
        return emp_list,err_return


#if __name__ == "__main__":
#    I_alloc_no= 2060
#    daily_view = create_whatif_po_wrapper(I_alloc_no) 
#    print(daily_view);