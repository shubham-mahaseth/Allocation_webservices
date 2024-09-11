
from .sale_alloc import sales_diff_qty


def wrapper_process_sales_qty(conn,i_hier1,i_hier2,i_hier3,i_sdate,i_year_interval,i_weeks):
    L_func ='wrapper_process_sales_qty'
    try:
        o_status = 0
        L_fun_sales,err_msg = sales_diff_qty(conn,o_status,i_hier1,i_hier2,i_hier3,i_sdate,i_year_interval,i_weeks)
        return L_fun_sales,err_msg
    except Exception as argument:
        #print("Exception occured in: ",argument)
        return [], L_func+": Exception occured in: " + str(argument)

# if __name__ == "__main__":
# 	o_status=None
# 	i_year_interval = 3
# 	i_sdate = '2024-08-04'
# 	i_weeks = 2
# 	i_hier1 = 176
# 	i_hier2 = 1176
# 	i_hier3 = 10176
# 	daily_view = wrapper_process_sales_qty(o_status,i_hier1,i_hier2,i_hier3,i_sdate,i_year_interval,i_weeks)  
# 	print(daily_view);
