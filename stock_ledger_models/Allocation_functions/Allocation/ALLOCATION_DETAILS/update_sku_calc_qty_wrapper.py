
from .update_sku_calc_qty import update_alloc_detail_diff_qty

def func(conn,I_alloc_no,             
                   I_wh_id,                
                   I_item_id,              
                   I_diff_id,              
                   I_order_no,             
                   I_location,             
                   I_adj_qty):
    L_func_name = "update_alloc_detail_diff_qty"
    O_status              = list()
    try:
        print(L_func_name, ": inside  ")
        L_func_call,err_msg = update_alloc_detail_diff_qty(conn,
                                                    I_alloc_no,             
                                                    I_wh_id,                
                                                    I_item_id,              
                                                    I_diff_id,              
                                                    I_order_no,             
                                                    I_location,             
                                                    I_adj_qty,
                                                    O_status)
        print(L_func_name, ": outside  :",L_func_call)
        return L_func_call,err_msg
    except Exception as argument:
        err_return = L_func_name+": "+"Exception occured: "+ str(argument)
        print(err_return)
        return [],err_return

#func(1773050,             
#     1,                
#     '123236179',              
#     'GREY',              
#     None,             
#     12,             
#     150) #150
