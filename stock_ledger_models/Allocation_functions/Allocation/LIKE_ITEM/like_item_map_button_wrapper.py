from ..LIKE_ITEM.setup_like_item import map_like_item_details

#----------------------------------------------------------
# Function to populate mapped items for allocation
#----------------------------------------------------------
def map_item(conn,I_alloc
             ,I_item_list
             ,I_item_parent         #for style diff
             ,I_sku                 
             ,I_diff_id             #for style diff
             ,I_no_sizes            
             ,I_weight              
             ,I_size_prf_ind        #for style diff
             ):

    L_func_name="map_item"
    O_status =list()
    try:
            print("MAP input : \n",I_alloc
             ,I_item_list
             ,I_item_parent         #for style diff
             ,I_sku                 
             ,I_diff_id             #for style diff
             ,I_no_sizes            
             ,I_weight              
             ,I_size_prf_ind )
            L_func_call, err_msg = map_like_item_details(conn
                                                ,I_alloc
                                                ,I_item_list
                                                ,I_item_parent         #for style diff
                                                ,I_sku                 
                                                ,I_diff_id             #for style diff
                                                ,I_no_sizes            
                                                ,I_weight              
                                                ,I_size_prf_ind        #for style diff
                                                ,O_status)
            return L_func_call, err_msg

    except Exception as argument:
        err_return = L_func_name+": "+"Exception occured in "+ str(argument)
        #print("Exception occured in: ",L_func_name,argument)
        return [],err_return




#if __name__ == "__main__":
#    I_alloc = 7
#    I_item_list = None
#    I_item_parent = 105364513
#    I_sku = None
#    I_diff_id = None
#    I_no_sizes = 1
#    I_weight = 1
#    I_size_prf_ind = 'N'

#    L_func_call = map_item(I_alloc
#                           ,I_item_list
#                           ,I_item_parent        
#                           ,I_sku                
#                           ,I_diff_id            
#                           ,I_no_sizes           
#                           ,I_weight             
#                           ,I_size_prf_ind)
#    print(L_func_call)


