from ..RULES_AND_LOCATIONS.populate_store import populate_store



#----------------------------------------------------------
# Function to populate stores for allocation
#----------------------------------------------------------
def pop_store(conn,I_search_criteria):
    L_func_name="pop_store"
    O_status =list()
    try:
        L_func_call,err_msg = populate_store(conn,
                                        I_search_criteria,
                                        O_status)
        print("Alloc_pop_store_table:2: ",L_func_call,err_msg,I_search_criteria)
        print("DEBUG CONN errmsg: ",err_msg)
        return L_func_call,err_msg

    except Exception as argument:
        print("DEBUG CONN")
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured:"+ str(argument)
        return [], err_return




#if __name__ == "__main__":
#    i_search_criteria = {"ALLOC_NO": 1700
#                          ,"ALL_STORE": 'N'
#                          ,"LOCATION": []
#                          ,"LOCATION_LIST": []
#                          ,"LOCATION_TRAIT": [1]
#                          ,"EXCLUDE_LOCATION": []
#                         }

#    l_func_call = pop_store(i_search_criteria)    
#    print(l_func_call)
