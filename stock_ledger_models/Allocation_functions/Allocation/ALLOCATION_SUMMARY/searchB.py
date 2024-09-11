
from .populate_srch_results import populate_search_results

def to_call_fun(conn,I_search):
    L_func_name = "to_call_fun"
    O_status =list()
    try: 
        L_func_call,err_msg = populate_search_results (conn,I_search,O_status)
        return L_func_call,err_msg

    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured in "+ str(argument)
        conn.rollback()
        return [],err_return

#to_call_fun()

#to_call_fun(1111)

#I_search = {"I_alloc_no"          : '1896',
#			"I_alloc_desc"        : None,
#			"I_from_release_date" : None,
#			"I_to_release_date"   : None,
#			"I_from_create_date"  : None,
#			"I_to_create_date"    : None,
#			"I_create_user"       : None,
#			"I_alloc_status"      : None,
#			"I_PO"                : None,
#			"I_Tst_no"            : None,
#			"I_context"           : None,
#			"I_promotion"         : None,
#			"I_dept"              : None,
#			"I_class"             : None,
#			"I_subclass"          : None,
#			"I_wh"                : None,
#			"I_item_parent"       : None,
#			"I_diff_id"           : None,
#			"I_item_sku"          : None,
#			"I_vpn"               : None,
#			"I_alloc_type"        : None,#acs
#			"I_source"            : None,
#			"I_asn"               : None,
#			"I_pack_id"           : None,
#			"I_batch_calc_ind"    : None}
##to_call_fun(I_search)

