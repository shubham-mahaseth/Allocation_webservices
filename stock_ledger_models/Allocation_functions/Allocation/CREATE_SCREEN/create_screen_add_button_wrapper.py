
from .populate_search_result import search



#----------------------------------------------------------
# Function to populate items for allocation
#----------------------------------------------------------
def populate_item(conn,I_search_criteria):
    L_func_name="populate_item"
    O_status =list()
    emp_lis = list()
    try:
        L_func_call,err_msg = search(conn,I_search_criteria,O_status)
        return L_func_call,err_msg

    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured:"+ str(argument)
        return emp_lis,err_return




#if __name__ == "__main__":
#    i_search_criteria = {'WHATIF_SOURCE_TYPE_IND': 0, 
#                         'WH_SOURCE_TYPE_IND': 1,
#                        'TSF_SOURCE_TYPE_IND': 0, 
#                        'PO_SOURCE_TYPE_IND': 0, 
#                        'ASN_SOURCE_TYPE_IND': 0, 
#                        'ALLOC_CRITERIA': 'W', 
#                        'CONTEXT': 'SALES', 
#                        'ALLOC_LEVEL': 'T', 
#                        'ALLOC_TYPE': 'W', 
#                        'STATUS': 'WS', 
#                        'PROMOTION': None, 
#                        'CREATE_ID': 'Admin', 
#                        'ALLOC_DESC': 'sample', 
#                        'RELEASE_DATE': '2023-04-05', 
#                        'ALLOC_NO': 1021, 
#                        'ALLOC_LEVEL_CODE': 'Sku', 
#                        'ALLOC_TYPE_CODE': 'SCHEDULE', 
#                        'STATUS_CODE': 'Worksheet', 
#                        'PROMOTION_CODE': None, 
#                        'CONTEXT_CODE': 'Sales', 
#                        'HIER1': [111], 
#                        'HIER2': [], 
#                        'HIER3': [], 
#                        'WH': [], 
#                        'SUPPLIER': [], 
#                        'SUPPLIER_SITE': [], 
#                        'PACK_NO': [],
#                       'ITEM_PARENT': [], 
#                       'DIFF_ID': [], 
#                       'SKU': [], 
#                       'ITEM_LIST': [], 
#                       'VPN': [], 
#                       'UDA': [], 
#                       'UDA_VALUE': [], 
#                       'EXCLUDE_UDA': [], 
#                       'EXCLUDE_UDA_VALUE': [], 
#                       'ITEM_GRANDPARENT': [], 
#                       'CLEARANCE_IND': None, 
#                       'RECALC_IND': None, 
#                       'PO': [], 
#                       'ASN': [], 
#                       'TSF': [], 
#                       'PO_TYPE': None, 
#                       'START_DATE': None, 
#                       'END_DATE': None, 
#                       'EISD_START_DATE': None, 
#                       'EISD_END_DATE': None, 
#                       'MIN_AVAIL_QTY': None, 
#                       'MAX_AVAIL_QTY': None, 
#                       'CREATE_DATETIME': '2023-04-05 14:33:59.342756'
#                       }

#    l_func_call = populate_item(i_search_criteria)    
#    print(l_func_call)
