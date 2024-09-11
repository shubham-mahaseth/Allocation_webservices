from ..GLOBAL_FILES.get_connection import get_mysql_conn
from ..INVENTORY_SETUP.update_alloc_ext import update_alloc


def update_alloc_wrapper(conn,L_alloc_no,L_level,L_date,L_recalc_ind,I_input_data):
    L_func_name="update_alloc_wrapper"
    O_status =list()
    try:
            mycursor=conn.cursor()
            mycursor.execute("SET sql_mode = ''; ")
            L_update_alloc_dtl,err_msg = update_alloc(conn,
                                              O_status,
                                              L_alloc_no,
                                              L_level,
                                              L_date,
                                              L_recalc_ind,
                                              I_input_data)
            #if L_update_alloc_dtl==True:
            return L_update_alloc_dtl,err_msg
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception :"+ str(argument)
        return False, err_return


#if __name__ == "__main__":
#    L_alloc_no='8'
#    L_level = 'T'
#    L_date='2023-02-25'
#    L_recalc_ind='Y'
#    I_input_data={'WHATIF_SOURCE_TYPE_IND': 0, 'WH_SOURCE_TYPE_IND': 1, 'MIN_AVAIL_QTY': None, 'MAX_AVAIL_QTY': None, 'ITEM_GRANDPARENT': [], 'CLEARANCE_IND': 'N', 'ALLOC_CRITERIA': 'W', 'CONTEXT': 'SALES', 'ALLOC_LEVEL': 'T', 'ALLOC_TYPE': 'A', 'STATUS': 'WS', 'PROMOTION': 1234, 'CREATE_ID': 'Tarun', 'ALLOC_DESC': 'sample', 'RELEASE_DATE': '2023-01-18', 'ALLOC_NO': 404, 'ALLOC_LEVEL_CODE': 'Sku', 'ALLOC_TYPE_CODE': 'Ad-Hoc', 'STATUS_CODE': 'Worksheet', 'PROMOTION_CODE': '', 'CONTEXT_CODE': 'Sales', 'HIER1': ['111'], 'HIER2': [], 'HIER3': [], 'WH': [], 'SUPPLIER': [], 'SUPPLIER_SITE': [], 'PACK_NO': [], 'ITEM_PARENT': [], 'DIFF_ID': [], 'SKU': [], 'VPN': [], 'UDA': [], 'UDA_VALUE': [], 'EXCLUDE_UDA': [], 'EXCLUDE_UDA_VALUE': [], 'ITEM_LIST': [], 'RECALC_IND': None, 'CREATE_DATETIME': "2022-02-10"}
#    daily_view = update_alloc_wrapper(L_alloc_no,L_level,L_date,L_recalc_ind,I_input_data) 
#    print(daily_view);
