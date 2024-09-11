#from get_connection import get_mysql_conn
from .alloc_wisummary import retreive_wisummary_details

def retreive_wis_dtls(conn,I_search):
    L_func_name = "retreive_wis_dtls"
    O_status =list()
    try:
        L_rwd_func_call = retreive_wisummary_details(conn,I_search,O_status)
        return L_rwd_func_call
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured :"+ str(argument)
        conn.rollback()
        return [],[],err_return, False

#I_search = {"I_alloc_no" : '1918351',
#			"I_po_type"  : 'WH',
#			"I_multi_wh" : 'N'}

#wis_dtls(I_search)
