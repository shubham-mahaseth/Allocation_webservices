
from .alloc_wisummary import retreive_wisummary_into_temp
from datetime import timedelta, date

def submit_wis_dtls(conn,I_search):
    L_func_name = "submit_wis_dtls"
    O_status =list()
    try:
        L_rwd_func_call,err_msg = retreive_wisummary_into_temp(conn,I_search,O_status)
        return L_rwd_func_call,err_msg
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception :"+ str(argument)
        conn.rollback()
        return False,err_return

#I_search = {"I_alloc_no" : '1918351',
#			"I_multi_wh" : 'N'}

#wis_dtls(I_search)

