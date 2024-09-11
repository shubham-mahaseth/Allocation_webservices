from ..WHATIF_SUMMARY.setup_whatif_summary import retreive_wisummary_details

def wis_dtls(conn,I_search):
    L_func_name = "wis_dtls"
    O_status =list()
    try:
        L_func_call = retreive_wisummary_details(conn,I_search,O_status)
        return L_func_call
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)

#I_search = {"I_alloc_no" : '1918351',
#			"I_po_type"  : 'WH',
#			"I_multi_wh" : 'N'}

#wis_dtls(I_search)

