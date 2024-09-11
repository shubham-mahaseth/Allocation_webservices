from ..WHATIF_SUMMARY.alloc_wisummary import upd_po_qty


def po_qty_change_wrapper(conn,I_alloc_no,I_wh_id,I_src_item
                          ,I_tran_item,I_diff_id,I_som_qty,po_qty):
    L_func_name="populate_popreview_wrapper"
    O_status =list()
    try:
        mycursor=conn.cursor()
        mycursor.execute("SET sql_mode = ''; ")
        print("wrapper")
        L_function,err_msg = upd_po_qty(conn,O_status,I_alloc_no,I_wh_id,I_src_item,I_tran_item,I_diff_id,I_som_qty,po_qty)
        return L_function,err_msg

    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured: "+ str(argument)
        return False,err_return


#if __name__ == "__main__":
#    I_alloc_no=4945 #2060
#    I_wh_id = 1
#    I_src_item = '127912971'
#    I_tran_item = '127912971'
#    I_diff_id = 'GREYORG'
#    I_som_qty = 2
#    po_qty = 17

#    daily_view = po_qty_change_wrapper(I_alloc_no,I_wh_id,I_src_item,I_tran_item,I_diff_id,I_som_qty,po_qty)
#    print(daily_view);