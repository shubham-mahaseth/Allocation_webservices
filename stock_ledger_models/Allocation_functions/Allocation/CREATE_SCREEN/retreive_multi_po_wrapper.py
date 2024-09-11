from ..CREATE_SCREEN.retreive_multi_pos import retreive_multi_po


def multi_po(conn,I_alloc_no,
                   I_hier1,
                   I_hier2,
                   I_hier3,
                   I_item_p,
                   I_diff_id,
                   I_item_list_no,
                   I_location,
                   I_not_after_date_from,
                   I_not_after_date_to,
                   I_eisd_from,
                   I_eisd_to,
                   I_supplier,
                   I_supplier_site,
                   I_po_type,
                   I_sku):
    L_func_name="multi_po"

    O_status =list()
    try:
        mycursor = conn.cursor()
        mycursor.execute("SET sql_mode = ''; ")
        L_func,err_msg = retreive_multi_po(conn,
                                    I_alloc_no,
                                    I_hier1,
                                    I_hier2,
                                    I_hier3,
                                    I_item_p,
                                    I_diff_id,
                                    I_item_list_no,
                                    I_location,
                                    I_not_after_date_from,
                                    I_not_after_date_to,
                                    I_eisd_from,
                                    I_eisd_to,
                                    I_supplier,
                                    I_supplier_site,
                                    I_po_type,
                                    I_sku,
                                    O_status)
        return L_func,err_msg
                
    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": Exception occured in: "+ str(argument)
        return [],err_return


#if __name__ == "__main__":
#    I_alloc_no = None
#    I_hier1 = None
#    I_hier2 = None
#    I_hier3 = None
#    I_item_p = None
#    I_diff_id = None
#    I_item_list_no = None
#    I_location = None
#    I_not_after_date_from = None
#    I_not_after_date_to = None
#    I_eisd_from = None
#    I_eisd_to = None
#    I_supplier = None
#    I_supplier_site = None
#    I_po_type = None
#    I_sku = None
#    O_status = None
#    daily_view = multi_po(I_alloc_no,
#                          I_hier1,
#                          I_hier2,
#                          I_hier3,
#                          I_item_p,
#                          I_diff_id,
#                          I_item_list_no,
#                          I_location,
#                          I_not_after_date_from,
#                          I_not_after_date_to,
#                          I_eisd_from,
#                          I_eisd_to,
#                          I_supplier,
#                          I_supplier_site,
#                          I_po_type,
#                          I_sku,
#                          O_status)  
#    print(daily_view);