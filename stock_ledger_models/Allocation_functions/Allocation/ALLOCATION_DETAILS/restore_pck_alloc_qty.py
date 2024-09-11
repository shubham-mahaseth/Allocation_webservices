import yaml



def restore_pck_alloc_qty(conn,O_status,I_alloc_no,I_wh):  
    L_func_name = "restore_pck_alloc_qty"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/alloc_pack_comp_queries.yaml') as fh:
            queries     = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_upd       = queries['restore_pck_alloc_qty']['Q_upd']

            O_status = 1
            mycursor=conn.cursor()
            mycursor.execute(Q_upd,(I_alloc_no,I_wh))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            conn.commit()
            conn.cursor().close()
            return True, ""


    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while updating sku_calc_qty in alloc_calc_item_loc table:  "+ str(error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured:  "+ str(error)
        print(err_return)
        conn.rollback()
        return False, err_return


# when the user enter value for avail_qty it need to update and commit in backend table
# aso_alc_sku_details_gtt getting inserted in aso_alc_sku_details function
#if __name__ == "__main__":
#    I_alloc_no=1700
#    I_wh = 1
#    conn=None
#    O_status=None
#    daily_view = restore_pck_alloc_qty(conn,O_status,I_alloc_no,I_wh)  
#    print(daily_view); 






