import yaml



def upd_pack_alloc_qty(conn,O_status,I_alloc_no):  
    L_func_name = "restore_pck_alloc_qty"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/alloc_pack_comp_queries.yaml') as fh:
            queries         = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_del_gtt       = queries['upd_pack_alloc_qty']['Q_del_gtt']
            Q_ins           = queries['upd_pack_alloc_qty']['Q_ins']


        mycursor = conn.cursor()
        mycursor.execute(Q_del_gtt,(I_alloc_no,))

        mycursor.execute(Q_ins,(I_alloc_no,I_alloc_no,))
        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

        conn.cursor().close()
        return True, ""


    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while selecting the status from alloc_head table:  "+ str(error)
        elif O_status == 2:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while selecting the alloc_no from alloc_head table:  "+ str(error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured:  "+ str(error)
        print(err_return)
        conn.rollback()
        return False, err_return