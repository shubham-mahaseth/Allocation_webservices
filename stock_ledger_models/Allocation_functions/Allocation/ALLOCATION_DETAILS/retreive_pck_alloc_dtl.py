from ..GLOBAL_FILES.null_handler import null_py
import mysql
import pandas as pd
import yaml


def retreive_pack_alloc_dtl(conn,I_alloc_no,O_status):
    
    L_fun = "retreive_pack_alloc_dtl"
    try:
        
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/retrv_pck_alloc_dtl_queries.yaml') as fh:
            queries                 = yaml.load(fh, Loader=yaml.SafeLoader)
            C_temp_tbl1             = queries['retreive_pack_alloc_dtl']['C_temp_tbl1']
            C_temp_tbl2             = queries['retreive_pack_alloc_dtl']['C_temp_tbl2']
            C_temp_tbl3             = queries['retreive_pack_alloc_dtl']['C_temp_tbl3']
            C_temp_tbl4             = queries['retreive_pack_alloc_dtl']['C_temp_tbl4']
            Q_sel_alloc_ctria       = queries['retreive_pack_alloc_dtl']['Q_sel_alloc_ctria']
            Q_sel_level             = queries['retreive_pack_alloc_dtl']['Q_sel_level']
            L_del_1                 = queries['retreive_pack_alloc_dtl']['L_del_1']
            L_del_2                 = queries['retreive_pack_alloc_dtl']['L_del_2']
            L_del_3                 = queries['retreive_pack_alloc_dtl']['L_del_3']
            L_ins_1                 = queries['retreive_pack_alloc_dtl']['L_ins_1']
            L_del_4                 = queries['retreive_pack_alloc_dtl']['L_del_4']
            Q_ins_dtl_tmp           = queries['retreive_pack_alloc_dtl']['Q_ins_dtl_tmp']
            C_temp_tbl5             = queries['retreive_pack_alloc_dtl']['C_temp_tbl5']
            L_mer_1                 = queries['retreive_pack_alloc_dtl']['L_mer_1']
            L_merg_2                = queries['retreive_pack_alloc_dtl']['L_merg_2']
            L_merg_3                = queries['retreive_pack_alloc_dtl']['L_merg_3']
            L_ins_2                 = queries['retreive_pack_alloc_dtl']['L_ins_2']
            L_temp_dup_tbl1         = queries['retreive_pack_alloc_dtl']['L_temp_dup_tbl1']
            L_merg_4                = queries['retreive_pack_alloc_dtl']['L_merg_4']
            Q_upd_ind               = queries['retreive_pack_alloc_dtl']['Q_upd_ind']
            Q_item_hdr_data         = queries['retreive_pack_alloc_dtl']['Q_item_hdr_data']
            Q_item_loc_data         = queries['retreive_pack_alloc_dtl']['Q_item_loc_data']
            Q_item_dtls_data        = queries['retreive_pack_alloc_dtl']['Q_item_dtls_data']
            Q_ins_pck_dtl           = queries['retreive_pack_alloc_dtl']['Q_ins_pck_dtl']
            Q_merg_ind              = queries['retreive_pack_alloc_dtl']['Q_merg_ind']
            Q_drop_temp_dup_tbl1    = queries['retreive_pack_alloc_dtl']['Q_drop_temp_dup_tbl1']

            mycursor = conn.cursor()
            #status
            O_status = 1 
            mycursor.execute(C_temp_tbl1)
            mycursor.execute(C_temp_tbl2)
            mycursor.execute(C_temp_tbl3)
            mycursor.execute(C_temp_tbl4)

            df_alloc_criteria = pd.read_sql(Q_sel_alloc_ctria,conn,params=(I_alloc_no,))
            L_alloc_criteria = df_alloc_criteria.alloc_criteria[0]
            print(1)

            df_alloc_level = pd.read_sql(Q_sel_level,conn,params=(I_alloc_no,))
            L_alloc_level = df_alloc_level.alloc_level[0]
            print(2)

            mycursor.execute(L_del_1,(I_alloc_no,))
            print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            print(3)

            O_status = 5
            mycursor.execute(L_del_2,(I_alloc_no,))
            print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            print(4)

            O_status = 6
            mycursor.execute(L_del_3,(I_alloc_no,))
            print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            print(5)

            if L_alloc_level == 'T':
                print("sku level")
                mycursor.execute(L_ins_1,(I_alloc_no,))
                print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                print(6)

                mycursor.execute(L_del_4,(I_alloc_no,))
                print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                print(7)

                mycursor.execute(Q_ins_dtl_tmp,(I_alloc_no,I_alloc_no,I_alloc_no,I_alloc_no,))
                print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                print(8)

                mycursor.execute(C_temp_tbl5,(I_alloc_no,))
                if null_py(L_alloc_criteria,'W') == 'F':
                    print("null_py if condition")
                    O_status = 10
                    mycursor.execute(L_mer_1,(I_alloc_no,I_alloc_no,))
                    print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)


                    O_status = 11
                    mycursor.execute(L_merg_2,(I_alloc_no,I_alloc_no,I_alloc_no,))
                    print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                else:
                    O_status = 12
                    mycursor.execute(L_merg_3,(I_alloc_no,))
                    print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                
                O_status = 13
                mycursor.execute(L_ins_2,(I_alloc_no,))
                print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                print("l_ins_2")

                O_status = 14
                mycursor.execute(Q_upd_ind,(I_alloc_no,))
                print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status = 15
                mycursor.execute(L_temp_dup_tbl1,(I_alloc_no,))
                mycursor.execute(L_merg_4,(I_alloc_no,))
                print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                mycursor.execute(Q_drop_temp_dup_tbl1)

                O_status = 16
                mycursor.execute(Q_ins_pck_dtl,(I_alloc_no,I_alloc_no,))
                print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status = 17
                mycursor.execute(Q_merg_ind,(I_alloc_no,))
                print(L_fun,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status = 18
                df_item_hdr_data  =  pd.read_sql(Q_item_hdr_data,conn,params=(I_alloc_no,))
                df_item_loc_data  =  pd.read_sql(Q_item_loc_data,conn,params=(I_alloc_no,))
                df_item_dtls_data =  pd.read_sql(Q_item_dtls_data,conn,params=(I_alloc_no,))

            #print("df_item_hdr_data::::",df_item_hdr_data.to_dict("records"))
            conn.commit()
            conn.cursor().close()
            return [df_item_hdr_data,df_item_loc_data,df_item_dtls_data],""


    except Exception as error:
        err_return = ""
        if O_status==1:
            err_return = L_fun+":"+str(O_status)+": Exception raised during creation of temporary tables: "+ str(error)
        elif O_status==2 or O_status==3:
            err_return = L_fun+":"+str(O_status)+": Exception raised during execution of cursor queries for alloc no: "+ str(I_alloc_no)+ ": "+str(error)
        elif O_status==4 or O_status==5 or O_status==6:
            err_return = L_fun+":"+str(O_status)+": Exception raised during execution of delete queries for alloc no: "+ str(I_alloc_no)+ ": "+ str(error)
        elif O_status==7:
            err_return = L_fun+":"+str(O_status)+": Exception raised during inserting data into alloc_item_loc_dtls_prv_temp table for alloc no: "+ str(I_alloc_no)+ ": "+ str(error)
        elif O_status==8:
            err_return = L_fun+":"+str(O_status)+": Exception raised during deleting data from alloc_item_loc_details_temp table for alloc no: "+ str(I_alloc_no)+ ": "+ str(error)
        elif O_status==9:
            err_return = L_fun+":"+str(O_status)+": Exception raised during inserting data into alloc_item_details_temp table for alloc no: "+ str(I_alloc_no)+ ": "+ str(error)
        elif O_status==10 or O_status==11 or O_status==12:
            err_return = L_fun+":"+str(O_status)+": Exception raised during execution of merge queries for alloc no: "+ str(I_alloc_no)+ ": "+ str(error)
        elif O_status==13:
            err_return = L_fun+":"+str(O_status)+": Exception raised during inserting data into alloc_item_header_temp and alloc_item_loc_details_temp table for alloc no: "+ str(I_alloc_no)+ ": "+ str(error)
        elif O_status==14:
            err_return = L_fun+":"+str(O_status)+": Exception raised during updating pack_ind in alloc_item_header_temp table "+ str(I_alloc_no)+ ": "+ str(error)
        elif O_status==15:
            err_return = L_fun+":"+str(O_status)+": Exception raised during updating pack_ind to 'Y': "+ str(I_alloc_no)+ ": "+ str(error)
        elif O_status==16:
            err_return = L_fun+":"+str(O_status)+": Exception raised during inserting data into alloc_item_loc_details_temp table for alloc no: "+ str(I_alloc_no)+ ": "+ str(error)
        elif O_status==17:
            err_return = L_fun+":"+str(O_status)+": Exception raised during updating sel_ind in alloc_item_loc_details_temp "+ str(I_alloc_no)+ ": "+ str(error)
        elif O_status==18:
            err_return = L_fun+":"+str(O_status)+": Exception raised during selecting data from alll temp tables: "+ str(I_alloc_no)+ ": "+ str(error)
        else:
            err_return = L_fun+":"+str(O_status)+": Exception Occured: "+ str(error)
        print(err_return)
        conn.rollback()
        return [],err_return
