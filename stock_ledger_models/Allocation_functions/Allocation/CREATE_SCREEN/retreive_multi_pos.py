
import pandas as pd
import yaml
from ..GLOBAL_FILES.get_vdate import get_vdate

def retreive_multi_po(conn,
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
                      O_status):

    L_func_name = "retreive_multi_po"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/retreive_multi_pos_queries.yaml') as fh:
            queries      = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_create_tble = queries['retreive_multi_pos']['Q_create_tble']
            Q_del_gtt = queries['retreive_multi_pos']['Q_del_gtt']
            Q_sel_multi_po = queries['retreive_multi_pos']['Q_sel_multi_po']
            Q_ins_temp = queries['retreive_multi_pos']['Q_ins_temp']
            Q_fetch = queries['retreive_multi_pos']['Q_fetch']
            
            mycursor = conn.cursor()

            O_status = 1
            mycursor.execute(Q_create_tble)

            emp_list = []
            mycursor.execute(Q_del_gtt,(I_alloc_no,))
            print("gtt table")

            O_status = 2
            L_vdate = get_vdate(conn)
            print("before query ",L_vdate)

            O_status = 3
            df_sel_recd = pd.read_sql(Q_sel_multi_po,conn,params=(I_item_list_no,I_item_list_no,I_item_list_no,I_item_list_no,I_location,
                                                                  I_hier1,I_hier2,I_hier3,I_supplier_site,I_supplier,I_item_p,I_sku,
                                                                  I_diff_id,I_diff_id,I_diff_id,I_diff_id,I_po_type,I_not_after_date_from,L_vdate,I_not_after_date_from,
                                                                  L_vdate,I_not_after_date_to,L_vdate,I_not_after_date_to,L_vdate,
                                                                  I_eisd_from,L_vdate,I_eisd_from,L_vdate,I_eisd_to,L_vdate,I_eisd_to,L_vdate,))
            if len(df_sel_recd)>0:
            
                for i in range(len(df_sel_recd)):
                    O_status = 4
                    L_po_no  = df_sel_recd.loc[i,"po_no"]
                    L_loc    = df_sel_recd.loc[i,"location"]
                    L_desc   = df_sel_recd.loc[i,"comment_desc"]
                    L_date   = df_sel_recd.loc[i,"instock_date"]
                    L_sup_id = df_sel_recd.loc[i,"supplier_id"]

                    mycursor.execute(Q_ins_temp,(I_alloc_no,L_po_no,L_desc,L_loc,L_date,L_sup_id,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            
            else:
                #return emp_list,False  #send emplty list [] and keep false
                return emp_list,"Incorrect data is in basic tables."

            df_fetch_rec = pd.read_sql(Q_fetch, conn)
            if len(df_sel_recd) >0:
                print("df_fetch_rec::",df_fetch_rec)
                conn.cursor().close()
                return df_fetch_rec,""
            else:
                conn.cursor().close()
                return emp_list,"No data found."  

    except Exception as error:
        err_return=""
        if O_status==1:
            #print(L_func_name,":",O_status,":","Exception occured while creating temporary table : ", error)
            err_return = L_func_name+":"+str(O_status)+": Exception occured while creating temporary table : "+ str(error)
        elif O_status==2:
            #print(L_func_name,":",O_status,":","Exception occured while fetching vdate: ", error) 
            err_return = L_func_name+":"+str(O_status)+": Exception occured while fetching vdate: "+ str(error)
        elif O_status==3:
            #print(L_func_name,":",O_status,":","Exception occured while executing the Q_sel_multi_po query: ", error) 
            err_return = L_func_name+":"+str(O_status)+": Exception occured while executing the Q_sel_multi_po query: "+ str(error)
        elif O_status==4:
            #print(L_func_name,":",O_status,":","Exception occured while running the for loop for df_sel_recd dataframe: ", error)
            err_return = L_func_name+":"+str(O_status)+": Exception while running the for loop for df_sel_recd dataframe: "+ str(error)
        else: 
            #print("Exception occured in: ",L_func_name.format(error),error)
            err_return = L_func_name+": Exception occured in: "+ str(error)
        print(err_return)
        conn.rollback()
        return emp_list,err_return