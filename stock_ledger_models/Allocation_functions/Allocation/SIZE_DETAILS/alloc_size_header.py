import pandas as pd
import yaml

def retreive_alloc_size_header(conn,
                               I_alloc_no,
                               I_wh_id,
                               I_order_no,
                               I_source_item,
                               I_diff_id,
                               O_status):
    L_func_name = "retreive_alloc_size_header"
    no_data = list()
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/alloc_size_header.yaml') as fls: 
            queries            = yaml.load(fls,Loader=yaml.SafeLoader)
            Q_crt_tbl          = queries['func_retreive_alloc_size_hdr']['Q_crt_tbl']
            Q_chck_tbl         = queries['func_retreive_alloc_size_hdr']['Q_chck_tbl']
            Q_chck_ac          = queries['func_retreive_alloc_size_hdr']['Q_chck_ac']
            Q_del_hdr_tmp      = queries['func_retreive_alloc_size_hdr']['Q_del_hdr_tmp']
            Q_ins_hdr_tmp      = queries['func_retreive_alloc_size_hdr']['Q_ins_hdr_tmp']
            Q_tmp_hdr_data     = queries['func_retreive_alloc_size_hdr']['Q_tmp_hdr_data']
            Q_tmp_hdr_data_wif = queries['func_retreive_alloc_size_hdr']['Q_tmp_hdr_data_wif']

            mycursor = conn.cursor()

            df_chck_tbl = pd.read_sql(Q_chck_tbl,conn)
            L_chck_tbl  = df_chck_tbl.chk[0]
            if L_chck_tbl == 1:
                print("Please drop table alloc_item_loc_sku_hdr_tmp.")
                return no_data,"Please drop table alloc_item_loc_sku_hdr_tmp."
            else:
                O_status = 1
                mycursor.execute(Q_crt_tbl)

            mycursor.execute(Q_del_hdr_tmp)

            df_chck_ac = pd.read_sql(Q_chck_ac,conn,params=(I_alloc_no,))
            L_ac       = df_chck_ac.alloc_criteria[0]

            O_status = 2
            mycursor.execute(Q_ins_hdr_tmp,(L_ac,L_ac,L_ac,L_ac,I_alloc_no,))
            print(O_status,"-","rows_affected: ",mycursor.rowcount)

            if L_ac != 'F':
                df_tmp_hdr_data = pd.read_sql(Q_tmp_hdr_data,conn,params=(I_alloc_no,I_wh_id,I_order_no,I_source_item,I_diff_id,))
            else:
                df_tmp_hdr_data = pd.read_sql(Q_tmp_hdr_data_wif,conn,params=(I_alloc_no,I_order_no,I_source_item,I_diff_id,))

            conn.commit()
            conn.cursor().close()
            return df_tmp_hdr_data,""

    except Exception as error:
        print("Failed to update table record: {}".format(error))  
        err_return=""
        if O_status == 1:
            #print(L_func_name,":",O_status,":","Exception raised while creating alloc_item_loc_sku_hdr_tmp table",error)
            err_return = L_func_name+":"+str(O_status)+": Exception raised while creating alloc_item_loc_sku_hdr_tmp table: "+ str(error)
        elif O_status == 2:
            #print(L_func_name,":",O_status,":","Exception raised during insering records into alloc_item_loc_sku_hdr_tmp",error)
            err_return = L_func_name+":"+str(O_status)+": Exception raised during insering records into alloc_item_loc_sku_hdr_tmp: "+ str(error)
        else:
            #print(L_func_name,":",O_status,":","Exception occured: ",error)
            err_return = L_func_name+": Exception occured in: "+ str(error)
        conn.rollback()
        conn.cursor().close()
        return no_data,err_return