
import pandas as pd
import yaml


def retreive_wisummary_details(conn,
                               I_search,
                               O_status): 
    L_func_name = "retreive_wisummary_details"
    no_data = list()
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_whatif_summary_queries.yaml') as ws:
            queries            = yaml.load(ws,Loader=yaml.SafeLoader)
            Q_chck_wis_data    = queries['func_retreive_dtls']['Q_chck_wis_data']
            Q_crt_hdr_tmp_tbl  = queries['func_retreive_dtls']['Q_crt_hdr_tmp_tbl']
            Q_crt_dtl_tmp_tbl  = queries['func_retreive_dtls']['Q_crt_dtl_tmp_tbl']
            Q_chck_data        = queries['func_retreive_dtls']['Q_chck_data']
            Q_chck_cnt         = queries['func_retreive_dtls']['Q_chck_cnt']
            Q_del_wis_data     = queries['func_retreive_dtls']['Q_del_wis_data']
            Q_ins_wis_hdr      = queries['func_retreive_dtls']['Q_ins_wis_hdr']
            Q_ins_wis_dtl      = queries['func_retreive_dtls']['Q_ins_wis_dtl']
            Q_ins_dtl          = queries['func_retreive_dtls']['Q_ins_dtl']
            Q_ins_hdr          = queries['func_retreive_dtls']['Q_ins_hdr']
            Q_wis_hdr_data     = queries['func_retreive_dtls']['Q_wis_hdr_data']
            Q_wis_dtl_data     = queries['func_retreive_dtls']['Q_wis_dtl_data']

            mycursor = conn.cursor()

            I_alloc_no = I_search["I_alloc_no"]
            I_po_type  = I_search["I_po_type"]
            I_multi_wh = I_search["I_multi_wh"]
            
            df_chck_wis_data = pd.read_sql(Q_chck_wis_data,conn,params=(I_po_type,I_alloc_no))
            if len(df_chck_wis_data) > 0:
                L_chck_wis_po_type = df_chck_wis_data.po_type[0]
                L_chck_wis_po_type1 = df_chck_wis_data.po_type1[0]
                if L_chck_wis_po_type != L_chck_wis_po_type1:
                    print("PO type cannot be changed")
                    return no_data
            else:
                mycursor.execute(Q_crt_hdr_tmp_tbl)
                mycursor.execute(Q_crt_dtl_tmp_tbl)

                df_chck_cnt = pd.read_sql(Q_chck_cnt,conn,params=(I_po_type,I_alloc_no))
                if (len(df_chck_cnt) != 0) and (df_chck_cnt.count_of_rec[0] != 0 and df_chck_cnt.count_of_order_rec[0] == 0):
                    mycursor.execute(Q_del_wis_data,I_alloc_no)
                
                df_chck_data = pd.read_sql(Q_chck_data,conn,params=(I_alloc_no,))
                if (len(df_chck_data) != 0) and (df_chck_data.chck[0] == 1):
                    mycursor.execute(Q_ins_wis_hdr,(I_multi_wh,I_po_type,I_alloc_no))
                    mycursor.execute(Q_ins_wis_dtl,(I_alloc_no,))
                else:
                    mycursor.execute(Q_ins_dtl,(I_po_type,I_po_type,I_po_type,I_po_type,I_po_type,I_po_type,I_po_type,I_alloc_no))

                    mycursor.execute(Q_ins_hdr,(I_multi_wh,I_po_type,I_alloc_no))
                    conn.commit()

            df_wis_hdr_data = pd.read_sql(Q_wis_hdr_data,conn,params=(I_alloc_no,))
            df_wis_dtl_data = pd.read_sql(Q_wis_dtl_data,conn,params=(I_alloc_no,))
            return df_wis_hdr_data,df_wis_dtl_data
    except Exception as error:
        print("Failed to update table record: {}".format(error)) 
        conn.cursor().close()
        return list(),list()
