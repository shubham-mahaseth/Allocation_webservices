import pandas as pd
import yaml

def retreive_alloc_head(conn,O_status,I_alloc_no):
    O_status = 0
    L_func_name = "retreive_alloc_head"

    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/retreive_alloc_head_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)

            Q_del_wi_alloc_dtl          = queries['retreive_alloc_head']['Q_del_wi_alloc_dtl']
            Q_del_wi_alloc_head         = queries['retreive_alloc_head']['Q_del_wi_alloc_head']
            Q_sel_rec_gtt               = queries['retreive_alloc_head']['Q_sel_rec_gtt']
            Q_ins_alloc_what_if_head    = queries['retreive_alloc_head']['Q_ins_alloc_what_if_head']
            Q_ins_alloc_what_if_detail  = queries['retreive_alloc_head']['Q_ins_alloc_what_if_detail']
            Q_ord_no_seq                = queries['retreive_alloc_head']['Q_ord_no_seq']
            Q_upd_ord_seq               = queries['retreive_alloc_head']['Q_upd_ord_seq']
            Q_fetch_recd                = "select * from alloc_what_if_detail where alloc_no = %s"
            


            O_status = 1
            mycursor = conn.cursor()
            mycursor.execute(Q_del_wi_alloc_dtl,(I_alloc_no,I_alloc_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            

            O_status = 2
            mycursor.execute(Q_del_wi_alloc_head,(I_alloc_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status = 3
            df_sel_rec_gtt = pd.read_sql(Q_sel_rec_gtt,conn,params=(I_alloc_no,))

            print("data of gtt",df_sel_rec_gtt)
            if len(df_sel_rec_gtt) > 0: 
                print("inside if loop")
                for index, row in df_sel_rec_gtt.iterrows():
                    L_ord_no   = f"{row['order_no']}"
                    print("checking orderno",L_ord_no)

                    mycursor.execute("LOCK TABLES po_ord_no_seq WRITE;")
                    df_ord_seq = pd.read_sql(Q_ord_no_seq,conn) 
                    L_ord_seq = int(df_ord_seq.po_order_seq[0])  
                    mycursor.execute(Q_upd_ord_seq,(L_ord_seq,))
                    mycursor.execute("UNLOCK TABLES;")
                    print("L_ord_seq....",L_ord_seq)

                    O_status = 4
                    mycursor.execute(Q_ins_alloc_what_if_head,(L_ord_seq,I_alloc_no,L_ord_no)) 
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                        
                    O_status = 5
                    mycursor.execute(Q_ins_alloc_what_if_detail,(L_ord_seq,I_alloc_no,L_ord_no))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            
            df_alloc_dtl_wi  =  pd.read_sql(Q_fetch_recd,conn,params=(I_alloc_no,))


            print("L_ord_seq:::",L_ord_seq)
            conn.commit()
            conn.cursor().close()
            return True, ""


    except Exception as error:
        err_return = ""
        if O_status == 1:
            print(L_func_name,":",O_status,":","Exception occured while deleting records from alloc_what_if_detail: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting records from alloc_what_if_detail :"+ str(error)
        elif O_status == 2:
            print(L_func_name,":",O_status,":","Exception occured while deleting records from alloc_what_if_head: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting records from alloc_what_if_head :"+ str(error)
        elif O_status == 3:
            print(L_func_name,":",O_status,":","Exception occured while selecting order_no from aso_alc_whatif_summary_gtt: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while selecting order_no from aso_alc_whatif_summary_gtt :"+ str(error)
        elif O_status==4 :
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting data into alloc_what_if_head table :"+ str(error)
            print(L_func_name,":",O_status,":","Exception occured while inserting data into alloc_what_if_head table: ", error)
        elif O_status==5 :
            print(L_func_name,":",O_status,":","Exception occured while inserting data into alloc_what_if_detail table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting data into alloc_what_if_detail table :"+ str(error)
        else:
            print("Exception occured in: ",L_func_name,error)
            err_return = L_func_name+": "+"Exception occured :"+ str(error)
        conn.rollback()
        return False,err_return