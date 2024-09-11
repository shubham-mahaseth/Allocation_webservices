import pandas as pd
from datetime import date
from ..WHATIF_SUMMARY.populate_popreview_temp  import populate_popreview_temp
from ..GLOBAL_FILES.get_vdate import get_vdate
import yaml
from ..ALLOCATION_SUMMARY.copy_alloc import copy_alloc_data




def create_whatif_po(conn,O_status,I_alloc_no,I_create_id):
    O_status = 0
    L_func_name = "create_whatif_po"
    emp_list = list()
    try:
        
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/create_whatif_po_queries.yaml') as fh:
            queries             = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_get_po_hdr        = queries['create_whatif_po']['Q_get_po_hdr']
            Q_get_po_dtl        = queries['create_whatif_po']['Q_get_po_dtl']
            Q_chck_supplier     = queries['create_whatif_po']['Q_chck_supplier']
            Q_chck_alloc        = queries['create_whatif_po']['Q_chck_alloc']
            Q_upd_wi_head       = queries['create_whatif_po']['Q_upd_wi_head']
            Q_upd_gtt           = queries['create_whatif_po']['Q_upd_gtt']
            Q_ins_ord_cfx_head  = queries['create_whatif_po']['Q_ins_ord_cfx_head']
            Q_upd_head          = queries['create_whatif_po']['Q_upd_head']
            Q_upd_gtt_1         = queries['create_whatif_po']['Q_upd_gtt_1']
            Q_ins_whatif_po_alloc  = queries['create_whatif_po']['Q_ins_whatif_po_alloc']
            Q_upd_head_1        = queries['create_whatif_po']['Q_upd_head_1']
            Q_sel_status        = queries['create_whatif_po']['Q_sel_status']
            Q_upd_status        = queries['create_whatif_po']['Q_upd_status']
            Q_new_po_num        = "SELECT * FROM alloc_whatif_summary_temp WHERE alloc_no = %s and order_no is not null; "


            O_status = 1
            mycursor = conn.cursor()
            L_get_vdate = get_vdate(conn)
            
            O_status = 2
            df_sel_rec = pd.read_sql(Q_get_po_hdr,conn,params=(L_get_vdate,I_alloc_no,))

            L_rib_orddtl_tbl = {}
            L_rib_orddtl_rec = {}
            L_rib_orddesc_rec = {}

            L_rib_orddtl_rec = {"RIB ID": [],
                                "ITEM": [],
                                "LOCATION": [],
                                "COST": [],
                                "REF_ITEM": [],
                                "ORIGINCOUNTRY": [],
                                "SUPPPACKSIZE": [],
                                "QTYORDERED": [],
                                "LOCTYPE": [],
                                "CANCELIND": [],
                                "RESTATEIND": []}

            L_rib_orddesc_rec = {"RIB ID":        [],
                                 "ORDER NUMBER":  [],
                                 "SUPPLIER SITE": [],
                                 "CURRENCY CODE": [],
                                 "TERMS":         [],
                                 "NOT BEFORE DATE": [],
                                 "NOT AFTER DATE": [],
                                 "OTB EOW DATE": [],
                                 "DEPT": [],
                                 "ORDER STATUS": [],
                                 "EXCHANGE RATE": [],
                                 "INCLUDE ON ORD IND": [],
                                 "WRITTEN RATE": [],
                                 #"dict": [],
                                 "ORIG_IND": [],
                                 "EDI INDICATOR": [],
                                 "PREMARK IND": [],
                                 "USER ID": [],
                                  "NONE":[]}

            if len(df_sel_rec) > 0: 
                O_status = 3
                print("checking data length",len(df_sel_rec))
                for index, row in df_sel_rec.iterrows():
                    L_not_bfr_date   = f"{row['not_before_date']}"
                    L_order_no       = f"{row['order_no']}"
                    L_order_type     = f"{row['order_type']}"
                    L_supplier       =  f"{row['supplier_site']}"
                    L_create_id      =  f"{row['create_id']}"
                    L_dummy_ord_no   =  f"{row['dummy_order_no']}"
                    
                    df_sel_rec_po_dtl = pd.read_sql(Q_get_po_dtl,conn,params=(L_order_no,I_alloc_no,))
                    if len(df_sel_rec_po_dtl) > 0:
                        O_status = 4
                        for index, row in df_sel_rec_po_dtl.iterrows():
                            L_item       = f"{row['item']}"
                            L_loc_type   = f"{row['location_type']}"
                            L_loc_id     =  f"{row['location_id']}"
                            L_ord_qty    =  f"{row['ord_qty']}"
                            L_org_cntry  =  f"{row['country_of_origin']}"

                            O_status = 5

                            L_rib_orddtl_rec["RIB ID"].append(0)
                            L_rib_orddtl_rec["ITEM"].append(L_item)
                            L_rib_orddtl_rec["LOCATION"].append(L_loc_id)
                            L_rib_orddtl_rec["COST"].append(None)
                            L_rib_orddtl_rec["REF_ITEM"].append(None)
                            L_rib_orddtl_rec["ORIGINCOUNTRY"].append(L_org_cntry)
                            L_rib_orddtl_rec["SUPPPACKSIZE"].append(None)
                            L_rib_orddtl_rec["QTYORDERED"].append(L_ord_qty)
                            L_rib_orddtl_rec["LOCTYPE"].append(L_loc_type)
                            L_rib_orddtl_rec["CANCELIND"].append(None)
                            L_rib_orddtl_rec["RESTATEIND"].append(None)

                            L_rib_orddtl_tbl = L_rib_orddtl_rec

                    O_status = 6
                    df_chck_supplier = pd.read_sql(Q_chck_supplier,conn,params=(L_supplier,))  
                    L_chck_sup = df_chck_supplier.alc_ship_window_days[0]
                    
                    O_status = 7
                    df_chck_alloc = pd.read_sql(Q_chck_alloc,conn)           
                    L_chck_alloc = df_chck_alloc.code[0]
                    

                    if len(L_rib_orddtl_tbl) > 0:
                        if L_chck_sup == None:
                            L_nad = L_not_bfr_date + L_chck_alloc 
                        else:
                            L_nad = None
                        if L_order_type == ('N/B','DSD'):
                            L_premark_ind = 'N'
                        else:
                            L_premark_ind = None

                            O_status = 8

                            L_rib_orddesc_rec["RIB ID"].append(0) 
                            L_rib_orddesc_rec["ORDER NUMBER"].append(L_order_no)
                            L_rib_orddesc_rec["SUPPLIER SITE"].append(L_supplier)
                            L_rib_orddesc_rec["CURRENCY CODE"].append(None)
                            L_rib_orddesc_rec["TERMS"].append(None)
                            L_rib_orddesc_rec["NOT BEFORE DATE"].append(L_not_bfr_date)
                            L_rib_orddesc_rec["NOT AFTER DATE"].append(L_nad)
                            L_rib_orddesc_rec["OTB EOW DATE"].append(None)
                            L_rib_orddesc_rec["DEPT"].append(None)
                            L_rib_orddesc_rec["ORDER STATUS"].append('W')
                            L_rib_orddesc_rec["EXCHANGE RATE"].append(None)
                            L_rib_orddesc_rec["INCLUDE ON ORD IND"].append(None)
                            L_rib_orddesc_rec["WRITTEN RATE"].append(L_get_vdate)
                            #L_rib_orddesc_rec["dict"].append(L_rib_orddtl_tbl) 
                            L_rib_orddesc_rec["ORIG_IND"].append('8')
                            L_rib_orddesc_rec["EDI INDICATOR"].append(None)
                            L_rib_orddesc_rec["PREMARK IND"].append(L_premark_ind)
                            L_rib_orddesc_rec["USER ID"].append(L_create_id)
                            L_rib_orddesc_rec["NONE"].append(None)

                            print(L_rib_orddesc_rec)
                            print(":::::::",L_rib_orddtl_tbl)
                            L_dict={**L_rib_orddesc_rec,**L_rib_orddtl_tbl}
                            
                            print("after concat")
                        
                            print(L_dict)
                            
                            #Start-code written to handle the exception"All arrays must be of the same length"
                            max_length = max(len(value) for value in L_dict.values()) 
                            L_dict = {key: value + [None] * (max_length - len(value)) for key, value in L_dict.items()} 
                            #End-code written to handle the exception"All arrays must be of the same length"
                            
                            df_dict = pd.DataFrame.from_dict(L_dict)
                            print(df_dict)
                            print("checking df value")
                            print(df_dict["LOCATION"])
                            
                            L_status_code = 'S' #declared variable is removed once consume function is added
                            #Lerr_message = None
                            #should call consume function and send the 'L_dict' dictionary #keep structure of calling function
                            #L_consume_status,Lconsum_result = consume(L_status_code,df_dict,RMS13.RMSSUB_XORDER.LP_cre_type) #consume function structure in added
                            
                            if L_status_code != 'S':
                                conn.rollback()
                                print("after conn.rollback")
                                
                                O_status = 9
                                mycursor.execute(Q_upd_wi_head,(L_order_no,I_alloc_no,))
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                mycursor.execute(Q_upd_gtt,(I_alloc_no,L_dummy_ord_no,))
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                            else:
                                O_status = 10
                                print("before L_order_type",L_order_type)
                                if L_order_type != 'CD': 
                                    mycursor.execute(Q_ins_ord_cfx_head,(L_order_no,))
                                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                                    
                                    mycursor.execute(Q_upd_head,(L_order_no,I_alloc_no,))
                                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                    mycursor.execute(Q_upd_gtt_1,(L_order_no,I_alloc_no,L_dummy_ord_no,))
                                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                    result1,err_msg1 = populate_popreview_temp(conn,O_status,I_alloc_no)

                                    if result1 == False:
                                        conn.rollback()
                                        return emp_list, err_msg1

                                else:
                                    O_status = 11
                                    print("not matched with L_order_type")
                                    Lerr_message = None
                                    #if Lerr_message == None: 
                                    P_order_no = L_order_no
                                    P_doc_type = 'F'
                                    #I_new_alloc_no =[] #removed no need to send new_alloc_no for copy alloc
                                    O_status = 11
                                    print("before calling copy alloc func")
                                    L_fun_copy_alloc,err_msg2 = copy_alloc_data(conn,I_alloc_no,I_create_id,O_status) 
                                    if len(str(L_fun_copy_alloc)) > 0:
                                        O_status = 12
                                        mycursor.execute(Q_ins_ord_cfx_head,(L_order_no,))
                                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                        O_status = 13
                                        mycursor.execute(Q_ins_whatif_po_alloc,(I_alloc_no,L_fun_copy_alloc,L_order_no,))
                                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                        mycursor.execute(Q_upd_head_1,(L_order_no,I_alloc_no,))
                                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                        mycursor.execute(Q_upd_gtt_1,(L_order_no,I_alloc_no,L_dummy_ord_no,))
                                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                                        result2,err_msg2 = populate_popreview_temp(conn,O_status,I_alloc_no)

                                        if result2 == False:
                                            conn.rollback()
                                            return emp_list,err_msg2
                                        else:
                                            conn.rollback()
                                    else:
                                        conn.rollback()

            df_status = pd.read_sql(Q_sel_status,conn,params=(I_alloc_no,))
            L_status = df_status["status"][0]

            if L_status == 'P':
                mycursor.execute(Q_upd_status,(I_alloc_no,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            df_new_po = pd.read_sql(Q_new_po_num,conn,params=(I_alloc_no,))
            #print("PO_NEW_NUMBER::: ",df_new_po.to_dict("records"))

            conn.commit()
            conn.cursor().close()
            return df_new_po, ""


    except Exception as error:
        err_return = ""
        if O_status == 1:
            print(L_func_name,":",O_status,":","Exception occured while selecting get vdate: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while selecting get vdate :"+ str(error)
        elif O_status == 2:
            print(L_func_name,":",O_status,":","Exception occured while selecting records from alloc_what_if_head : ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while selecting records from alloc_what_if_head :"+ str(error)
        elif O_status == 3:
            print(L_func_name,":",O_status,":","Exception occured while selecting order_no from aso_alc_whatif_summary_gtt: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while selecting order_no from aso_alc_whatif_summary_gtt :"+ str(error)
        elif O_status==4 :
            print(L_func_name,":",O_status,":","Exception occured while selecting records from detail table ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while selecting records from detail table :"+ str(error)
        elif O_status==5 :
            print(L_func_name,":",O_status,":","Exception occured while assigning values to dictionary: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while assigning values to dictionary :"+ str(error)
        elif O_status==6 :
            print(L_func_name,":",O_status,":","Exception occured while selecting the alc_ship_window_days  ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while selecting the alc_ship_window_days :"+ str(error)
        elif O_status==7 :
            print(L_func_name,":",O_status,":","Exception occured while checking code from code_detail table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while checking code from code_detail table :"+ str(error)
        elif O_status==8 :
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while while assigning values to dictionary :"+ str(error)
            print(L_func_name,":",O_status,":","Exception occured while while assigning values to dictionary: ", error)
        elif O_status==9 :
            print(L_func_name,":",O_status,":","Exception occured while updating data in alloc what_if table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while updating data in alloc what_if table :"+ str(error)
        elif O_status==10 :
            print(L_func_name,":",O_status,":","Exception occured while inserting the values into ordhead_cfa_ext: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting the values into ordhead_cfa_ext :"+ str(error)
        elif O_status==11 :
            print(L_func_name,":",O_status,":","Exception occured before calling copy_alloc function: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured before calling copy_alloc function :"+ str(error)
        elif O_status==12 :
            print(L_func_name,":",O_status,":","Exception occured while inserting the values into ordhead_cfa_ext: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting the values into ordhead_cfa_ext :"+ str(error)
        elif O_status==13 :
            print(L_func_name,":",O_status,":","Exception occured while inserting the values into alloc_whatif_po_alloc: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting the values into alloc_whatif_po_alloc :"+ str(error)
        else:
            print("Exception occured in: ",L_func_name,error)
            err_return = L_func_name+": "+"Exception occured :"+ str(error)
        conn.rollback()
        return emp_list,err_return