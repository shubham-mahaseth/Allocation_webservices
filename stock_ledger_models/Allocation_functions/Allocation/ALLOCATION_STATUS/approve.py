import pandas as pd
from datetime import date
from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
from ..CREATE_ALLOC_FUNCTIONS.create_alloc import create_alloc
import yaml



def approve(conn,O_status,I_alloc_no): 
    L_func_name = "approve"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/approve_queries.yaml') as fh:
            queries         = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_c_get_approval_disable   = queries['approve']['Q_c_get_approval_disable']
            Q_get_status               = queries['approve']['Q_get_status']
            Q_get_release_date         = queries['approve']['Q_get_release_date']
            Q_alloc_criteria           = queries['approve']['Q_alloc_criteria']
            Q_get_last_inv_upd         = queries['approve']['Q_get_last_inv_upd']
            Q_get_last_inv_upd_po      = queries['approve']['Q_get_last_inv_upd_po']
            Q_get_last_inv_upd_asn     = queries['approve']['Q_get_last_inv_upd_asn']
            Q_get_last_inv_upd_tsf     = queries['approve']['Q_get_last_inv_upd_tsf']
            Q_get_item_sel_date        = queries['approve']['Q_get_item_sel_date']
            Q_get_max_upd_date         = queries['approve']['Q_get_max_upd_date']
            Q_chck_on_hand_diff        = queries['approve']['Q_chck_on_hand_diff']
            Q_upd_reclac_ind           =  queries['approve']['Q_upd_reclac_ind']
            Q_asn_chck_recv_date       =  queries['approve']['Q_asn_chck_recv_date']
            Q_get_days                 =  queries['approve']['Q_get_days']
            Q_tsf_chck_rel_date        =  queries['approve']['Q_tsf_chck_rel_date']
            Q_mark_alloc_torecal       =  queries['approve']['Q_mark_alloc_torecal']
            Q_chck_recalc_ind          =  queries['approve']['Q_chck_recalc_ind']
            Q_chck_sku_calc_qty        =  queries['approve']['Q_chck_sku_calc_qty']
            Q_del_records              =  queries['approve']['Q_del_records']
            Q_ins_alloc_item_loc       =  queries['approve']['Q_ins_alloc_item_loc']
            Q_sel_alloc_no             =  queries['approve']['Q_sel_alloc_no']
            Q_upd_status               =  queries['approve']['Q_upd_status']
            Q_del_itm_search           =  queries['approve']['Q_del_itm_search']
            Q_get_sysem_date           =  queries['approve']['Q_get_sysem_date']
            
            print(" Check APrrove 1802 :: inside approve ")
            mycursor=conn.cursor()
            df_chck_appr = pd.read_sql(Q_c_get_approval_disable,conn)

            O_status = 1
            L_chck_ind=df_chck_appr["code_desc"][0] 
            if L_chck_ind =='N':
                err_message = "Allocation approval is currently turned off."
                return False,err_message
            
            O_status = 2
            df_chck_status = pd.read_sql(Q_get_status,conn,params=(I_alloc_no,))
            L_chck_status=df_chck_status["status"][0]
            if L_chck_status == 'APV':
                err_message = "Allocation is already approved."
                return False,err_message
            

            O_status = 3
            df_chck_rel_date = pd.read_sql(Q_get_release_date,conn,params=(I_alloc_no,))
            L_rel_date=df_chck_rel_date["release_date"][0]

            df_get_system_date = pd.read_sql(Q_get_sysem_date,conn)
            L_system_date = df_get_system_date["SYSTEM_DATE"][0]
            
            if L_rel_date < L_system_date : #used SYSTEM_DATE column from calendar_variables table instand of vdate.
                err_message = "Release date cannot be greater than the system_date from calendar_variables table."
                return False,err_message

            O_status = 4
            mycursor.execute("SET sql_mode = ''; ")
            df_chck_alloc_critra = pd.read_sql(Q_alloc_criteria,conn,params=(I_alloc_no,))
            L_chck_wh=df_chck_alloc_critra["ALLOC_CRITERIA"][0]

            L_chck_1_wh  = 0
            L_chck_1_po  = 0
            L_chck_1_asn = 0
            L_chck_1_tsf = 0
            L_chck_diff_asgn = 0

            O_status = 5
            if L_chck_wh == None or L_chck_wh == 'W':
                df_get_inv_wh = pd.read_sql(Q_get_last_inv_upd,conn,params=(I_alloc_no,))
                if len(df_get_inv_wh) > 0:
                    L_chck_1_wh = df_get_inv_wh.record[0]

            
            elif L_chck_wh == 'P':
                df_get_inv_po = pd.read_sql(Q_get_last_inv_upd_po,conn,params=(I_alloc_no,))
                if len(df_get_inv_po) > 0:
                    L_chck_1_po = df_get_inv_po.record[0]
            
            elif L_chck_wh == 'A':
                df_get_inv_asn = pd.read_sql(Q_get_last_inv_upd_asn,conn,params=(I_alloc_no,))
                if len(df_get_inv_asn) > 0:
                    L_chck_1_asn = df_get_inv_asn.record[0]
            
            elif L_chck_wh == 'T':
                df_get_inv_tsf = pd.read_sql(Q_get_last_inv_upd_tsf,conn,params=(I_alloc_no,))
                if len(df_get_inv_tsf) > 0:
                    L_chck_1_tsf = df_get_inv_tsf.record[0]
            
            O_status = 6

            O_status = 7
            if L_chck_1_wh == 1 or L_chck_1_po == 1 or L_chck_1_asn == 1 or L_chck_1_tsf == 1:
                L_chck_diff_asgn = 1

            if L_chck_diff_asgn == 1:
                mycursor.execute(Q_upd_reclac_ind,(I_alloc_no,))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()
                err_message = "Inventory has been updated. Recalculation required."
                return False,err_message
            
            O_status = 8
            df_chck_asn_date = pd.read_sql(Q_asn_chck_recv_date,conn,params=(I_alloc_no,))
            L_asn_date=df_chck_asn_date

            O_status = 9
            if len(L_asn_date) != 1 and L_chck_1_asn == 'A':
                mycursor.execute(Q_upd_reclac_ind,(I_alloc_no,))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()
                err_message = "One or more ASN's are being received. Recalculation needed."
                return False,err_message

            O_status = 10
            df_days = pd.read_sql(Q_get_days,conn)
            L_chck_days = df_days["days"][0]

            df_chck_tsf_date = pd.read_sql(Q_tsf_chck_rel_date,conn,params=(I_alloc_no,int(L_chck_days,)))
            L_chck_tsf=df_chck_tsf_date
            if len(L_chck_tsf) == 1: 
                mycursor.execute(Q_upd_reclac_ind,(I_alloc_no,))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()
                err_message = "Transfer Allocation cannot be approved as system date is not Transfer Approval Date", L_chck_days ," Day(s)."
                return False,err_message


            O_status = 11
            df_alloc_torecal = pd.read_sql(Q_mark_alloc_torecal,conn,params=(I_alloc_no,I_alloc_no,))
            L_get_mrk_torecal = df_alloc_torecal["alloc_no"] 

            for i in range(len(L_get_mrk_torecal)):
                L_update = L_get_mrk_torecal[i]
                mycursor.execute(Q_upd_reclac_ind,(L_update,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                conn.commit()
            
            O_status = 12
            df_chck_recalc = pd.read_sql(Q_chck_recalc_ind,conn,params=(I_alloc_no,))
            L_chck_recalc_ind=df_chck_recalc["recalc_ind"][0]
            
            df_chck_sku = pd.read_sql(Q_chck_sku_calc_qty,conn,params=(I_alloc_no,))
            L_chck_sku_calc_qty = df_chck_sku["sku_calc_qty"][0]

            if L_chck_sku_calc_qty == 0 or L_chck_recalc_ind == 'Y':
                err_message = "Allocation is not ready to approve"
                return False,err_message

            O_status = 13
            mycursor.execute(Q_del_records,(I_alloc_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status = 14
            mycursor.execute(Q_ins_alloc_item_loc,(I_alloc_no,I_alloc_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status = 15
            L_call_fun = create_alloc(conn,O_status,I_alloc_no,'APV') #making I_status consistency with allocation status
            if L_call_fun == False:
                conn.rollback()
                return False,""

            O_status = 16
            mycursor.execute(Q_upd_status,(I_alloc_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            O_status = 17
            mycursor.execute(Q_del_itm_search,(I_alloc_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            

        conn.commit()
        conn.cursor().close()
        return True,""

    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured because of approve is turned off "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured because of approve is turned off ", error)
        elif O_status == 2:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured because allocation is already approved "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured because allocation is already approved ", error)
        elif O_status == 3:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured because of release date is older "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured because of release date is older ", error)
        elif O_status == 4:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while selecting alloc_criteria "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while selecting alloc_criteria ", error)
        elif O_status == 5:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while selectiong lastest inventory update "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while selectiong lastest inventory update ", error)
        elif O_status == 6:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while checking the lastest inventory date and last updated date "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while checking the lastest inventory date and last updated date ", error)
        elif O_status == 7:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while checking the value of lastest inventory udpate "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while checking the value of lastest inventory udpate ", error)
        elif O_status == 8:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while checking the asn received date "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while checking the asn received date ", error)
        elif O_status == 9:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while checking the tsf received date "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while checking the tsf received date ", error)
        elif O_status == 10:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while checking system_date for tsf"+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while checking system_date for tsf", error)
        elif O_status == 11:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while checking alloc to recalc "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while checking alloc to recalc ", error)
        elif O_status == 12:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while checking the recalc_ind and sum of sku_calc_qty"+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while checking the recalc_ind and sum of sku_calc_qty", error)
        elif O_status == 13:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while deleting the records from alloc_item_location "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while deleting the records from alloc_item_location ", error)
        elif O_status == 14:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while inserting the records in alloc_item_location "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while inserting the records in alloc_item_location ", error)
        elif O_status == 15:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured before calling the create_alloc function "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured before calling the create_alloc function ", error)
        elif O_status == 16:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while updating the status in alloc_head "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while updating the status in alloc_head ", error)
        elif O_status == 17:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured while deleting the records from alloc_itm_search_dtl "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while deleting the records from alloc_itm_search_dtl ", error)
        else:
            err_return = L_func_name+":"+str(O_status)+":"+"Exception occured: "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False,err_return


#if __name__ == "__main__":
#    I_alloc_no=332
#    O_status=None
#    daily_view = approve(O_status,I_alloc_no)  
#    print(daily_view);


