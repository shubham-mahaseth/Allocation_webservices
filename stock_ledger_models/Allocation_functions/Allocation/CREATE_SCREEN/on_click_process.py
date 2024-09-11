from ..INVENTORY_SETUP.update_alloc_ext import update_alloc
from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
import pandas as pd
import yaml

##################################################################################
#Created By - Priyanshu Pandey                                                   #
#File Name - on_click_process.py                                                 #
#Purpose - To process on click functionalities                                   #
##################################################################################


#--------------------------------------------------------------
# Function to populate allocation header
#--------------------------------------------------------------
def populate_header (conn,
                     I_alloc,
                     O_status):
    L_func_name ="populate_header"
    O_status = 0
    emp_list = list()
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/on_click_process_queries.yaml') as fh:
            queries      = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_fetch_head = queries['populate_header']['Q_fetch_head']

            #status
            O_status = 1 
            df_head = pd.read_sql(Q_fetch_head,conn,params=(I_alloc,))
            return df_head, ""

    except Exception as error:
        err_return = ""
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured fetching alloc header: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured fetching alloc header :"+ str(error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(error)
        return emp_list, err_return

#--------------------------------------------------------------
# Function to refresh all tables
#--------------------------------------------------------------
def refresh_grid (conn,
                  I_alloc,
                  O_status):
    L_func_name ="refresh_grid"
    O_status = 0
    print("EXECUTING: ",L_func_name)

    ####################################
    #Add new tables as when added in DB#
    ####################################

    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/on_click_process_queries.yaml') as fh:
            queries           = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_del_itm_srch    = queries['refresh_grid']['Q_del_itm_srch']
            Q_del_itm_loc     = queries['refresh_grid']['Q_del_itm_loc']
            Q_del_itm_src     = queries['refresh_grid']['Q_del_itm_src']
            Q_del_allitmloc   = queries['refresh_grid']['Q_del_allitmloc']
            Q_del_weight      = queries['refresh_grid']['Q_del_weight']
            Q_del_calc_ql     = queries['refresh_grid']['Q_del_calc_ql']
            Q_del_like_itm    = queries['refresh_grid']['Q_del_like_itm']
            Q_del_itm_src_dtl = queries['refresh_grid']['Q_del_itm_src_dtl']
            Q_del_ql          = queries['refresh_grid']['Q_del_ql']
            Q_del_loc         = queries['refresh_grid']['Q_del_loc']
            Q_del_loc_grp     = queries['refresh_grid']['Q_del_loc_grp']
            Q_del_rule        = queries['refresh_grid']['Q_del_rule']
            Q_del_rule_dt     = queries['refresh_grid']['Q_del_rule_dt']
            Q_upd_head        = queries['refresh_grid']['Q_upd_head']

            mycursor = conn.cursor()

            #-------------
            #CREATE SCREEN
            #-------------
            #status
            O_status = 1            
            mycursor.execute(Q_del_itm_srch,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 2
            mycursor.execute(Q_del_itm_loc,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #----------
            #RL SCREEN
            #----------
            #status
            O_status = 3
            mycursor.execute(Q_del_itm_src,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 3
            mycursor.execute(Q_del_calc_ql,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 3
            mycursor.execute(Q_del_weight,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 3
            mycursor.execute(Q_del_allitmloc,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 4
            mycursor.execute(Q_del_loc,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 5
            mycursor.execute(Q_del_loc_grp,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 6
            mycursor.execute(Q_del_rule,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 7
            mycursor.execute(Q_del_rule_dt,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 8
            mycursor.execute(Q_upd_head,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #----------
            #LIKE-ITEM
            #----------
            #status
            O_status = 9
            mycursor.execute(Q_del_like_itm,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 10
            mycursor.execute(Q_del_itm_src_dtl,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #---
            #QL
            #---
            #status
            O_status = 11
            mycursor.execute(Q_del_ql,(I_alloc,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            I_input_data = list()
            result1,err_msg1 =  update_alloc(conn, O_status,I_alloc,None, None,'Y',I_input_data) 
            if result1 == False:
                #status
                O_status = 12
                print(O_status)
                conn.rollback()
                conn.cursor().close()
                return False, L_func_name+" : "+str(O_status)+": "+err_msg1
            conn.commit()
            conn.cursor().close()
            return True, ""
    except Exception as error:
        err_return = ""
        if O_status==1 or O_status==2:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while refreshing create screen data: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while refreshing create screen data: ", error)
        elif O_status>=3 and O_status<=8:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while refreshing rules & locations screen data: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while refreshing rules & locations screen data: ", error)
        elif O_status>=9 and O_status<=10:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while refreshing like-item screen data: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while refreshing like-item screen data: ", error)
        elif O_status==11:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while refreshing quantity limits screen data: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while refreshing quantity limits screen data: ", error)
        elif O_status==12:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while executing update_alloc function: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while executing update_alloc function: ", error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured: "+ str(error)
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

#--------------------------------------------------------------
# Function to populate error report
#--------------------------------------------------------------
def populate_error (conn,
                     I_alloc,
                     I_err1,
                     I_err2,
                     I_err3,
                     I_err4,
                     I_err5,
                     I_to_date,
                     I_from_date,
                     O_status):
    L_func_name           = "populate_error"
    O_status              = 0
    L_total_loc           = 0
    L_total_sku           = 0
    L_total_whs           = 0
    L_total_all_item_locs = 0
    emp_list              = list()
    df_err_report = pd.DataFrame()
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/on_click_process_queries.yaml') as fh:
            queries                 = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_cre_err_tbl           = queries['populate_error']['Q_cre_err_tbl']
            Q_del_tmp               = queries['populate_error']['Q_del_tmp']
            Q_chk_alloc             = queries['populate_error']['Q_chk_alloc']
            Q_ins_error             = queries['populate_error']['Q_ins_error']
            Q_inact_itm_loc_y       = queries['populate_error']['Q_inact_itm_loc_y']
            Q_inact_itm_loc_n       = queries['populate_error']['Q_inact_itm_loc_n']
            Q_sku_inact_wh          = queries['populate_error']['Q_sku_inact_wh']
            Q_wif_loop1             = queries['populate_error']['Q_wif_loop1']
            Q_cnt_tot_sku           = queries['populate_error']['Q_cnt_tot_sku']
            Q_cnt_tot_loc           = queries['populate_error']['Q_cnt_tot_loc']
            Q_cnt_tot_whs           = queries['populate_error']['Q_cnt_tot_whs']
            Q_loop_loc              = queries['populate_error']['Q_loop_loc']
            Q_inact_wif_loc         = queries['populate_error']['Q_inact_wif_loc']
            Q_tot_all_itm_loc       = queries['populate_error']['Q_tot_all_itm_loc']
            Q_upd_err_desc_all_item = queries['populate_error']['Q_upd_err_desc_all_item']
            Q_err_rec_loop          = queries['populate_error']['Q_err_rec_loop']
            Q_upd_err_rec           = queries['populate_error']['Q_upd_err_rec']
            Q_whs_rec_loop          = queries['populate_error']['Q_whs_rec_loop']
            Q_wif_itm_wh            = queries['populate_error']['Q_wif_itm_wh']
            Q_tot_all_itm_wh        = queries['populate_error']['Q_tot_all_itm_wh']
            Q_upd_all_wh_err        = queries['populate_error']['Q_upd_all_wh_err']
            Q_min_need              = queries['populate_error']['Q_min_need']
            Q_min_need_wif          = queries['populate_error']['Q_min_need_wif']
            Q_min_need_n            = queries['populate_error']['Q_min_need_n']
            Q_holdback_u            = queries['populate_error']['Q_holdback_u']
            Q_holdback_p            = queries['populate_error']['Q_holdback_p']
            Q_itm_loc_status        = queries['populate_error']['Q_itm_loc_status']
            Q_fetch_err             = queries['populate_error']['Q_fetch_err']
            Q_fetch_err1            = queries['populate_error']['Q_fetch_err1']
            Q_fetch_err2            = queries['populate_error']['Q_fetch_err2']

            mycursor = conn.cursor()

            #status
            O_status = 1
            #creating temp table 
            mycursor.execute(Q_cre_err_tbl)
            #status
            O_status = 2
            mycursor.execute(Q_del_tmp,(I_alloc,))

            df_head = pd.read_sql(Q_chk_alloc,conn,params=(I_alloc,))
            #status
            O_status = 3
            if len(df_head)>0:
                L_alloc_criteria   = df_head.alloc_criteria[0]
                L_alloc_level      = df_head.alloc_level[0]
                L_wh_store_rel_ind = df_head.wh_store_rel_ind[0]

                #status
                O_status = 4
                if L_alloc_criteria !='F':
                    if (L_alloc_level =='T' and L_wh_store_rel_ind !='N') or L_alloc_level == 'D':
                        #status
                        O_status = 5 
                        mycursor.execute(Q_ins_error,(I_alloc,I_alloc))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                    #status
                    O_status = 6 
                    if L_wh_store_rel_ind =='Y':
                        mycursor.execute(Q_inact_itm_loc_y,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    else:
                        #status
                        O_status = 7
                        mycursor.execute(Q_inact_itm_loc_n,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                if L_alloc_criteria in('T','W'):
                    #status
                    O_status = 8
                    mycursor.execute(Q_sku_inact_wh,(I_alloc,L_alloc_level))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                if L_alloc_criteria =='F':
                    #status
                    O_status = 9
                    df_loop_wi = pd.read_sql(Q_wif_loop1,conn,params=(I_alloc,))

                    #status
                    O_status = 10 
                    for i in range(len(df_loop_wi)):
                        L_item    = df_loop_wi["item"][i]
                        L_diff_id = df_loop_wi["diff_id"][i]
                        df_total_sku = pd.read_sql(Q_cnt_tot_sku,conn,params=(L_item,L_diff_id))

                        #status
                        O_status = 11
                        if len(df_total_sku)>0:
                            L_total_sku = df_total_sku["total_sku"][0]

                        #status
                        O_status = 12 
                        df_total_loc = pd.read_sql(Q_cnt_tot_loc,conn,params=(I_alloc,))
                        if len(df_total_sku)>0:
                            L_total_loc = df_total_loc["total_loc"][0]
                        
                        #status
                        O_status = 13
                        df_total_whs = pd.read_sql(Q_cnt_tot_whs,conn,params=(I_alloc,))
                        if len(df_total_whs)>0:
                            L_total_whs = df_total_whs["total_wh"][0]

                        #status
                        O_status = 14
                        df_loc_loop = pd.read_sql(Q_loop_loc,conn,params=(I_alloc,))
                        for x in range(len(df_loc_loop)):
                            if L_alloc_level =='T':
                                #status
                                O_status = 15
                                params = (I_alloc,L_item,df_loc_loop["loc"][x],L_total_sku)
                                params = convert_numpy(params)
                                mycursor.execute(Q_inact_wif_loc,params)
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                        #status
                        O_status = 16
                        df_all_item_locs = pd.read_sql(Q_tot_all_itm_loc,conn,params=(L_item,L_diff_id))
                        if len(df_all_item_locs)>0:
                            L_total_all_item_locs = df_all_item_locs["all_item_locs"][0]

                        #status
                        O_status = 17
                        if L_total_all_item_locs == L_total_loc:
                            if L_alloc_level =='T':
                                mycursor.execute(Q_upd_err_desc_all_item,(L_item,L_diff_id))
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                        #status
                        O_status = 18
                        df_err_rec_loop = pd.read_sql(Q_err_rec_loop,conn,params=(L_item,L_diff_id))
                        for x in range(len(df_err_rec_loop)):
                            if L_total_loc == df_err_rec_loop["loc"][x]:
                                #status
                                O_status = 19
                                mycursor.execute(Q_upd_err_rec,(df_err_rec_loop["tran_item"][x],))
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                        df_whs_loop = pd.read_sql(Q_whs_rec_loop,conn,params=(I_alloc,))
                        for x in range(len(df_whs_loop)):
                            if L_alloc_level =='T':
                                #status
                                O_status = 20
                                params = (I_alloc,L_item,df_whs_loop["wh"][x],L_total_sku)
                                params = convert_numpy(params)
                                mycursor.execute(Q_wif_itm_wh,params)
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                        #status
                        O_status = 21
                        df_all_itm_wh = pd.read_sql(Q_tot_all_itm_wh,conn,params=(L_item,L_diff_id))
                        if len(df_all_itm_wh)>0:
                            L_total_all_item_locs = df_all_itm_wh["loc"][0]

                        #status
                        O_status = 22
                        if L_total_all_item_locs == L_total_whs:
                            if L_alloc_level == 'T':
                                #status
                                O_status = 23
                                mycursor.execute(Q_upd_all_wh_err,(L_item,L_diff_id))
                                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                if L_wh_store_rel_ind =='Y':
                    #status
                    O_status = 23
                    mycursor.execute(Q_min_need,(I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                    #status
                    O_status = 24
                    mycursor.execute(Q_min_need_wif,(I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                else:
                    #status
                    O_status = 25
                    mycursor.execute(Q_min_need_n,(I_alloc,I_alloc))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                #status
                O_status = 26
                mycursor.execute(Q_holdback_u,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                #status
                O_status = 27
                mycursor.execute(Q_holdback_p,(I_alloc,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                if L_alloc_criteria !='F' and L_wh_store_rel_ind =='N':
                    if L_alloc_level =='T':
                        #status
                        O_status = 28
                        mycursor.execute(Q_itm_loc_status,(I_alloc,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 29
            #-------
            #output
            #-------
            if I_alloc !=None and (len(I_err1)==0 and len(I_err2)==0 and len(I_err3)==0):
                df_err_report = pd.read_sql(Q_fetch_err,conn,params=(I_alloc,))

            elif I_alloc !=None and (len(I_err1)>0 or len(I_err2)>0 or len(I_err3)>0 or len(I_err4)>0 or len(I_err5)>0):
                if len(I_err1)>0:
                    df_err_report1=pd.read_sql(Q_fetch_err1.format(I_alloc,I_err1),conn)
                    if len(df_err_report1)>0:
                        df_err_report = df_err_report.append(df_err_report1)

                if len(I_err2)>0:
                    df_err_report2=pd.read_sql(Q_fetch_err1.format(I_alloc,I_err2),conn)
                    if len(df_err_report2)>0:
                        df_err_report = df_err_report.append(df_err_report2)

                if len(I_err3)>0:
                    df_err_report3=pd.read_sql(Q_fetch_err1.format(I_alloc,I_err3),conn)
                    if len(df_err_report3)>0:
                        df_err_report = df_err_report.append(df_err_report3)

                if len(I_err4)>0:
                    df_err_report4=pd.read_sql(Q_fetch_err1.format(I_alloc,I_err4),conn)
                    if len(df_err_report4)>0:
                        df_err_report = df_err_report.append(df_err_report4)

                if len(I_err5)>0:
                    df_err_report5=pd.read_sql(Q_fetch_err1.format(I_alloc,I_err5),conn)
                    if len(df_err_report5)>0:
                        df_err_report = df_err_report.append(df_err_report5)

            elif I_alloc ==None and (len(I_err1)>0 or len(I_err2)>0 or len(I_err3)>0 or len(I_err4)>0 or len(I_err5)>0):
                if len(I_err1)>0:
                    df_err_report1=pd.read_sql(Q_fetch_err2.format(I_err1,I_err1,I_err1,I_from_date,I_to_date),conn)
                    if len(df_err_report1)>0:
                        df_err_report = df_err_report.append(df_err_report1)

                if len(I_err2)>0:
                    df_err_report2=pd.read_sql(Q_fetch_err2.format(I_err2,I_err2,I_err2,I_from_date,I_to_date),conn)
                    if len(df_err_report2)>0:
                        df_err_report = df_err_report.append(df_err_report2)

                if len(I_err3)>0:
                    df_err_report3=pd.read_sql(Q_fetch_err2.format(I_err3,I_err3,I_err3,I_from_date,I_to_date),conn)
                    if len(df_err_report3)>0:
                        df_err_report = df_err_report.append(df_err_report3)

                if len(I_err4)>0:
                    df_err_report4=pd.read_sql(Q_fetch_err2.format(I_err4,I_err4,I_err4,I_from_date,I_to_date),conn)
                    if len(df_err_report4)>0:
                        df_err_report = df_err_report.append(df_err_report4)
                
                if len(I_err5)>0:
                    df_err_report3=pd.read_sql(Q_fetch_err2.format(I_err5,I_err5,I_err5,I_from_date,I_to_date),conn)
                    if len(df_err_report5)>0:
                        df_err_report = df_err_report.append(df_err_report5) 
            conn.commit()
            conn.cursor().close()
            return df_err_report, ""

    except Exception as error:
        err_return= ''
        if O_status==1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while creating temp table: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while creating temp table: ", error)
        elif O_status==2:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while refreshing temp table data: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while refreshing temp table data: ", error)
        elif O_status==3:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while retrieving alloc_head data: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while retrieving alloc_head data: ", error)
        elif O_status>=4 and O_status<=28:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while generating validation errors: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while generating validation errors: ", error)
        elif O_status==29:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while retrieving error report: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while retrieving error report: ", error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured in: "+ str(error)
            #print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return emp_list,err_return

