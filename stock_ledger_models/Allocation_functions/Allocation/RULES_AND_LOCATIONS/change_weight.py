from unittest import result
from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
from datetime import date, timedelta, datetime
import pandas as pd
import numpy as np
import yaml
import math as mt

##################################################################################
#Created By - Priyanshu/Krishna                                                  #
#File Name - change_weight.py                                                    #
#Purpose - Contains all the functions for change weight functionalities          #
##################################################################################

#--------------------------------------------------------------
# Function to setup eow (CAL_TO_454) 
#--------------------------------------------------------------
def seed_ly_eow (conn
                ,I_dd
                ,I_mm
                ,I_yyyy               
                ,O_status):
    L_func_name ="seed_ly_eow"
    O_status = 0
    emp_list = list()
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/change_weight_queries.yaml') as fh:
            queries       = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_align_input = queries['seed_ly_eow']['Q_align_input']
            Q_seed_eow    = queries['seed_ly_eow']['Q_seed_eow']
            Q_out_wk_454  = queries['seed_ly_eow']['Q_out_wk_454']
            Q_out_dd_454  = queries['seed_ly_eow']['Q_out_dd_454']

            mycursor = conn.cursor()
            #status
            O_status=1

            df_input = pd.read_sql(Q_align_input,conn,params=(I_yyyy,I_mm,I_dd))
            L_cal_date1 = df_input.cal_date[0]
            L_cal_date = datetime.strptime(L_cal_date1, '%Y-%m-%d').date()

            #status
            O_status=2

            #Q_seed_eow
            df_seed_eow = pd.read_sql(Q_seed_eow,conn,params=(L_cal_date,L_cal_date))
            if len(df_seed_eow)==0:
                print("Calendar is out of range. Please setup your calendar table")
                return emp_list,L_func_name+":"+str(O_status)+": "+"Calendar is out of range. Please setup your calendar table"

            L_days = df_seed_eow.dd[0]
            O_mm   = df_seed_eow.mm[0]
            O_yyyy = df_seed_eow.yyyy[0]

            #status
            O_status=3
            np_int = np.int64(L_days)
            L_days = np_int.item()
            #Q_out_wk_454
            df_wk_454 = pd.read_sql(Q_out_wk_454,conn,params=(L_days,))
            O_wk = df_wk_454.wk_454[0]
            np_int = np.int64(O_wk)
            O_wk = np_int.item()
            if O_wk>5:
                #status
                O_status=4
                return emp_list,L_func_name+":"+str(O_status)+": "+"NO_DATA_FOUND." #False

            #status
            O_status=4

            #Q_out_dd_454
            df_dd_454 = pd.read_sql(Q_out_dd_454,conn,params=(L_days,O_wk))
            O_dd = df_dd_454.dd_454[0]

            #status
            O_status=5

            O_date_list =list()
            O_date_list.append(O_dd)
            O_date_list.append(O_mm)
            O_date_list.append(O_yyyy)
            O_date_list.append(O_wk)

            conn.cursor().close()
            return O_date_list, ""

    except Exception as argument:
        #status
        print(L_func_name,O_status, ": CALENDAR table out of range, no data found see your DBA")        
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(argument)
        conn.rollback()
        conn.cursor().close()
        return emp_list,err_return #False

#--------------------------------------------------------------
# Function to fetch eow (C454_TO_CAL) 
#--------------------------------------------------------------
def fetch_ly_eow (conn
                ,I_dd
                ,I_mm
                ,I_yyyy
                ,I_wk
                ,O_status):
    L_func_name ="seed_ly_eow"
    emp_list = list()
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/change_weight_queries.yaml') as fh:
            queries         = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_date_add      = queries['fetch_ly_eow']['Q_date_add']
            Q_fetch_eow     = queries['fetch_ly_eow']['Q_fetch_eow']
            #Q_format_output = queries['fetch_ly_eow']['Q_format_output']
            
            df_date_add = pd.read_sql(Q_date_add,conn,params=(I_wk,I_dd))
            L_date_add  = df_date_add.date_add[0]
            #status
            O_status = 1

            np_int = np.float64(L_date_add)
            L_date_add = np_int.item()
            np_int = np.float64(I_yyyy)
            I_yyyy = np_int.item()
            np_int = np.int64(I_mm)
            I_mm = np_int.item()            

            #Q_fetch_eow
            df_fetch_eow = pd.read_sql(Q_fetch_eow,conn,params=(L_date_add,I_yyyy,I_mm))
            #status
            O_status = 2

            L_cal_date    = df_fetch_eow.cal_date[0]
            L_no_of_weeks = df_fetch_eow.no_of_weeks[0]

            #Q_format_output
            #df_format_output = pd.read_sql(Q_format_output,conn,params=(L_cal_date,L_cal_date,L_cal_date))
            #status
            O_status = 3

            O_dd   = L_cal_date.strftime("%d")
            O_mm   = L_cal_date.strftime("%m")
            O_yyyy = L_cal_date.strftime("%Y")

            if L_no_of_weeks<I_wk:
                return emp_list,L_func_name+":"+str(O_status)+": "+"fetch_ly_eow function is failed." #false
            #status
            O_status = 4

            O_date_list =list()
            O_date_list.append(O_dd)
            O_date_list.append(O_mm)
            O_date_list.append(O_yyyy)

            return O_date_list,""

    except Exception as argument:
        #status 
        print(L_func_name,O_status)
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+":"+str(O_status)+": "+"Exception occured :"+ str(argument)
        conn.rollback()
        return emp_list,err_return #False
   
#--------------------------------------------------------------
# Function to populate eow (CAL_TO_454_LDOW) 
#--------------------------------------------------------------
def pop_ly_eow (conn
                ,I_dd
                ,I_mm
                ,I_yyyy
                ,O_status):
    L_func_name ="pop_ly_eow"
    O_status = 0
    emp_list = list()
    print("EXECUTING: ",L_func_name)
    try:
        O_status = 1
        F_seed_ly_eow, err_msg1 = seed_ly_eow(conn,I_dd,I_mm,I_yyyy,O_status)
        if len(F_seed_ly_eow)>0:
            L_day = 7
            L_month = F_seed_ly_eow[1]
            L_year = F_seed_ly_eow[2]
            L_week = F_seed_ly_eow[3]
            O_status = 2
            F_fetch_ly_eow, err_msg2 = fetch_ly_eow(conn,L_day,L_month,L_year,L_week,O_status)
            if len(F_fetch_ly_eow)>0:
                return F_fetch_ly_eow, ""
            else:
                return emp_list,err_msg2
        else:
            return emp_list,err_msg1
    except Exception as error:    
        err_return = ""
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while calling the seed_ly_eow: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while calling the seed_ly_eow :"+ str(error)
        elif O_status==2:
            print(L_func_name,":",O_status,":","Exception occured while calling the seed_ly_eow: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while calling the seed_ly_eow :"+ str(error)
        else:
            print("Exception occured in: ",O_status,L_func_name.format(error))
            err_return = L_func_name+": "+"Exception occured:"+ str(error)
        conn.rollback()
        return emp_list,err_return
#--------------------------------------------------------------
# Function to setup change weight 
#--------------------------------------------------------------
def load_change_weight_dates (conn
                              ,I_alloc
                              ,O_status):
    L_func_name ="load_change_weight_dates"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/change_weight_queries.yaml') as fh:
            queries           = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_fetch_rule      = queries['change_weight']['Q_fetch_rule']
            Q_del_rule_date   = queries['change_weight']['Q_del_rule_date']
            Q_ins_rule_date   = queries['change_weight']['Q_ins_rule_date']
            Q_date_loop_range = queries['change_weight']['Q_date_loop_range']
            Q_fetch_ty_eow    = queries['change_weight']['Q_fetch_ty_eow']
            Q_chk_ly_ty_ind   = queries['change_weight']['Q_chk_ly_ty_ind']
            Q_fetch_cur_date  = queries['change_weight']['Q_fetch_cur_date']
            #Q_format_output   = queries['change_weight']['Q_format_output']
            Q_ly_eow          = queries['change_weight']['Q_ly_eow']
            Q_sel_weight          = queries['change_weight']['Q_sel_weight']
            

            mycursor = conn.cursor()
            #status
            O_status=1

            df_fetch_rule     = pd.read_sql(Q_fetch_rule,conn,params=(I_alloc,))
            if len(df_fetch_rule) == 0:
                print("Rules need to be setup")
                return False,L_func_name+":"+str(O_status)+": "+"Rules need to be setup."

            L_weeks_this_year = df_fetch_rule.weeks_this_year[0]
            L_weeks_last_year = df_fetch_rule.weeks_last_year[0]
            L_start_date1     = df_fetch_rule.start_date1[0]
            L_start_date2     = df_fetch_rule.start_date2[0]
            L_end_date1       = df_fetch_rule.end_date1[0]
            L_end_date2       = df_fetch_rule.end_date2[0]
            L_rule_type       = df_fetch_rule.rule_type[0]

            #status
            O_status=2

            #Q_del_rule_date
            mycursor.execute(Q_del_rule_date,(I_alloc,))

            #status
            O_status=3

            L_eow_dates = list()

            if L_start_date1 !=None or L_start_date2 != None:

                #status
                O_status=4
                df_date_loop = pd.read_sql(Q_date_loop_range,conn,params=(I_alloc,))
                L_date1 = df_date_loop.date1[0]
                L_date2 = df_date_loop.date2[0]
                L_date1 = mt.trunc(L_date1)
                L_date2 = mt.trunc(L_date2)
                #status
                O_status=5
                if L_date1 != -1:
                    #status
                    O_status=6
                    L_start_loop = L_start_date1 - timedelta(7)

                    
                    for i in range(L_date1):
                        L_start_loop = L_start_loop + timedelta(7)
                        L_eow_dates.append(L_start_loop)

                        
                if L_date2 != -1:
                    #status
                    O_status=7
                    L_start_loop = L_start_date2 - timedelta(7)
                    
                    for i in range(L_date2):
                        L_start_loop = L_start_loop + timedelta(7)
                        L_eow_dates.append(L_start_loop)

            if L_weeks_this_year != None:
                L_weeks_this_year = mt.trunc(L_weeks_this_year)

                #status
                O_status=8

                df_ty_eow = pd.read_sql(Q_fetch_ty_eow,conn)
                L_ty_eow = df_ty_eow.last_eow_date[0]

                #status
                O_status=9

                if L_rule_type =='H':
                    L_start_loop = L_ty_eow + timedelta(7)
                    for i in range(L_weeks_this_year):
                        L_start_loop = L_start_loop - timedelta(7)
                        L_eow_dates.append(L_start_loop)
                else:
                    L_start_loop = L_ty_eow - timedelta(7)
                    for i in range(L_weeks_this_year):
                        L_start_loop = L_start_loop + timedelta(7)
                        L_eow_dates.append(L_start_loop)
            
            #Q_chk_ly_ty_ind
            df_ly_ty_ind = pd.read_sql(Q_chk_ly_ty_ind,conn,params=(I_alloc,))
            L_df_ly_ty_ind = df_ly_ty_ind.ty_ly_ind[0]

            #ty
            if L_weeks_last_year != None:
                L_weeks_last_year = mt.trunc(L_weeks_last_year)

                #status
                O_status=10
                #Q_fetch_cur_date
                df_cur_date = pd.read_sql(Q_fetch_cur_date,conn)
                L_last_year_date = df_cur_date.last_year[0]
                
                #Q_format_output
                #df_format_output = pd.read_sql(Q_format_output,conn,params=(L_last_year_date,L_last_year_date,L_last_year_date))
                I_dd   = L_last_year_date.strftime("%d")
                I_mm   = L_last_year_date.strftime("%m")
                I_yyyy = L_last_year_date.strftime("%Y")

                #status
                O_status=11

                F_pop_ly_eow,err_msg = pop_ly_eow (conn,I_dd,I_mm,I_yyyy,O_status)
                if len(F_pop_ly_eow)==0:
                    #status
                    O_status="pop_ly_eow"
                    print(O_status)
                    conn.rollback()
                    return False, err_msg

                O_yyyy = F_pop_ly_eow[2]
                O_mm   = F_pop_ly_eow[1]
                O_dd   = F_pop_ly_eow[0]

                #Q_ly_eow
                df_ly_eow = pd.read_sql(Q_ly_eow,conn,params=(O_yyyy,O_mm,O_dd))
                L_ly_eow = df_ly_eow.ly_eow[0]
                L_ly_eow = datetime.strptime(L_ly_eow, '%Y-%m-%d').date()

                if L_rule_type =='H':
                    L_start_loop = L_ly_eow + timedelta(7)
                    for i in range(L_weeks_last_year):
                        L_start_loop = L_start_loop - timedelta(7)
                        L_eow_dates.append(L_start_loop)
                else:
                    L_start_loop = L_ly_eow - timedelta(7)
                    for i in range(L_weeks_last_year):
                        L_start_loop = L_start_loop + timedelta(7)
                        L_eow_dates.append(L_start_loop)

            #getting unique values
            L_eow_dates= np.unique(L_eow_dates)

            df_sel_weight = pd.read_sql(Q_sel_weight,conn,params=(I_alloc,))
            weight_list=[]
            if len(df_sel_weight)>0:
                for val in df_sel_weight.values:
                    weight_list.append(val[0])

            #status
            O_status=12
            for i in range(len(L_eow_dates)):
                var1=1
                if len(weight_list)>0:
                    if len(weight_list)<= i:
                        var1 = 1
                    else:
                        var1 = weight_list[i]
                    mycursor.execute(Q_ins_rule_date,(I_alloc,L_eow_dates[i],L_df_ly_ty_ind,None,var1)) #changed to get updated weight 
                    print("Q_ins",O_status,"-","rows_affected: ",mycursor.rowcount)
                else:
                    mycursor.execute(Q_ins_rule_date,(I_alloc,L_eow_dates[i],L_df_ly_ty_ind,None,1))
                    print("Q_ins",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status=13

            if len(L_eow_dates)>0:
                result1,err_msg1 = pop_need_dates(conn,I_alloc,O_status)
                if result1 == False:
                    conn.rollback()
                    return False, err_msg1

            conn.commit()
            conn.cursor().close()
            return True, ""

    except Exception as error:
        err_return = ""
        if O_status==(1):
            print(L_func_name,":",O_status,":","Exception occured while processing fetching the data: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing fetching the data :"+ str(error)
        elif O_status==(2) and O_status==(3):
            print(L_func_name,":",O_status,":","Exception occured while processing Q_del_rule_date: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_del_rule_date :"+ str(error)
        elif O_status>=(3) and  O_status<=(6):
            print(L_func_name,":",O_status,":","Exception occured while processing Q_date_loop_range: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_date_loop_range :"+ str(error)
        elif O_status==(7):
            print(L_func_name,":",O_status,":","Exception occured while processing Q_fetch_ty_eow: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_fetch_ty_eow :"+ str(error)
        elif O_status==(8):
            print(L_func_name,":",O_status,":","Exception occured while processing Q_chk_ly_ty_ind: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_chk_ly_ty_ind :"+ str(error)
        elif O_status==(9):
            print(L_func_name,":",O_status,":","Exception occured while processing Q_fetch_cur_date: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_fetch_cur_date :"+ str(error)
        elif O_status==(10):
            print(L_func_name,":",O_status,":","Exception occured while calling the function pop_ly_eow: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while calling the function pop_ly_eow :"+ str(error)
        elif O_status==(11):
            print(L_func_name,":",O_status,":","Exception occured while processing Q_ins_rule_date: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_ins_rule_date :"+ str(error)
        elif O_status==(12):
            print(L_func_name,":",O_status,":","Exception occured while calling the pop_need_dates: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while calling the pop_need_dates :"+ str(error)
        else:
            print("Exception occured in: ",O_status,L_func_name,error)
            err_return = L_func_name+": "+"Exception occured :"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False,err_return



def pop_need_dates(conn,I_alloc,O_status):
    L_func_name ="pop_need_dates"
    O_status = 0
    try:     
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/change_weight_queries.yaml') as fh:
            queries         = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_del_calc_date = queries['pop_need_dates']['Q_del_calc_date']
            Q_ins_need_date = queries['pop_need_dates']['Q_ins_need_date']
            
            mycursor = conn.cursor()
            O_status=1
            mycursor.execute(Q_del_calc_date,(I_alloc,))
            print(O_status,"-","rows_affected: ",mycursor.rowcount)
            #Q_ins_need_date
            O_status=2
            mycursor.execute(Q_ins_need_date,(I_alloc,))
            print(O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()
            conn.cursor().close()
            return True, ""
    except Exception as error:
        err_return = ""
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while execute the Q_del_calc_date: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while execute the Q_del_calc_date :"+ str(error)
        elif O_status==2:
            print(L_func_name,":",O_status,":","Exception occured while execute the Q_ins_need_date: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while execute the Q_ins_need_date :"+ str(error)
        else:
            print("Exception occured in: ",L_func_name,error)
            err_return = L_func_name+": "+"Exception occured :"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False,err_return
#---------------------------------------------------------------------------------------------
def retrieve_rule_dates_weight(conn,I_alloc_no,O_status):
    L_func_name ="retrieve_rule_dates_weight"
    try: 
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/change_weight_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_del   = queries['retrieve_rule_dates_weight']['Q_del']
            Q_ins   = queries['retrieve_rule_dates_weight']['Q_ins']
            Q_fetch = queries['retrieve_rule_dates_weight']['Q_fetch']
            
            O_status=1
            mycursor=conn.cursor()
            I_get_mysql_conn = list()
            I_get_mysql_conn.append(0)

            mycursor.execute(Q_del,(I_alloc_no,))
            print("Q_del",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit() 

            O_status=2
                       
            mycursor.execute(Q_ins,(I_alloc_no,))
            print("Q_ins",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            O_status=3
            #mycursor.execute(Q_del,(I_alloc_no,))
            df_fetch=pd.read_sql(Q_fetch,conn,params=(I_alloc_no,))
            conn.cursor().close()
            return df_fetch,""
    except Exception as error:
        err_return = ""
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while execute the CONN: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while execute the CONN :"+ str(error)
        elif O_status==2:
             print(L_func_name,":",O_status,":","Exception occured while fetching the Q_del: ", error)
             err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while fetching the Q_del :"+ str(error)
        elif O_status==3:
             print(L_func_name,":",O_status,":","Exception occured while fetching the Q_ins: ", error)
             err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while fetching the Q_ins :"+ str(error)
        else:
            print("Exception occured in: ",O_status,L_func_name,error)
            err_return = L_func_name+": "+"Exception occured:"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False,err_return
#------------------------------------------------------------------------------------------
def load_rule_dates_weight(conn,
                           I_alloc_no,
                           O_status):
    L_func_name ="load_rule_dates_weight"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/change_weight_queries.yaml') as fh:
            queries        = yaml.load(fh, Loader=yaml.SafeLoader)
            L_merge_1      = queries['load_rule_dates_weight']['L_merge_1']
            C_get_rule_rec = queries['load_rule_dates_weight']['C_get_rule_rec']
            mycursor=conn.cursor()
            O_status=1
            mycursor.execute(L_merge_1,(I_alloc_no,))

            conn.commit()
            O_status=2
            if 1 != 0:
                L_changes_found = 1
            if L_changes_found == 1:
                mycursor.execute(C_get_rule_rec,(I_alloc_no,))
                O_status=3
                L_var=mycursor.fetchall()
                if L_var!=None:
                    O_status = list()
                    result1,err_msg1 = pop_need_dates(conn,I_alloc_no,O_status)
                    if result1 == False:
                        conn.rollback()
                        return False, err_msg1
            mycursor.close()
            return True, ""
	
    except Exception as error:
        err_return = ""
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while fecthing  the CONN: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while fecthing  the CONN :"+ str(error)
        elif O_status==2:
            print(L_func_name,":",O_status,":","Exception occured while fetching the Q_del: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while fetching the Q_del :"+ str(error)
        elif O_status==3:
            print(L_func_name,":",O_status,":","Exception occured while fetching the Q_ins: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while fetching the Q_ins :"+ str(error)
        else:
            print("Exception occured in: ",O_status,L_func_name,error)
            err_return = L_func_name+": "+"Exception occured:"+ str(error)
        conn.rollback()
        return False,err_return