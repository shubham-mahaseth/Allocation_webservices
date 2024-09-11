from ..ALLOCATION_STATUS.approve import approve 
from ..ALLOCATION_STATUS.reserve import reserve
from ..CALCULATION.calculate_validation import calculate_validation
from ..CALCULATION.calculate import calculate
from ..GLOBAL_FILES.get_vdate import get_vdate
from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
from ..SCHEDULE.setup_schedule_alloc import update_run_status
from ..CREATE_SCREEN.process_split_data import copy_alloc_schl
from ..ALLOCATION_SUMMARY.copy_alloc import copy_alloc_data
from datetime import date, timedelta
import yaml
import pandas as pd
from datetime import date
#----------------------------------------------------------
# FUNCTION create_alloc_from_parent
#----------------------------------------------------------

def create_alloc_from_parent (conn,I_thread_value):
    L_program               = 'create_alloc_from_parent'
    L_days                  = 0
    L_copy_success          = False
    L_validate_calc_success = False
    L_calculate_success     = False
    L_approve_success       = False
    L_run_status_success    = False
    V_date                  = get_vdate(conn)
    today                   = date.today()
    formatted_date          = today.strftime("%Y-%m-%d")
    I_alloc_id = None
    err_msg = ''
    #O_error_message=None
    try:
        print("I_THREAD_VALUE:",I_thread_value)
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/schedule_alloc_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            c_get_days   = queries['aso_update']['c_get_days']
            Q_get_schedule_parms = queries['aso_update']['Q_get_schedule_parms']
            Q_UPDATE_alloc_head_1 = queries['aso_update']['Q_UPDATE_alloc_head_1']
            Q_UPDATE_alloc_head_2 = queries['aso_update']['Q_UPDATE_alloc_head_2']
            Q_UPDATE_alloc_location = queries['aso_update']['Q_UPDATE_alloc_location']
            #status
            O_status = 1
            cursor = conn.cursor()
            #Cursor to get the days for schedlued allocation
            L_days = pd.read_sql(c_get_days,conn)
            print("DAYS :",L_days)
            L_val = L_days["DAYS"][0]
            L_in = (convert_numpy([L_val]))[0]
            print("SYSTEM_DATE :",V_date,"\n",L_days,type(L_days))
            #L_v_date = convert_numpy(V_date)
            #print("converted vdate ::::",L_v_date)
            #for i in range(len(str(L_in))):
                #status
            O_status = 2
            L_run_date = str(V_date + timedelta(days=L_in))
            print("L_RUN_DATE:",L_run_date)
                #Cursor to get the parameters for the passed alloc ID
                #L_input = L_days["DAYS"][0]
                #L_input = (L_input,I_thread_value)
                #L_input = convert_numpy(L_input)
                #L_in = (convert_numpy([L_val]))[0]
                
                
                #L_run_date = convert_numpy(L_run_date)
                #print("value of I_thread_value",I_thread_value,'\n',L_run_date,L_in,L_run_date,L_run_date,I_thread_value)
                #c_get_schedule_parms = pd.read_sql(Q_get_schedule_parms,conn, params=(L_input)) 
                #print("V_date,L_days,V_date,V_date,I_thread_value",'\n',Q_get_schedule_parms%(V_date,L_val,V_date,V_date,I_thread_value))
            c_get_schedule_parms = pd.read_sql(Q_get_schedule_parms,conn,params=(L_run_date,V_date,V_date,I_thread_value))
            
            print("C_GET_SCHEDULE_PARMS DFrame :",c_get_schedule_parms)
            for i in range(len(c_get_schedule_parms)):
                O_status = 3
                alloc_no = int(c_get_schedule_parms["alloc_no"][i])
                child_alloc_status = c_get_schedule_parms["child_alloc_status"][i]
                child_alloc_desc = c_get_schedule_parms["child_desc"][i]
                print("ALLOC_NO : ",alloc_no)
                create_id = c_get_schedule_parms["create_id"][i]
                #child_desc = str(c_get_schedule_parms["child_desc"][i])
                L_copy_success_no,err_msg = copy_alloc_data(conn,alloc_no,create_id,O_status)
                print("NEW CHILD ALLOC :",L_copy_success)

                #status
                O_status = 4
                if L_copy_success_no != None:
                    #L_input  = L_days["DAYS"][i]
                    L_child_desc = child_alloc_desc
                    #L_input_1 = (formatted_date,L_input, I_alloc_id)
                    #L_input_2 = (I_alloc_id, formatted_date, L_input)
                    #L_input_1 = convert_numpy(L_input_1)
                    #L_input_2 = convert_numpy(L_input_2)
                    cursor.execute(Q_UPDATE_alloc_head_1, (L_child_desc, create_id, alloc_no, L_copy_success_no,))
                    print ("\nQ_UPDATE_alloc_head_1 success", cursor.rowcount)

                    cursor.execute(Q_UPDATE_alloc_head_2, params=(V_date,L_in,L_copy_success_no))
                    print ("\nQ_UPDATE_alloc_head_2 success", cursor.rowcount)
                    #conn.commit() chandan
                    cursor.execute(Q_UPDATE_alloc_location, params=(L_copy_success_no,V_date,L_in))
                    print ("\nQ_UPDATE_alloc_location success", cursor.rowcount)
                    #status
                    O_status = 5
                    
                    L_new_alloc_no,err_msg = copy_alloc_schl(conn,alloc_no,L_copy_success_no,'Y',O_status)
                    if L_new_alloc_no == 0:
                        return False,L_program+":"+str(O_status)+": " + str(err_msg)
                    conn.commit()
                    print("L_new_alloc_no: ",L_new_alloc_no)

                    #status
                    O_status = 6                    
                    L_validate_calc_success,err_msg = calculate_validation( conn,
                                                                   L_copy_success_no,
                                                                   O_status)
                    print ("L_validate_calc_success:", L_validate_calc_success)
                    #status
                    O_status = 7
                    if L_validate_calc_success == True:                        
                        L_calculate_success,err_msg = calculate(conn,
                                                        L_copy_success_no,
                                                        O_status)
                        print("L_calculate_success: ",L_calculate_success)
                    else:
                        return False, L_program+":"+str(O_status)+": " + str(err_msg)
                    #status
                    O_status = 8
                    if (L_calculate_success == True and L_validate_calc_success == True) and child_alloc_status == 'APV':
                        L_approve_success,err_msg = approve(conn,
                                                    O_status,
                                                    L_copy_success_no)
                    #status
                    O_status = 9
                    if (L_calculate_success == True and L_validate_calc_success == True) and child_alloc_status == 'RSV':
                        L_approve_success,err_msg = reserve(conn,
                                                    O_status,
                                                    L_copy_success_no)
                        print("L_approve_success: ",L_approve_success) 
                    #status
                    O_status = 10
                    O_error_message=None
                    L_run_status_success,err_msg = update_run_status(conn,L_copy_success_no,O_error_message, alloc_no)
                    print("L_run_status_success: ",L_run_status_success)
            # conn.commit()
            return True,''
    except Exception as error:
        err_return     =  ""
        if O_status    == 1:
            err_return =  L_program+":"+str(O_status)+": Exception raised in Cursor to get the days for schedlued allocation: "+ str(error)
        if O_status    == 2:
            err_return =  L_program+":"+str(O_status)+": Exception raised in Cursor to get the parameters for the passed alloc ID:  "+ str(error)
        if O_status    == 3:
            err_return =  L_program+":"+str(O_status)+": Exception raised in sub function call copy_alloc: "+ str(error)
        if O_status    == 4:
            err_return =  L_program+":"+str(O_status)+": Exception raised in Updates to be performed after L_copy_success : "+ str(error)
        if O_status    == 5:
            err_return =  L_program+":"+str(O_status)+": Exception raised in sub-function call copy_alloc_data : "+ str(error)
        if O_status    == 6:
            err_return =  L_program+":"+str(O_status)+": Exception raised in sub-function call calculate_validation: "+ str(error)
        if O_status    == 7:
            err_return =  L_program+":"+str(O_status)+": Exception raised in sub-function call calculate: "+ str(error)
        if O_status    == 8:
            err_return =  L_program+":"+str(O_status)+": Exception raised in sub-function call approve: "+ str(error)
        if O_status    == 9:
            err_return =  L_program+":"+str(O_status)+": Exception raised in sub-function call reserve: "+ str(error)
        if O_status    == 10:
            err_return =  L_program+":"+str(O_status)+": Exception raised in sub-function call aso_update_run_status: "+ str(error)
        else:
            err_return = L_program+":"+str(O_status)+": Exception Occured: "+ str(error)
        print(err_return)
        conn.rollback()
        return False, err_return
