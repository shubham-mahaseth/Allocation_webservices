#--------------------------
#Created by - Phaneendra
#--------------------------
from datetime import date, timedelta
import yaml
import pandas as pd
from ..GLOBAL_FILES.get_vdate import get_vdate
from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
from ..ALLOCATION_STATUS.approve import approve 
from ..ALLOCATION_STATUS.reserve import reserve
from ..SCHEDULE.retreive_purge_alloc import retreive_purge_alloc

#----------------------------------------------------------
# FUNCTION aso_chk_dup_styl_clr
#----------------------------------------------------------

def chk_dup_styl_clr (conn, 
                          I_alloc_id,
                          L_alloc_level):
    L_program     = 'chk_dup_styl_clr'
    L_alloc_level = ''

    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/schedule_alloc_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            c_get_alloc_level = queries['aso_update']['c_get_alloc_level']
            Q_Update_alloc_itm_search_dtl = queries['aso_update']['Q_Update_alloc_itm_search_dtl']
            #status
            O_status = 1
            cursor = conn.cursor()
            Get_alloc_level = pd.read_sql(c_get_alloc_level,conn,params=( I_alloc_id,)) 
            L_alloc_level= Get_alloc_level["alloc_level"][0]
            
            O_status = 2
            L_SYSDATE = get_vdate(conn)
            cursor.execute (Q_Update_alloc_itm_search_dtl,(L_alloc_level,L_SYSDATE,L_SYSDATE,I_alloc_id,L_alloc_level))
            print("Update_success",cursor.rowcount)            

            conn.commit()
            conn.cursor().close()
            return True, ""
    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return =  L_program+":"+str(O_status)+": Exception raised in to get the alloc_level for the alloc_no FROM alloc_head: "+ str(error)
        elif O_status == 2:
            err_return =  L_program+":"+str(O_status)+": Exception raised in Cursor to Update alloc_itm_search_dtl: "+ str(error)
        else:
            err_return =  L_program+":"+str(O_status)+": Exception Occured: "+ str(error)
        print(err_return)
        conn.cursor().close()
        return False, err_return 

#----------------------------------------------------------
# FUNCTION aso_update_alloc_parms
#----------------------------------------------------------

def update_alloc_parms (conn, 
                        I_alloc_id, 
                        I_child_alloc_status, 
                        I_start_date,
                        I_end_date, 
                        I_frequency, 
                        I_days_of_week, 
                        I_next_schedule_run,
                        I_alloc_type, 
                        I_create_id, 
                        I_last_update_id):
    L_program = 'update_alloc_parms'
    try:       
        # Update or Insert records in the "alloc_schedule_params" table
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/schedule_alloc_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            Update_alloc_parms   = queries['aso_update']['Update_alloc_parms']
            #status
            O_status = 1
            #Cursor to Update/Insert allocation in alloc_schedule_params
            cursor = conn.cursor()
            cursor.execute(Update_alloc_parms,(I_alloc_id, 
                                               I_child_alloc_status, 
                                               I_start_date, 
                                               I_end_date, 
                                               I_frequency,
                                               I_days_of_week, 
                                               I_next_schedule_run, 
                                               I_create_id,
                                               I_last_update_id,
                                               I_child_alloc_status,
                                               I_start_date,
                                               I_end_date, 
                                               I_frequency,
                                               I_days_of_week, 
                                               I_next_schedule_run,
                                               I_last_update_id))
            if cursor.rowcount == 1 or cursor.rowcount > 1:
                print("Rows Updated/Inserted: ", cursor.rowcount)
        #status
        O_status = 2
        # Call the "aso_update_alloc_status" function
        check_UAS, err_msg = update_alloc_status( conn,
                                I_child_alloc_status,#'SCHD',
                                I_alloc_id )
        if check_UAS  == False:
            conn.rollback()
            conn.cursor().close()
            return False, L_program+":"+str(O_status)+": " + str(err_msg)
        conn.cursor().close()
        return True, ""

        
    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return =  L_program+":"+str(O_status)+": Exception raised in Cursor to Update/Insert allocation in alloc_schedule_params: "+ str(error)
        elif O_status == 2:
            err_return =  L_program+":"+str(O_status)+": Exception Occured in Call the aso_update_alloc_status function: "+ str(error)
        else:
            err_return =  L_program+":"+str(O_status)+": Exception Occured: "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return


#----------------------------------------------------------
# FUNCTION aso_update_alloc_status
#----------------------------------------------------------

def update_alloc_status(conn, 
                        I_status, 
                        I_alloc_id) :
    L_program = 'update_alloc_status'
    O_status  = 0
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/schedule_alloc_queries.yaml') as fh:
            queries        = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_alloc_status = queries['aso_update']['Q_alloc_status']
            cursor = conn.cursor()
            O_status=1
            cursor.execute(Q_alloc_status, (I_status, I_alloc_id,))
            conn.commit() 
            if cursor.rowcount > 0:
                print("Updated successfully")
                conn.cursor().close()
                return True, ""
            else:
                print("No data Updated")
                conn.rollback()
                conn.cursor().close()
                return False, L_program,":"+str(O_status)+ ": No data Updated "
    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return =  L_program+":"+str(O_status)+": Exception raised in Cursor to Update status in alloc_head: "+ str(error)
        else:
            err_return =  L_program+":"+str(O_status)+": Exception Occured: "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

#----------------------------------------------------------
# FUNCTION aso_update_alloc_type
#----------------------------------------------------------

def update_alloc_type(conn,
                      I_alloc_type, 
                      USER, 
                      I_alloc_id) :
    L_program = 'update_alloc_type';
    O_status  = 0
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/schedule_alloc_queries.yaml') as fh:
            queries      = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_alloc_type = queries['aso_update']['Q_alloc_type']
            cursor       = conn.cursor()

            O_status=1
            cursor.execute(Q_alloc_type, (I_alloc_type, USER, I_alloc_id,))
            print("Rows_affected: ",cursor.rowcount)
            conn.commit()
            conn.cursor().close()
            return True, ""
    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return =  L_program+":"+str(O_status)+": Exception raised while updating records in alloc_head: "+ str(error)
        else:
            err_return =  L_program+":"+str(O_status)+": Exception Occured: "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

#----------------------------------------------------------
# FUNCTION aso_update_run_status
#----------------------------------------------------------

def update_run_status(conn, 
                      I_alloc_id_child,
                      O_error_message, 
                      I_alloc_id_parent) :

    L_program = 'update_run_status'
    O_status=0
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/schedule_alloc_queries.yaml') as fh:
            queries      = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_run_status = queries['aso_update']['Q_run_status']

            cursor = conn.cursor()
            print("inside the function")
            O_status=1
            cursor.execute(Q_run_status, (I_alloc_id_child,O_error_message, I_alloc_id_parent,))
            conn.commit()
            conn.cursor().close()
            return True, ""
    except Exception as error:
        err_return =  L_program+":"+str(O_status)+": Exception Occured: "+ str(error)
        print (err_return)
        conn.cursor().close()
        return False, err_return

#----------------------------------------------------------
# FUNCTION aso_update_schedule_date
#----------------------------------------------------------
def update_schedule_date (conn):
    L_program    = 'update_schedule_date'
    L_days       = 0
    L_maxthread  = 10
    L_run_date1   = date.today()
    L_run_eow    = date.today()
    L_thread_val = 0
    O_status     = 0
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/schedule_alloc_queries.yaml') as fh:
            queries                       = yaml.load(fh, Loader=yaml.SafeLoader)
            c_get_days                    = queries['aso_update']['c_get_days']
            L_run_eow1                    = queries['aso_update']['L_run_eow1']
            query                         = queries['aso_update']['query']
            c_get_thread_alloc            = queries['aso_update']['c_get_thread_alloc']
            UPDATE_alloc_schedule_params1 = queries['aso_update']['UPDATE_alloc_schedule_params1']
            UPDATE_alloc_schedule_params2 = queries['aso_update']['UPDATE_alloc_schedule_params2']
            Update_null                   = queries['aso_update']['Update_null']
            
            O_status = 1
            cursor = conn.cursor()
            allocation_days = pd.read_sql(c_get_days, conn)
            for i in range(len(allocation_days)):
                L_sysdate = get_vdate(conn)

                DAYS = int (allocation_days["DAYS"][i])

                L_run_date = str(L_sysdate + timedelta(days=DAYS))
                print("L_run_date date:::",L_run_date)

            O_status = 2
            print("Test Eror::::",L_run_eow1,(L_run_date,L_run_date))
            L_run_eow1_query = f"""
                    SELECT DATE_ADD('{L_run_date}', INTERVAL (7 - DAYOFWEEK('{L_run_date}')) % 7 DAY) AS end_of_week
                    FROM dual;
                    """
            end_of_week = pd.read_sql (L_run_eow1_query, conn)
            # end_of_week = pd.read_sql (L_run_eow1, conn, params= (L_run_date,L_run_date))
            print("last saturday value;;;",end_of_week)
            
            for i in range(len(end_of_week)):
                EOW = str (end_of_week["end_of_week"][i]) 
                print("eow dates;;;;;;",EOW)

            O_status = 3
            Eligible_alloc_list = pd.read_sql (query, conn, params = (L_run_date,EOW,EOW,L_run_date,L_run_date,L_run_date,L_run_date)) #L_run_date,L_run_date,L_run_date
            print("dataframe::::::,,,",Eligible_alloc_list)
                
            L_alloc_no = Eligible_alloc_list["alloc_no"][0]
            
            L_eligible = Eligible_alloc_list["eligible"][0]
            print("value 1",L_eligible,L_alloc_no)
            
        
            O_status = 4
            #Cursor to Update all allocations with null thread id
            cursor.execute(Update_null)
        
            O_status = 5
            for i in range(len(end_of_week)):
                EOW = str (end_of_week["end_of_week"][i])
                print("EOW::::",EOW)

                print("befor for loop",Eligible_alloc_list)

                for i in range(len(Eligible_alloc_list)):
                    L_alloc = int(Eligible_alloc_list["alloc_no"][i])
                    L_eligible = int(Eligible_alloc_list["eligible"][i])
                    print(L_alloc,L_eligible)
                    if L_eligible == 1:
                        print("upto eligible executed")
                    #L_eligible_alloc_id  = int(Eligible_alloc_list["alloc_no"][i])
                    #L_eligible_rec_count = int (alloc_item_count["rec_count"][i])
                    
                        cursor.execute(UPDATE_alloc_schedule_params1,(L_run_date,EOW,L_alloc,L_eligible))
                        check_RPA, err_msg =retreive_purge_alloc (conn,L_alloc)
                        print()
                        if len(check_RPA) == 0 and len(err_msg)>0:
                            print(err_msg)
                            return False, L_program,":"+str(O_status)+ ": " + str(err_msg)
                        print("first for loop")
                    # Retrieve and purge the allocation
                    #function yet to be developed
                    #else aso.ASO_ALC_ALLOC_SQL.RETREIVE_PURGE_ALLOC (O_error_message, c_get_eligible_alloc_rec.alloc_id)
                    #    return False;

                #status
                O_status = 6
                print('c_get_thread_alloc::',c_get_thread_alloc)
                alloc_item_count = pd.read_sql (c_get_thread_alloc , conn)
                print('Hv')
                for i in range(len(alloc_item_count)):
                    print("2nd for loop condition")
                    L_alloc_id     = int (alloc_item_count["alloc_no"][i])
                    if L_maxthread == L_thread_val:
                       L_thread_val = 0
                    cursor.execute(UPDATE_alloc_schedule_params2,(L_thread_val,L_alloc_id))
                    print(" Update sucess/Failure",i," :::",cursor.rowcount)
                    L_thread_val = L_thread_val+1

            conn.commit()
            conn.cursor().close()
            return True, ''

    except Exception as error:        
        err_return = ""
        if O_status == 1:
            err_return =  L_program+":"+str(O_status)+": Exception raised in Cursor to get the days: "+ str(error)
        elif O_status == 2:
            err_return =  L_program+":"+str(O_status)+": Exception raised in Cursor to get the Date of Current or following Saturday: "+ str(error)
        elif O_status == 3:
            #err_return =  L_program+":"+str(O_status)+": Exception raised in Cursor to get the eligible allocations: "+ str(error)
            err_return = 'THERE ARE NO SCHEDULE ALLOCATIONS AVAILABLE TO PROCESS THE BATCH.'
        elif O_status == 4:
            err_return =  L_program+":"+str(O_status)+": Exception raised while in Cursor to Update all allocations with null thread id: "+ str(error)
        elif O_status == 5:
            err_return =  L_program+":"+str(O_status)+": Exception raised in Update the allocation schedule parameters: "+ str(error)
        elif O_status == 6:
            err_return =  L_program+":"+str(O_status)+": Exception raised in Update the allocation schedule parameters: "+ str(error)
        else:
            err_return =  L_program+":"+str(O_status)+": Exception Occured: "+ str(error)

        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return
#----------------------------------------------------------
# FUNCTION delete_nonavail_from_parent
#----------------------------------------------------------

def delete_nonavail_from_parent (conn, alloc_no):
    L_program    = 'delete_nonavail_from_parent'
    L_days       = 0
    L_maxthread  = 10
    L_run_date   = date.today()
    L_run_eow    = date.today()
    L_thread_val = 0
    O_status     = 0
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/schedule_alloc_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            c_get_item_source     = queries['aso_update']['c_get_item_source']
            c_get_qty_limit       = queries['aso_update']['c_get_qty_limit']
            c_get_allitemloc_temp = queries['aso_update']['c_get_allitemloc_temp']
            c_get_item_loc_search = queries['aso_update']['c_get_item_loc_search']
            Q_delete_nonavail_1   = queries['aso_update']['Q_delete_nonavail_1']
            Q_delete_nonavail_2   = queries['aso_update']['Q_delete_nonavail_2']
            Q_delete_nonavail_3   = queries['aso_update']['Q_delete_nonavail_3']
            Q_delete_nonavail_4   = queries['aso_update']['Q_delete_nonavail_4']
            Q_delete_nonavail_5   = queries['aso_update']['Q_delete_nonavail_5']
            cursor = conn.cursor()
            #status
            O_status = 1
            get_item_source = pd.read_sql(c_get_item_source, conn)
            for i in range(len(get_item_source)):
                # DELETE records in two tables
                var_1 = int(get_item_source["item_source_id"][i])
                cursor.execute(Q_delete_nonavail_1, (var_1,))
                print("No of records deleted1: ", cursor.rowcount)
                cursor.execute(Q_delete_nonavail_2, (var_1,))
                print("No of records deleted2: ", cursor.rowcount)
            #status
            O_status = 2
            get_qty_limit = pd.read_sql(c_get_qty_limit, conn)
            print("get_qty_limit: ", get_qty_limit)
            for i in range(len(get_qty_limit)):
                # DELETE records in alloc_quantity_limits
                var_3 = int(get_qty_limit["quantity_limits_id"][i])
                cursor.execute(Q_delete_nonavail_3, (var_3,))
                print("No of records deleted: ", cursor.rowcount)
            #status
            O_status = 3
            get_allitemloc_temp = pd.read_sql(c_get_allitemloc_temp, conn)
            for i in range(len(get_allitemloc_temp)):
                L_alloc_no      = int(get_allitemloc_temp["alloc_no"][i])
                L_tran_item     = int(get_allitemloc_temp["tran_item"][i])
                L_tran_diff1_id = str(get_allitemloc_temp["tran_diff1_id"][i])
                L_tran_diff2_id = str(get_allitemloc_temp["tran_diff2_id"][i])
                cursor.execute(Q_delete_nonavail_4, (L_alloc_no, L_tran_item, L_tran_diff1_id, L_tran_diff2_id))
                print("No of records deleted: ", cursor.rowcount)
            #status
            O_status = 4
            get_item_loc_search = pd.read_sql(c_get_item_loc_search, conn)
            for i in range(len(get_item_loc_search)):
                L_alloc_no   = int(get_item_loc_search["alloc_no"][i])
                L_item       = int(get_item_loc_search["item"][i])
                L_diff_idstr = (get_item_loc_search["diff_id"][i]) 
                L_loc        = int(get_item_loc_search["loc"][i])
                cursor.execute(Q_delete_nonavail_5, (L_alloc_no, L_item, L_diff_idstr, L_loc ))
                print("No of records deleted: ", cursor.rowcount)
            conn.commit()
            conn.cursor().close()
            return True, "" 
    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return =  L_program+":"+str(O_status)+": Exception Occured while fetching item_source_id:: "+ str(error)
        elif O_status == 2:
            err_return =  L_program+":"+str(O_status)+": Exception Occured while fetching quantity_limits_id: "+ str(error)
        elif O_status == 3:
            err_return =  L_program+":"+str(O_status)+": Exception occured while fetching data from alloc_calc_allitemloc: "+ str(error)
        elif O_status == 4:
            err_return =  L_program+":"+str(O_status)+": Exception occured while fetching data alloc_itm_search_dtl: "+ str(error)
        else:
            err_return =  L_program+":"+str(O_status)+": Exception Occured: "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

#----------------------------------------------------------
# FUNCTION delete_nonselected_from_parent
#----------------------------------------------------------

def delete_nonselected_from_parent (conn, I_alloc_id):
    L_program = 'delete_nonselected_from_parent'
    O_status  = 0
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/schedule_alloc_queries.yaml') as fh:
            queries                      = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_del_alloc_like_item_source = queries['aso_update']['Q_del_alloc_like_item_source']
            Q_del_alloc_item_source_dtl  = queries['aso_update']['Q_del_alloc_item_source_dtl']
            Q_del_alloc_quantity_limits  = queries['aso_update']['Q_del_alloc_quantity_limits']
            Q_del_alloc_calc_allitemloc  = queries['aso_update']['Q_del_alloc_calc_allitemloc']
            Q_del_alloc_itm_search_dtl   = queries['aso_update']['Q_del_alloc_itm_search_dtl']
            cursor = conn.cursor()
            #status
            O_status = 1
            # DELETE records in alloc_like_item_source
            cursor.execute(Q_del_alloc_like_item_source, (I_alloc_id,))
            print("No of records deleted: ", cursor.rowcount)
            #status
            O_status = 2
            # DELETE records in alloc_item_source_dtl
            cursor.execute(Q_del_alloc_item_source_dtl, (I_alloc_id,))
            print("No of records deleted: ", cursor.rowcount)
            #status
            O_status = 3
            # DELETE records in alloc_quantity_limits
            cursor.execute(Q_del_alloc_quantity_limits, (I_alloc_id,I_alloc_id))
            print("No of records deleted: ", cursor.rowcount)
            #status
            O_status = 4
            # DELETE records in alloc_calc_allitemloc
            cursor.execute(Q_del_alloc_calc_allitemloc, (I_alloc_id,))
            print("No of records deleted: ", cursor.rowcount)
            #status
            O_status = 5
            # DELETE records in alloc_itm_search_dtl
            cursor.execute(Q_del_alloc_itm_search_dtl, (I_alloc_id,))
            print("No of records deleted: ", cursor.rowcount)
            conn.commit()
            conn.cursor().close()
            return True, ""
    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return =  L_program+":"+str(O_status)+": Exception occured while deleting records in alloc_like_item_source: "+ str(error)
        elif O_status == 2:
            err_return =  L_program+":"+str(O_status)+": Exception occured while deleting records in alloc_item_source_dtl: "+ str(error)
        elif O_status == 3:
            err_return =  L_program+":"+str(O_status)+": Exception occured while deleting records in alloc_quantity_limits: "+ str(error)
        elif O_status == 4:
            err_return =  L_program+":"+str(O_status)+": Exception occured while deleting records in alloc_calc_allitemloc: "+ str(error)
        elif O_status == 5:
            err_return =  L_program+":"+str(O_status)+": Exception occured while deleting records in alloc_itm_search_dtl: "+ str(error)
        else:
            err_return =  L_program+":"+str(O_status)+": Exception Occured: "+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

#----------------------------------------------------------
# FUNCTION retreive_schedule_data
#----------------------------------------------------------

def retreive_schedule_data (conn, 
                            I_alloc_id):
    L_program = 'retreive_schedule_data'
    O_status  = 0
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/schedule_alloc_queries.yaml') as fh:
            queries     = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_get_param = queries['aso_update']['Q_get_param']
            
            df_params = pd.read_sql(Q_get_param,conn,params=(I_alloc_id,))
            return df_params , ""
    except Exception as error:
        err_return =  L_program+":"+str(O_status)+": Exception Occured: "+ str(error)
        print (err_return)
        return list(), err_return