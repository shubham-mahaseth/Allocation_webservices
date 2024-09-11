import yaml
from datetime import date, timedelta
import pandas as pd
from ..GLOBAL_FILES.get_vdate import get_vdate
#from ..INVENTORY_SETUP.inventory_setup import setup_location
from ..RULES_AND_LOCATIONS.change_weight import pop_need_dates

#----------------------------------------------------------
# FUNCTION retreive_purge_alloc
#----------------------------------------------------------

def retreive_purge_alloc (conn, 
                          I_alloc_id):
    L_program = 'retreive_purge_alloc';
    L_DAYS    = 0
    L_V_DATE  = get_vdate(conn)
    L_create_date = date
    O_status  = 0
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_purge_alloc.yaml') as fh:
            queries                         = yaml.load(fh, Loader=yaml.SafeLoader)
            c_get_purge_approve_days        = queries['retreive_purge_alloc']['c_get_purge_approve_days']
            c_get_status                    = queries['retreive_purge_alloc']['c_get_status']
            c_get_item_search               = queries['retreive_purge_alloc']['c_get_item_search']
            Q_insert_alloc_itm_search_dtl   = queries['retreive_purge_alloc']['Q_insert_alloc_itm_search_dtl']
            Q_del_alloc_itm_search_dtl_hst  = queries['retreive_purge_alloc']['Q_del_alloc_itm_search_dtl_hst']
            Q_insert_alloc_calc_source_temp = queries['retreive_purge_alloc']['Q_insert_alloc_calc_source_temp']
            Q_del_alloc_calc_source_hst     = queries['retreive_purge_alloc']['Q_del_alloc_calc_source_hst']
            c_get_rule_rec                  = queries['retreive_purge_alloc']['c_get_rule_rec']
            Q_del_alc_quantity_limits       = queries['retreive_purge_alloc']['Q_del_alc_quantity_limits']
            c_get_item_loc                  = queries['retreive_purge_alloc']['c_get_item_loc']
            Q_insert_alloc_calc_item_loc    = queries['retreive_purge_alloc']['Q_insert_alloc_calc_item_loc']
            Q_del_alloc_calc_item_loc_hst   = queries['retreive_purge_alloc']['Q_del_alloc_calc_item_loc_hst'] 
            cursor = conn.cursor()
            #status
            O_status = 1
            L_purge_no_of_days = pd.read_sql (c_get_purge_approve_days, conn)
            L_DAYS = int (L_purge_no_of_days["DAYS"][0])
            #status
            O_status = 2
            L_V_DATE = get_vdate(conn)
            L_purge_date_older = str (L_V_DATE - timedelta(days=L_DAYS))
            #status
            O_status = 3
            df_status = pd.read_sql (c_get_status, conn, params= (I_alloc_id,))
            L_GET_STATUS = str (df_status["status"][0])
            L_create_date = str (df_status["create_date"][0])
            #status
            O_status = 4
            L_item_search_found = pd.read_sql (c_get_item_search, conn, params= (I_alloc_id,))
            L_itm_found = int (L_item_search_found["1"][0])            
            L_itm_found = L_itm_found if L_itm_found is not None else 0
            #status
            O_status = 5
            if L_itm_found == 0 and L_create_date <= L_purge_date_older:
                cursor.execute (Q_insert_alloc_itm_search_dtl,(I_alloc_id,))
                print("No of records inserted in alloc_itm_search_dtl: ", cursor.rowcount)
                #status
                O_status = 6
                cursor.execute (Q_del_alloc_itm_search_dtl_hst,(I_alloc_id,))
                print("No of records deleted in alloc_itm_search_dtl_hst: ", cursor.rowcount)
                #status
                O_status = 7
                cursor.execute (Q_insert_alloc_calc_source_temp,(I_alloc_id,))
                print("No of records inserted in alloc_calc_source_temp: ", cursor.rowcount)
                #status
                O_status = 8
                cursor.execute (Q_del_alloc_calc_source_hst,(I_alloc_id,))
                print("No of records deleted in alloc_calc_source_hst: ", cursor.rowcount)
            #status
            O_status = 9
            L_alc_rule_row = pd.read_sql (c_get_rule_rec, conn, params= (I_alloc_id,))
            #print("L_alc_rule_row:", L_alc_rule_row)
            #status
            O_status = 10
            if len(L_alc_rule_row)> 0:
                from ..INVENTORY_SETUP.inventory_setup import setup_location

                L_setup_destination, err_msg = setup_location (conn
                                                      ,I_alloc_id
                                                      ,O_status)
                print ("L_setup_destination:", L_setup_destination)
                if L_setup_destination == False:
                    print("setup_location failed: ",O_status)
                    conn.rollback()
                    conn.cursor().close()
                    return list(), L_program+":"+str(O_status)+": "+str(err_msg)
                #status
                O_status = 11
                l_pop_need_dates, err_msg  = pop_need_dates (conn,
                                                   I_alloc_id,
                                                   O_status)
                print ("l_pop_need_dates:", l_pop_need_dates)
                if l_pop_need_dates == False:
                    print("pop_need_dates failed: ",O_status)
                    conn.rollback()
                    conn.cursor().close()
                    return list(), L_program+":"+str(O_status)+": "+str(err_msg)
            #status
            O_status = 12
            cursor.execute (Q_del_alc_quantity_limits,(I_alloc_id,I_alloc_id,I_alloc_id))
            print("No of records deleted in alc_quantity_limits: ", cursor.rowcount)
            #status
            O_status = 13
            #if L_GET_STATUS in ['2', '3', '4', '5', '6', '10', '0']:
            if L_GET_STATUS in ['APV','EXT','COLS','CNL','RSV','POC','WS']:
                L_item_loc_found = pd.read_sql (c_get_item_loc,conn,params= (I_alloc_id,))
                V_item_loc_found = int (L_item_loc_found["1"][0])
                V_item_loc_found = V_item_loc_found if V_item_loc_found is not None else 0
                print ("V_item_loc_found:", V_item_loc_found)
                #status
                O_status = 14
                if V_item_loc_found == 0 and L_create_date <= L_purge_date_older:
                    cursor.execute ( Q_insert_alloc_calc_item_loc, (I_alloc_id,))
                    print("No of records inserted in alloc_calc_item_loc: ", cursor.rowcount)
                    #status
                    O_status = 15
                    cursor.execute ( Q_del_alloc_calc_item_loc_hst,(I_alloc_id,))
                    print("No of records deleted in alloc_calc_item_loc_hst: ", cursor.rowcount)
            conn.commit()
            conn.cursor().close()
            print("O_status: ",16)
            print(L_alc_rule_row)
            return L_alc_rule_row, "" 
    except Exception as error:
        err_return = ""
        if O_status == 1:
            err_return =  L_program+":"+str(O_status)+": Exception occured while fetching L_DAYS: "+ str(error)
        elif O_status == 2:
            err_return =  L_program+":"+str(O_status)+": Exception occured while fetching L_purge_date_older: "+ str(error)
        elif O_status == 3:
            err_return =  L_program+":"+str(O_status)+": Exception occured while fetching status and create_datetime: "+ str(error)
        elif O_status == 4:
            err_return =  L_program+":"+str(O_status)+": Exception Occured while fetching L_itm_found: "+ str(error)
        elif O_status == 5:
            err_return =  L_program+":"+str(O_status)+": Exception Occured while inserting data in alloc_itm_search_dtl: "+ str(error)
        elif O_status == 6:
            err_return =  L_program+":"+str(O_status)+": Exception Occured while deleting data in alloc_itm_search_dtl_hst: "+ str(error)
        elif O_status == 7:
            err_return =  L_program+":"+str(O_status)+": Exception Occured while inserting data in alloc_calc_source_temp: "+ str(error)
        elif O_status == 8:
            err_return =  L_program+":"+str(O_status)+": Exception Occured while deleting data in alloc_calc_source_hst: "+ str(error)
        elif O_status == 9:
            err_return =  L_program+":"+str(O_status)+": Exception Occured while fetching L_alc_rule_row: "+ str(error)
        elif O_status == 10:
            err_return =  L_program+":"+str(O_status)+": Exception Occured in function call L_setup_destination: "+ str(error)
        elif O_status == 11:
            err_return =  L_program+":"+str(O_status)+": Exception Occured in function call l_pop_need_dates: "+ str(error)
        elif O_status == 12:
            err_return =  L_program+":"+str(O_status)+": Exception Occured while deleting data in alc_quantity_limits: "+ str(error)
        elif O_status == 13:
            err_return =  L_program+":"+str(O_status)+": Exception Occured while fetching L_item_loc_found: "+ str(error)
        elif O_status == 14:
            err_return =  L_program+":"+str(O_status)+": Exception Occured while inserting data in alloc_calc_item_loc: "+ str(error)
        elif O_status == 15:
            err_return =  L_program+":"+str(O_status)+": Exception Occured while deleting data in alloc_calc_item_loc_hst: "+ str(error)
        else:
            err_return =  L_program+":"+str(O_status)+": Exception Occured: "+ str(error)

        print(err_return)
        conn.rollback()
        return list(), err_return

