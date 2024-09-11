from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
from ..INVENTORY_SETUP.update_alloc_ext import update_alloc
from ..INVENTORY_SETUP.inventory_setup import setup_location
import yaml
import pandas as pd
import math as mt

##################################################################################
#Created By - Naveen Ramanathan                                                  #
#File Name  - setup_rules_locations.py                                           #
#Purpose    - For retreiving locations in Rules & Locations screen               #
##################################################################################

#REFERENCE: [GROUP_TYPE = 1-LOC LIST,5-LOC TRAIT,6-ALL STORE,7-STORE] [LOC_TYPE = M-MULTI LOCATION,T-TRAITS,A-ALL STORE,L-LOCATION]
def RETREIVE_LOCATIONS(conn,
                       I_alloc_no):
    L_fun ="RETREIVE_LOCATIONS"
    O_status = 0
    print("EXECUTING: ",L_fun)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_rules_locations_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            C_temp_tbl = queries['retreive_locations']['C_temp_tbl']            
            C_temp_tbl2       = queries['retreive_locations']['C_temp_tbl2']
            L_ins      = queries['retreive_locations']['L_ins']
            L_sel      = queries['retreive_locations']['L_sel']
            Q_del_tmp  = queries['retreive_locations']['Q_del_tmp']

            mycursor = conn.cursor()
            #status
            O_status = 1
            mycursor.execute(C_temp_tbl)
            mycursor.execute(C_temp_tbl2)
            mycursor.execute(Q_del_tmp,(I_alloc_no,))
            print(O_status,"-","rows_affected: ",mycursor.rowcount) 
            #status
            O_status = 2
            mycursor.execute(L_ins,(I_alloc_no,I_alloc_no,I_alloc_no,I_alloc_no))
            print(O_status,"-","rows_affected: ",mycursor.rowcount) 
            #status
            O_status = 3
            df_out = pd.read_sql(L_sel,conn,params=(I_alloc_no,))
            conn.commit()
            conn.cursor().close()
            return df_out, ""
            
    except Exception as error:
        emp_list = list()
        err_return = ""
        if O_status==1:
            print(L_fun,":",O_status,":","Exception raised during temporary table creation:", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during temporary table creation :"+ str(error)
        elif O_status==2:
            print(L_fun,":",O_status,":","Exception raised during data insertion into alloc_location_temp table:", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during data insertion into alloc_location_temp table :"+ str(error)
        elif O_status==3:
            print(L_fun,":",O_status,":","Exception raised during data retreival process:", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during data retreival process :"+ str(error)
        else:
            print(L_fun,":",O_status,":","Exception Occured: ", error)
            err_return = L_fun+": "+"Exception Occured :"+ str(error)
        conn.rollback()
        #conn.cursor().close()
        return emp_list,err_return          

##################################################################################

def INSERT_LOCATIONS(conn,
                     I_alloc_no):
    L_fun ="INSERT_LOCATIONS"
    O_status = 0
    print("EXECUTING: ",L_fun)
    L_changes_found = 0
    L_loc_group_id = None
    L_recal_found = 0

    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_rules_locations_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)

            C_get_rule_rec    = queries['insert_locations']['C_get_rule_rec']
            C_check_all_store = queries['insert_locations']['C_check_all_store']
            C_check_location  = queries['insert_locations']['C_check_location']
            C_check_loc_list  = queries['insert_locations']['C_check_loc_list']
            C_check_loc_trait = queries['insert_locations']['C_check_loc_trait']
            C_check_recalc    = queries['insert_locations']['C_check_recalc']
            C_check_loc_group = queries['insert_locations']['C_check_loc_group']
            Q_gen_loc_grp_id  = queries['insert_locations']['Q_gen_loc_grp_id']
            L_del1            = queries['insert_locations']['L_del1']
            L_del3            = queries['insert_locations']['L_del3']
            L_del4            = queries['insert_locations']['L_del4']
            L_del5            = queries['insert_locations']['L_del5']
            L_del7            = queries['insert_locations']['L_del7']
            L_del8            = queries['insert_locations']['L_del8']
            L_del9            = queries['insert_locations']['L_del9']
            L_del11           = queries['insert_locations']['L_del11']
            L_del12           = queries['insert_locations']['L_del12']
            L_del13           = queries['insert_locations']['L_del13']
            L_del15           = queries['insert_locations']['L_del15']
            L_del16           = queries['insert_locations']['L_del16']
            L_del17           = queries['insert_locations']['L_del17']
            L_mer1            = queries['insert_locations']['L_mer1']
            L_mer3            = queries['insert_locations']['L_mer3']
            L_mer5            = queries['insert_locations']['L_mer5']
            L_mer7            = queries['insert_locations']['L_mer7']
            L_mer9            = queries['insert_locations']['L_mer9']
            L_mer11           = queries['insert_locations']['L_mer11']
            L_mer13           = queries['insert_locations']['L_mer13']
            L_mer15           = queries['insert_locations']['L_mer15']
            Q_merge_like_dtl  = queries['insert_locations']['Q_merge_like_dtl']
            Q_upd_inputs      = queries['insert_locations']['Q_upd_inputs']

            mycursor = conn.cursor() 
            mycursor.execute(Q_upd_inputs,(I_alloc_no,))
            print(O_status,"-","rows_affected: ",L_changes_found)
            #status
            O_status = 1
            mycursor.execute(L_del1,(I_alloc_no,I_alloc_no))
            L_changes_found = mycursor.rowcount
            print(O_status,"-","rows_affected: ",L_changes_found)

            ########################## BLOCK-1 => Loc_Type='L' ##########################
            #status
            O_status = 2
            mycursor.execute(L_del3,(I_alloc_no,I_alloc_no))
            L_changes_found = mycursor.rowcount
            print(O_status,"-","rows_affected: ",L_changes_found)

            #status
            O_status = 3
            mycursor.execute(L_del4,(I_alloc_no,I_alloc_no))
            L_changes_found = mycursor.rowcount
            print(O_status,"-","rows_affected: ",L_changes_found)

            #status
            O_status = 4
            mycursor.execute(L_del5,(I_alloc_no,))
            print(O_status,"-","rows_affected: ",L_changes_found)

            #status
            O_status = 5
            L_changes_found = mycursor.rowcount
            print(O_status,"-","rows_affected: ",L_changes_found)

            #status
            O_status = 6
            df_check_location = pd.read_sql(C_check_location,conn,params=(I_alloc_no,))

            if len(df_check_location) >0:                
                df_check_loc_group = pd.read_sql(C_check_loc_group,conn,params=(I_alloc_no,'L','L','L','L'))
                #status
                O_status = 7
                if len(df_check_loc_group) >0:
                    L_loc_group_id = df_check_loc_group.loc_group_id[0]                
                else:
                    #status
                    O_status = 8
                    df_loc_grp_id = pd.read_sql(Q_gen_loc_grp_id,conn) 
                    L_loc_group_id = df_loc_grp_id.loc_group_id[0]
                    L_loc_group_id = mt.trunc(L_loc_group_id)

                #status
                O_status = 9
                mycursor.execute(L_mer1,(L_loc_group_id,I_alloc_no,L_loc_group_id))
                L_changes_found = mycursor.rowcount
                print(O_status,"-","rows_affected: ",L_changes_found)

                #status
                O_status = 10
                mycursor.execute(L_mer3,(L_loc_group_id,I_alloc_no,L_loc_group_id))
                L_changes_found = mycursor.rowcount
                print(O_status,"-","rows_affected: ",L_changes_found)

            ########################## BLOCK-2 => Loc_Type='M' ##########################
            #status
            O_status = 12
            mycursor.execute(L_del7,(I_alloc_no,I_alloc_no))
            L_changes_found = mycursor.rowcount
            print(O_status,"-","rows_affected: ",L_changes_found)

            #status
            O_status = 13
            mycursor.execute(L_del8,(I_alloc_no,I_alloc_no))
            L_changes_found = mycursor.rowcount
            print(O_status,"-","rows_affected: ",L_changes_found)

            #status
            O_status = 14
            mycursor.execute(L_del9,(I_alloc_no,))
            L_changes_found = mycursor.rowcount
            print(O_status,"-","rows_affected: ",L_changes_found)

            O_status = 500
            df_check_loc_list = pd.read_sql(C_check_loc_list,conn,params=(I_alloc_no,))
            if len(df_check_loc_list) >0:
               #status
                O_status = 16
                df_check_loc_group = pd.read_sql(C_check_loc_group,conn,params=(I_alloc_no,'M','M','M','M'))
                if len(df_check_loc_group) >0:
                    L_loc_group_id = df_check_loc_group.loc_group_id[0]
                else:
                    #status
                    O_status = 8
                    df_loc_grp_id = pd.read_sql(Q_gen_loc_grp_id,conn) 
                    L_loc_group_id = df_loc_grp_id.loc_group_id[0]
                    L_loc_group_id = mt.trunc(L_loc_group_id)

                #status
                O_status = 18
                mycursor.execute(L_mer5,(L_loc_group_id,I_alloc_no,L_loc_group_id))
                L_changes_found = mycursor.rowcount
                print(O_status,"-","rows_affected: ",L_changes_found)

                #status
                O_status = 19
                mycursor.execute(L_mer7,(L_loc_group_id,I_alloc_no,L_loc_group_id))
                L_changes_found = mycursor.rowcount
                print(O_status,"-","rows_affected: ",L_changes_found)

            ########################## BLOCK-3 => Loc_Type='T' ##########################
            #status
            O_status = 21
            mycursor.execute(L_del11,(I_alloc_no,I_alloc_no))
            L_changes_found = mycursor.rowcount
            print(O_status,"-","rows_affected: ",L_changes_found)

            #status
            O_status = 22
            mycursor.execute(L_del12,(I_alloc_no,I_alloc_no))
            L_changes_found = mycursor.rowcount
            print(O_status,"-","rows_affected: ",L_changes_found)

            #status
            O_status = 23
            mycursor.execute(L_del13,(I_alloc_no,))
            L_changes_found = mycursor.rowcount
            print(O_status,"-","rows_affected: ",L_changes_found)

            O_status = 600
            df_check_loc_trait = pd.read_sql(C_check_loc_trait,conn,params=(I_alloc_no,))
            if len(df_check_loc_trait) >0:
                O_status = 24
                df_check_loc_group = pd.read_sql(C_check_loc_group,conn,params=(I_alloc_no,'T','T','T','T'))
                if len(df_check_loc_group) >0:
                    L_loc_group_id = df_check_loc_group.loc_group_id[0]
                else:
                    #status
                    O_status = 8
                    df_loc_grp_id = pd.read_sql(Q_gen_loc_grp_id,conn) 
                    L_loc_group_id = df_loc_grp_id.loc_group_id[0]
                    L_loc_group_id = mt.trunc(L_loc_group_id)

                #status
                O_status = 26
                mycursor.execute(L_mer9,(L_loc_group_id,I_alloc_no,L_loc_group_id))
                L_changes_found = mycursor.rowcount
                print(O_status,"-","rows_affected: ",L_changes_found)

                #status
                O_status = 27
                mycursor.execute(L_mer11,(L_loc_group_id,I_alloc_no,L_loc_group_id))
                L_changes_found = mycursor.rowcount
                print(O_status,"-","rows_affected: ",L_changes_found)

            ########################## BLOCK-4 => Loc_Type='A' ##########################
            #status
            O_status = 29
            mycursor.execute(L_del15,(I_alloc_no,I_alloc_no))
            L_changes_found = mycursor.rowcount
            print(O_status,"-","rows_affected: ",L_changes_found)
            #status
            O_status = 30
            mycursor.execute(L_del16,(I_alloc_no,I_alloc_no))
            L_changes_found = mycursor.rowcount
            print(O_status,"-","rows_affected: ",L_changes_found)
            #status
            O_status = 31
            mycursor.execute(L_del17,(I_alloc_no,))
            L_changes_found = mycursor.rowcount
            print(O_status,"-","rows_affected: ",L_changes_found)

            O_status = 700
            df_all_store = pd.read_sql(C_check_all_store,conn,params=(I_alloc_no,))
            if len(df_all_store) >0:
                O_status = 32
                df_check_loc_group = pd.read_sql(C_check_loc_group,conn,params=(I_alloc_no,'A','A','A','A'))
                print("df_check_loc_group",df_check_loc_group)
                if len(df_check_loc_group) >0:
                    L_loc_group_id = df_check_loc_group.loc_group_id[0]
                else:
                    #status
                    O_status = 8
                    df_loc_grp_id = pd.read_sql(Q_gen_loc_grp_id,conn) 
                    L_loc_group_id = df_loc_grp_id.loc_group_id[0]
                    L_loc_group_id = mt.trunc(L_loc_group_id)

                #status
                O_status = 34
                mycursor.execute(L_mer13,(L_loc_group_id,I_alloc_no,L_loc_group_id))
                L_changes_found = mycursor.rowcount
                print(O_status,"-","rows_affected: ",L_changes_found)

                #status
                O_status = 35
                mycursor.execute(L_mer15,(L_loc_group_id,I_alloc_no,L_loc_group_id))
                L_changes_found = mycursor.rowcount
                print(O_status,"-","rows_affected: ",L_changes_found)

            ########################## BLOCK-5 => FUNCTION CALLS ##########################
            #status
            O_status = 37
            mycursor.execute(Q_merge_like_dtl,(I_alloc_no,I_alloc_no))
            print(O_status,"-","rows_affected: ",L_changes_found)

            df_check_recalc = pd.read_sql(C_check_recalc,conn,params=(I_alloc_no,))
            if len(df_check_recalc) > 0:
                L_recal_found = df_check_recalc.recalc[0]
            #status
            O_status = 38
            if L_recal_found == 1 or L_changes_found >0:
                #status
                O_status = 39
                I_input_data = list()
                result1, err_msg1 = update_alloc(conn, O_status, I_alloc_no, None, None,'Y', I_input_data)
                if result1 == False:
                    conn.rollback()
                    conn.cursor().close()
                    return False ,err_msg1   
            O_status = 800
            df_get_rule_rec = pd.read_sql(C_get_rule_rec,conn,params=(I_alloc_no,))
            #status
            O_status = 40
            if len(df_get_rule_rec) >0:
                #status
                O_status = 41
                result2, err_msg2 = setup_location(conn,I_alloc_no,'C')
                if result2 == False:
                    conn.rollback()
                    conn.cursor().close()
                    return False,err_msg2
            conn.cursor().close()
            return True, ""
            
    except Exception as error:
        err_return = ""
        if O_status == 1 or O_status == 2 or O_status == 3 or O_status == 4 or O_status == 12 or O_status ==13 or O_status == 14 or O_status == 21 or O_status == 22 or O_status == 23 or O_status == 29 or O_status == 30 or O_status == 31:
            print(L_fun,":",O_status,":","Exception raised during execution of delete queries for alloc no:", I_alloc_no,":", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during execution of delete queries for alloc no :"+ str(error)
        elif O_status == 9 or O_status == 10 or O_status == 11 or O_status == 18 or O_status == 19 or O_status == 20 or O_status == 26 or O_status == 27 or O_status == 28 or O_status == 34 or O_status == 35 or O_status == 36:
            print(L_fun,":",O_status,":","Exception raised during execution of merge queries for alloc no:",I_alloc_no,":", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during execution of merge queries for alloc no :"+ str(error)
        elif O_status == 6 or O_status == 7 or O_status == 500 or O_status == 16 or O_status == 25 or O_status == 600 or O_status == 700 or O_status == 33 or O_status == 37 or O_status == 800:
            print(L_fun,":",O_status,":","Exception raised during execution of cursor queries for alloc no:",I_alloc_no,":", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during execution of cursor queries for alloc no :"+ str(error)
        elif O_status == 39 or O_status == 41:
            print(L_fun,":",O_status,":","Exception raised during execution of function calling: ", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during execution of function calling :"+ str(error)
        else:
            print(L_fun,":",O_status,":","Exception Occured: ", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception Occured:"+ str(error)
        conn.cursor().close()
        return False, err_return 

##################################################################################

def DELETE_LOCATIONS(conn,
                     I_alloc_no):
    L_fun ="DELETE_LOCATIONS"
    O_status = 0
    print("EXECUTING: ",L_fun)
    L_changes_found = 0
    emp_list = list()
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_rules_locations_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            C_get_rule_rec            =    queries['delete_locations']['C_get_rule_rec']
            C_check_all_store_del     =    queries['delete_locations']['C_check_all_store_del']
            C_check_location_del      =    queries['delete_locations']['C_check_location_del']
            C_check_loc_list_del      =    queries['delete_locations']['C_check_loc_list_del']
            C_check_loc_trait_del     =    queries['delete_locations']['C_check_loc_trait_del']
            L_del_alloc_location      =    queries['delete_locations']['L_del_alloc_location']
            L_del_alloc_loc_group_dtl =    queries['delete_locations']['L_del_alloc_loc_group_dtl']
            Q_del_loc_temp            =    queries['delete_locations']['Q_del_loc_temp']
            Q_fetch_loc               =    queries['delete_locations']['Q_fetch_loc']

            mycursor = conn.cursor()
            ########################## BLOCK-1 => Group_type=7 ##########################
            #status
            O_status = 1
            df_local = pd.read_sql(C_check_location_del,conn,params=(I_alloc_no,I_alloc_no))
            if len(df_local) >0:
                #status
                O_status = 2
                mycursor.execute(L_del_alloc_location,(I_alloc_no,7))
                L_changes_found = mycursor.rowcount
                print(O_status,"-","rows_affected: ",L_changes_found)
                #status
                O_status = 3
                mycursor.execute(L_del_alloc_loc_group_dtl,(I_alloc_no,))
                L_changes_found = mycursor.rowcount
                print(O_status,"-","rows_affected: ",L_changes_found)
            
            ########################## BLOCK-2 => Group_type=1 ##########################
            #status
            O_status = 4
            df_local = pd.read_sql(C_check_loc_list_del,conn,params=(I_alloc_no,I_alloc_no))
            if len(df_local) >0:
                #status
                O_status = 5
                mycursor.execute(L_del_alloc_location,(I_alloc_no,1))
                L_changes_found = mycursor.rowcount
                print(O_status,"-","rows_affected: ",L_changes_found)
                #status
                O_status = 6
                mycursor.execute(L_del_alloc_loc_group_dtl,(I_alloc_no,))
                L_changes_found = mycursor.rowcount
                print(O_status,"-","rows_affected: ",L_changes_found)

            ########################## BLOCK-3 => Group_type=5 ##########################
            #status
            O_status = 7
            df_local = pd.read_sql(C_check_loc_trait_del,conn,params=(I_alloc_no,I_alloc_no))
            if len(df_local) >0:
                #status
                O_status = 8
                mycursor.execute(L_del_alloc_location,(I_alloc_no,5))
                L_changes_found = mycursor.rowcount
                print(O_status,"-","rows_affected: ",L_changes_found)
                #status
                O_status = 9
                mycursor.execute(L_del_alloc_loc_group_dtl,(I_alloc_no,))
                L_changes_found = mycursor.rowcount
                print(O_status,"-","rows_affected: ",L_changes_found)

            ########################## BLOCK-4 => Group_type=6 ##########################
            #status
            O_status = 10
            df_local = pd.read_sql(C_check_all_store_del,conn,params=(I_alloc_no,I_alloc_no))
            if len(df_local) >0:
                #status
                O_status = 11
                mycursor.execute(L_del_alloc_location,(I_alloc_no,6))
                L_changes_found = mycursor.rowcount
                print(O_status,"-","rows_affected: ",L_changes_found)
                #status
                O_status = 12
                mycursor.execute(L_del_alloc_loc_group_dtl,(I_alloc_no,))
                L_changes_found = mycursor.rowcount
                print(O_status,"-","rows_affected: ",L_changes_found)

            #deleting temp table
            #status
            O_status = 12.5
            mycursor.execute(Q_del_loc_temp,(I_alloc_no,))
            print(O_status,"-","rows_affected: ",L_changes_found)

            ########################## BLOCK-5 => FUNCTION CALLS ##########################
            print('L_changes_found:', L_changes_found)
            if L_changes_found == 0:
                #status
                O_status = 13
                I_input_data = list()
                result1, err_msg1 = update_alloc(conn, O_status, I_alloc_no, None, None,'Y', I_input_data)
                if result1 == False:
                    print('update_alloc failed')
                    conn.rollback()
                    conn.cursor().close()
                    return False , err_msg1
                print('update_alloc success')
            else:
                #status
                O_status = 14
                df_get_rule_rec = pd.read_sql(C_get_rule_rec,conn,params=(I_alloc_no,))
                #status
                O_status = 15
                if len(df_get_rule_rec) >0:
                    #status
                    O_status = 16
                    result2, err_msg2 = setup_location (conn,I_alloc_no,O_status)
                    if result2 == False:
                        conn.rollback()
                        conn.cursor().close()
                        return False ,err_msg2
        conn.commit()
        df_location = pd.read_sql(Q_fetch_loc,conn,params=(I_alloc_no,))
        conn.cursor().close()
        return df_location,""

    except Exception as error:
        err_return = ""
        if O_status == 2 or O_status == 3 or O_status == 5 or O_status == 6 or O_status == 8 or O_status ==9 or O_status == 11 or O_status == 12:
            print(L_fun,":",O_status,":","Exception raised during execution of delete queries for alloc no:", I_alloc_no,":", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during execution of delete queries for alloc no :"+ str(error)
        elif O_status == 1 or O_status == 4 or O_status == 7 or O_status == 10 or O_status == 14:
            print(L_fun,":",O_status,":","Exception raised during execution of cursor queries for alloc no:", I_alloc_no,":", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during execution of cursor queries for alloc no :"+ str(error)
        elif O_status == 13 or O_status == 16:
            print(L_fun,":",O_status,":","Exception raised during execution of function calling: ", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during execution of function calling :"+ str(error)
        else:
            print(L_fun,":",O_status,":","Exception Occured: ", error)
            err_return = L_fun+": "+"Exception Occured :"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False,err_return

##################################################################################

def UPDATE_STORE_WH_REL(conn,
                        I_alloc_no,
                        I_store_wh_rel_ind):
    L_fun ="UPDATE_STORE_WH_REL"
    O_status = 0
    print("EXECUTING: ",L_fun)
    L_changes_found = 0

    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_rules_locations_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            L_upd       =    queries['upd_store_wh_rel']['L_upd']
            L_chk       =    queries['upd_store_wh_rel']['L_chk']

            mycursor = conn.cursor()
            #status
            O_status = 1
            df_chk = pd.read_sql(L_chk,conn,params=(I_alloc_no,))
            if len(df_chk)==0:
                print("Invalid allocation")
                return False, "Invalid allocation"

            mycursor.execute(L_upd,(I_store_wh_rel_ind,I_alloc_no))
            L_changes_found = mycursor.rowcount
            print(O_status,"-","rows_affected: ",L_changes_found)
            if L_changes_found>0:
                #status
                O_status = 2
                I_input_data = list()
                result1, err_msg1 = update_alloc(conn, O_status, I_alloc_no, None, None,'Y', I_input_data)
                if result1 == False:
                    conn.rollback()
                    conn.cursor().close()
                    return False, err_msg1
        conn.cursor().close()
        return True,""

    except Exception as error:
        err_return = ""
        if O_status == 1:
            print(L_fun,":",O_status,":","Exception raised during execution of update query for alloc no:", I_alloc_no,":", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during execution of update query for alloc no :"+ str(error)
        elif O_status == 2:
            print(L_fun,":",O_status,":","Exception raised during execution of function calling: ", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during execution of function calling :"+ str(error)
        else:
            print(L_fun,":",O_status,":","Exception Occured: ", error)
            err_return = L_fun+": "+"Exception Occured:"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

##################################################################################

def UPDATE_SIZE_PROFILE_IND(conn,
                            I_alloc_no,
                            I_size_prof_ind):

    L_fun ="UPDATE_SIZE_PROFILE_IND"
    O_status = 0
    print("EXECUTING: ",L_fun)
    L_changes_found = 0

    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_rules_locations_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            L_upd   = queries['update_size_profile_ind']['L_upd']

            mycursor = conn.cursor()
            #status
            O_status = 1
            mycursor.execute(L_upd,(I_size_prof_ind,I_alloc_no))
            L_changes_found = mycursor.rowcount
            print(O_status,"-","rows_affected: ",L_changes_found)
            if L_changes_found > 0:
                #status
                O_status = 2
                I_input_data = list()
                result1, err_msg1 = update_alloc(conn, O_status, I_alloc_no, None, None,'Y', I_input_data)
                if result1 == False:
                    conn.rollback()
                    conn.cursor().close()
                    return False , err_msg1
            conn.cursor().close()
            return True,""

    except Exception as error:
        err_return = ""
        if O_status == 1:
            print(L_fun,":",O_status,":","Exception raised during execution of merge query for alloc no:", I_alloc_no,":", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during execution of merge query for alloc no :"+ str(error)
        elif O_status == 2:
            print(L_fun,":",O_status,":","Exception raised during execution of function calling: ", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during execution of function calling :"+ str(error)
        else:
            print(L_fun,":",O_status,":","Exception Occured: ", error)
            err_return = L_fun+": "+"Exception Occured :"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False,err_return

##################################################################################

def UPDATE_MIN_PRESENTATION_QTY(conn,
                                I_alloc_no):    
    L_fun ="UPDATE_MIN_PRESENTATION_QTY"
    O_status = 0
    print("EXECUTING: ",L_fun)
    #Function is written for Source type - WH and Alloc level - SKU

    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_rules_locations_queries.yaml')  as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            C_temp_tbl         = queries['update_min_presentation_qty']['C_temp_tbl']
            C_temp_tbl_qty_lts = queries['update_min_presentation_qty']['C_temp_tbl_qty_lts']
            C_cursors          = queries['update_min_presentation_qty']['C_cursors']
            L_del1             = queries['update_min_presentation_qty']['L_del1']
            Q_ins_wh           = queries['update_min_presentation_qty']['Q_ins_wh'] #ce-1777
            L_mer1             = queries['update_min_presentation_qty']['L_mer1']
            L_mer2             = queries['update_min_presentation_qty']['L_mer2']

            #ce-1777
            Q_ins_po           = queries['update_min_presentation_qty']['Q_ins_po']
            Q_ins_asn          = queries['update_min_presentation_qty']['Q_ins_asn']
            Q_ins_tsf          = queries['update_min_presentation_qty']['Q_ins_tsf']
            Q_ins_wif          = queries['update_min_presentation_qty']['Q_ins_wif']
            Q_mrge_qnty        = queries['update_min_presentation_qty']['Q_mrge_qnty']
            Q_ins_pack         = queries['update_min_presentation_qty']['Q_ins_pack']
            Q_del_pack         = queries['update_min_presentation_qty']['Q_del_pack']

            mycursor = conn.cursor()
            #status
            O_status = 1 
            mycursor.execute(C_temp_tbl)
            #status
            O_status = 2
            mycursor.execute(C_temp_tbl_qty_lts)
            #status
            O_status = 3
            df_cursor = pd.read_sql(C_cursors,conn,params=(I_alloc_no,))
            L_alloc_level     = df_cursor.alloc_level[0]
            L_alloc_criteria  = df_cursor.alloc_criteria[0]
            L_enforce_wh_store_rel_ind = df_cursor.wh_store_rel_ind[0]
            #status
            O_status = 4
            mycursor.execute(L_del1)
            print(O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            #ce-1777
            O_status = 5
            if L_alloc_criteria == 'W':
                #status
                O_status = 6
                mycursor.execute(Q_ins_wh,(I_alloc_no,L_alloc_level,I_alloc_no,L_enforce_wh_store_rel_ind))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)

            elif L_alloc_criteria == 'P':
                mycursor.execute(Q_ins_po,(I_alloc_no,L_alloc_level,I_alloc_no))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)

            elif L_alloc_criteria == 'A':
                mycursor.execute(Q_ins_asn,(I_alloc_no,L_alloc_level,I_alloc_no))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)

            elif L_alloc_criteria == 'T':
                mycursor.execute(Q_ins_tsf,(I_alloc_no,L_alloc_level,I_alloc_no))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)

            elif L_alloc_criteria == 'F':
                mycursor.execute(Q_ins_wif,(I_alloc_no,L_alloc_level,I_alloc_no))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)
                
            #status
            O_status = 7
            if L_alloc_level == 'T':
                #status
                mycursor.execute(Q_ins_pack) #ce-1777
                print(O_status,"-","rows_affected: ",mycursor.rowcount)

                mycursor.execute(Q_del_pack) #ce-1777
                print(O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status = 8
                mycursor.execute(L_mer1,(I_alloc_no,))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status = 9
                mycursor.execute(L_mer2,(I_alloc_no,))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)
            #ce-1777
            elif L_alloc_level == 'D':
                mycursor.execute(Q_mrge_qnty,(I_alloc_no,))
                print(O_status,"-","rows_affected: ",mycursor.rowcount)

            conn.commit()
            conn.cursor().close()
            return True, ""
    except Exception as error:
        err_return = ""
        if O_status == 1 or O_status == 2:
            print(L_fun,":",O_status,":","Exception raised during temporary table creation", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during temporary table creation :"+ str(error)
        elif O_status == 3:
            print(L_fun,":",O_status,":","Exception raised during execution of cursor queries for alloc no:", I_alloc_no,":", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during execution of cursor queries for alloc no :"+ str(error)
        elif O_status == 4:
            print(L_fun,":",O_status,":","Exception raised during execution of delete queries for deleting data from temp table for alloc no:", I_alloc_no,":", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during execution of delete queries for deleting data from temp table for alloc no :"+ str(error)
        elif O_status == 6:
            print(L_fun,":",O_status,":","Exception raised during execution of insert queries for inserting data into temp table for alloc no:", I_alloc_no,":", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during execution of insert queries for inserting data into temp table for alloc no :"+ str(error)
        elif O_status == 8 or O_status == 9:
            print(L_fun,":",O_status,":","Exception raised during execution of merge queries for merging data into alloc_quantity_limits table for alloc no:", I_alloc_no,":", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception raised during execution of merge queries for merging data into alloc_quantity_limits table for alloc no :"+ str(error)
        else:
            print(L_fun,":",O_status,":","Exception Occured: ", error)
            err_return = L_fun+":"+str(O_status)+": "+"Exception Occured:"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

##################################################################################

def insert_rule(conn,
                I_rule_rec,
                O_status):    
    L_func_name ="insert_rule"
    O_status = 0
    print("EXECUTING: ",L_func_name)

    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_rules_locations_queries.yaml')  as fh:
            queries    = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_del_rule = queries['insert_rule']['Q_del_rule']
            Q_ins_rule = queries['insert_rule']['Q_ins_rule']

            mycursor = conn.cursor()
            #status
            O_status = 1
            if len(I_rule_rec)>0:
                #status
                O_status = 2
                L_alloc_no                     = I_rule_rec["ALLOC_NO"]
                L_template_no                  = I_rule_rec["TEMPLATE_NO"]
                L_rule_type                    = I_rule_rec["RULE_TYPE"]
                L_rule_level                   = I_rule_rec["RULE_LEVEL"]
                L_exact_ind                    = I_rule_rec["EXACT_IND"]
                L_size_profile_ind             = I_rule_rec["SIZE_PROFILE_IND"]
                L_cascade_ind                  = I_rule_rec["CASCADE_IND"]
                L_net_need_ind                 = I_rule_rec["NET_NEED_IND"]
                L_use_rule_level_on_hand_ind   = I_rule_rec["USE_RULE_LEVEL_ON_HAND_IND"]
                L_include_clearance_stock_ind  = I_rule_rec["INCLUDE_CLEARANCE_STOCK_IND"]
                L_regular_sales_ind            = I_rule_rec["REGULAR_SALES_IND"]
                L_promo_sales_ind              = I_rule_rec["PROMO_SALES_IND"]
                L_clearance_sales_ind          = I_rule_rec["CLEARANCE_SALES_IND"]
                L_include_inv_in_min_ind       = I_rule_rec["INCLUDE_INV_IN_MIN_IND"]
                L_include_inv_in_max_ind       = I_rule_rec["INCLUDE_INV_IN_MAX_IND"]
                L_on_order_commit_date         = I_rule_rec["ON_ORDER_COMMIT_DATE"]
                L_on_order_commit_weeks        = I_rule_rec["ON_ORDER_COMMIT_WEEKS"]
                L_iwos_weeks                   = I_rule_rec["IWOS_WEEKS"]
                L_weeks_this_year              = I_rule_rec["WEEKS_THIS_YEAR"]
                L_weeks_last_year              = I_rule_rec["WEEKS_LAST_YEAR"]
                L_weeks_future                 = I_rule_rec["WEEKS_FUTURE"]
                L_start_date1                  = I_rule_rec["START_DATE1"]
                L_end_date1                    = I_rule_rec["END_DATE1"]
                L_start_date2                  = I_rule_rec["START_DATE2"]
                L_end_date2                    = I_rule_rec["END_DATE2"]
                L_corporate_rule_id            = I_rule_rec["CORPORATE_RULE_ID"]
                L_include_mid_tier_on_hand_ind = I_rule_rec["INCLUDE_MID_TIER_ON_HAND_IND"]
                L_enforce_pres_min_ind         = I_rule_rec["ENFORCE_PRES_MIN_IND"]
                L_lead_time_need_ind           = I_rule_rec["LEAD_TIME_NEED_IND"]
                L_lead_time_need_rule_type     = I_rule_rec["LEAD_TIME_NEED_RULE_TYPE"]
                L_lead_time_need_start_date    = I_rule_rec["LEAD_TIME_NEED_START_DATE"]
                L_lead_time_need_end_date      = I_rule_rec["LEAD_TIME_NEED_END_DATE"]
                L_convert_to_pack              = I_rule_rec["CONVERT_TO_PACK"]

                #status
                O_status = 3
                mycursor.execute(Q_del_rule,(L_alloc_no,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                L_params = (L_alloc_no,                  L_template_no,                  L_rule_type,            L_rule_level,                 L_exact_ind, 
                            L_size_profile_ind,          L_cascade_ind,                  L_net_need_ind,         L_use_rule_level_on_hand_ind, L_include_clearance_stock_ind, 
                            L_regular_sales_ind,         L_promo_sales_ind,              L_clearance_sales_ind,  L_include_inv_in_min_ind,     L_include_inv_in_max_ind,
                            L_on_order_commit_date,      L_on_order_commit_weeks,        L_iwos_weeks,           L_weeks_this_year,            L_weeks_last_year, 
                            L_weeks_future,              L_start_date1,                  L_end_date1,            L_start_date2,                L_end_date2, 
                            L_corporate_rule_id,         L_include_mid_tier_on_hand_ind, L_enforce_pres_min_ind, L_lead_time_need_ind,         L_lead_time_need_rule_type, 
                            L_lead_time_need_start_date, L_lead_time_need_end_date,      L_convert_to_pack)
                #status
                O_status = 4
                mycursor.execute(Q_ins_rule,(L_params))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                conn.commit()
                conn.cursor().close()
                return True,""
            else:
                print("No inputs for allocation rules")
                conn.cursor().close()
                return False,L_func_name+":"+str(O_status)+": "+"No inputs for allocation rules"

    except Exception as error:
        err_return = ""
        if O_status<=2:
            print(L_func_name,":",O_status,":","Exception occured while retrieving input: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while retrieving input :"+ str(error)
        elif O_status==3:
            print(L_func_name,":",O_status,":","Exception occured while deleting allocation rule data: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting allocation rule data :"+ str(error)
        elif O_status==4:
            print(L_func_name,":",O_status,":","Exception occured while insering rule data: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while insering rule data :"+ str(error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
            err_return = L_func_name+": "+"Exception occured :"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False,err_return