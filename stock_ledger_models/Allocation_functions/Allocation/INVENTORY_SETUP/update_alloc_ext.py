from .inventory_setup import setup_location
import pandas as pd
#from GLOBAL_FILES.upd_alloc_ext_queries import update_alloc_ext
import yaml


#--------------------------------------------------------------
# Function to update_alc_alloc_ext 
#--------------------------------------------------------------
def update_alloc(conn,O_status,L_alloc_no,L_level,L_date,L_recalc_ind,I_input_data):
    L_func_name ="update_alloc"
    O_status=0

    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/upd_alloc_ext_queries.yaml') as fh:
            queries            = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_sel_alloc_rule   = queries['update_alloc_ext']['Q_sel_alloc_rule']
            Q_sel_date_status  = queries['update_alloc_ext']['Q_sel_date_status']
            Q_upd_date         = queries['update_alloc_ext']['Q_upd_date']
            Q_upd_date_itemloc = queries['update_alloc_ext']['Q_upd_date_itemloc']
            Q_upd_alc_loc      = queries['update_alloc_ext']['Q_upd_alc_loc']
            Q_merge            = queries['update_alloc_ext']['Q_merge']
            Q_chck_calc_data   = queries['update_alloc_ext']['Q_chck_calc_data']
            mycursor=conn.cursor()
            mycursor.execute("SET sql_mode = ''; ")


            if len(I_input_data)>0:
                L_alloc_criteria  = I_input_data["ALLOC_CRITERIA"]
                L_status          = I_input_data["STATUS"]
                L_alloc_desc      = I_input_data["ALLOC_DESC"]
                L_alloc_type      = I_input_data["ALLOC_TYPE"]
                L_context         = I_input_data["CONTEXT"]
                L_promotion       = I_input_data["PROMOTION"]
                L_create_id       = I_input_data["CREATE_ID"]
                L_create_datetime = I_input_data["CREATE_DATETIME"]
            else:
                L_alloc_criteria  = None
                L_status          = None
                L_alloc_desc      = None
                L_alloc_type      = None
                L_context         = None
                L_create_id       = None
                L_promotion       = None
                L_create_datetime = None


            df_search_data = pd.read_sql(Q_sel_date_status,conn,params=(L_alloc_no,))
            if len(df_search_data)>0:
                df_rel_date=df_search_data["release_date"][0]
            else:
                df_rel_date=None

            if L_date != None:
                O_status=20
                mycursor.execute(Q_upd_date,(L_date,L_alloc_no))
                mycursor.execute(Q_upd_date_itemloc,(L_date,L_alloc_no))
                mycursor.execute(Q_upd_alc_loc,(L_alloc_no,L_date))

            O_status=30
            if df_rel_date != None and df_rel_date != L_date and L_date!=None: #changes for calling the setup_location twice from RL screen OK button                mycursor.execute(Q_sel_alloc_rule,(L_alloc_no,))
                fetch=mycursor.fetchall()
                if len(fetch)> 0:
                    df_chck_calc_data = pd.read_sql(Q_chck_calc_data,conn,params=(L_alloc_no,))
                    if len(df_chck_calc_data) > 0:
                        L_func, err_msg1= setup_location(conn,L_alloc_no,O_status)
                        if L_func == False:
                            conn.rollback()
                            conn.cursor().close()
                            return False,err_msg1

        O_status=40
        mycursor.execute(Q_merge,(L_alloc_no,L_alloc_criteria,L_status,L_alloc_desc,L_alloc_type,L_level,L_context,L_promotion,L_date,L_recalc_ind,L_create_id,L_create_datetime,L_alloc_criteria,L_context,L_alloc_desc,L_promotion,L_alloc_type,L_level,L_date,L_date,L_recalc_ind,))
        #conn.commit()
        conn.cursor().close()
        return True, ""


    except Exception as argument:
        err_return = ""
        if O_status==10:
            print("update_alloc:Exception occured in selecting release_date from alloc_head : ",L_func_name,argument)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured in selecting release_date from alloc_head:"+ str(argument)
        elif O_status==20:
            print("update_alloc: Exception occured while updating the query:",L_func_name,argument)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while updating the query:"+ str(argument)
        elif O_status==30:
            print("update_alloc:Exception occured while calling the setup_location function ",L_func_name,argument)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while calling the setup_location function:"+ str(argument)
        elif O_status==40:
            print("update_alloc:Exception occured in merge and insert query ",L_func_name,argument)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured in merge and insert query:"+ str(argument)
        else:
            print("load_item:",L_func_name,argument)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(argument)
        conn.rollback()
        conn.cursor().close()
        return False,err_return