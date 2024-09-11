#from curses import ERR
import pandas as pd
from ..INVENTORY_SETUP.update_alloc_ext import update_alloc
import yaml

##################################################################################
#Created By - Proxima360                                                         #
#File Name  - setup_qty_limits.py                                                #
#Purpose    - All functions related to quantity limits screen                    #
##################################################################################

def P360_RETREIVE_QUANTITY_LIMITS(conn,I_alloc_no,I_mode):
    O_status = 0
    L_func_name = "P360_RETREIVE_QUANTITY_LIMITS"
    emp = list()
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_qty_limits_queries.yaml') as fh:
            queries          = yaml.load(fh, Loader=yaml.SafeLoader)
            C_temp_tbl       = queries['retreive_quantity_limits']['C_temp_tbl']
            C_alloc_level    = queries['retreive_quantity_limits']['C_alloc_level']
            C_alloc_criteria = queries['retreive_quantity_limits']['C_alloc_criteria']
            L_del_1          = queries['retreive_quantity_limits']['L_del_1']
            L_del_2          = queries['retreive_quantity_limits']['L_del_2']
            L_del_3          = queries['retreive_quantity_limits']['L_del_3']
            L_ins_1          = queries['retreive_quantity_limits']['L_ins_1']
            L_ins_2          = queries['retreive_quantity_limits']['L_ins_2']
            L_ins_3          = queries['retreive_quantity_limits']['L_ins_3']
            Q_get_qty_limit  = queries['retreive_quantity_limits']['Q_get_qty_limit']
            Q_del_tmp        = queries['retreive_quantity_limits']['Q_del_tmp']
            Q_del_alloc_qty_limits        = queries['retreive_quantity_limits']['Q_del_alloc_qty_limits'] #Changes from shubham start#
            Q_insert_alloc_qty_limits_temp        = queries['retreive_quantity_limits']['Q_insert_alloc_qty_limits_temp'] #Changes from shubham start#

            mycursor = conn.cursor()
            #status
            O_status = 1
            mycursor.execute(C_temp_tbl)
            #status
            O_status = 2
            df_alloc_level = pd.read_sql(C_alloc_level,conn,params=(I_alloc_no,))
            L_alloc_level = df_alloc_level.alloc_level[0]
            #status
            O_status = 3
            df_alloc_criteria = pd.read_sql(C_alloc_criteria,conn,params=(I_alloc_no,))
            L_alloc_criteria = df_alloc_criteria.alloc_criteria[0]
            #status
            O_status = 4
            if I_mode != "VIEW":
                #status
                O_status = 5 
                mycursor.execute(L_del_1,(I_alloc_no, ))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #status
                O_status= 6
                mycursor.execute(L_del_2,(I_alloc_no, I_alloc_no))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                
                #status
                O_status=7
                mycursor.execute(L_del_3,(I_alloc_no, I_alloc_no, I_alloc_no,I_alloc_no,I_alloc_no))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #status
                O_status=8
                #conn.commit()
                mycursor.execute(L_ins_1,(I_alloc_no, I_alloc_no))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #status
                O_status=9
                #conn.commit()
                if L_alloc_level == 'T':
                    #Changes from shubham start#
                    mycursor.execute(Q_del_alloc_qty_limits,(I_alloc_no,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    #Changes from shubham end#
                    #status
                    O_status=10
                    #conn.commit()
                    mycursor.execute(L_ins_2,(I_alloc_no, L_alloc_criteria, L_alloc_criteria, I_alloc_no))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    #status
                    O_status=11
                    #conn.commit()
            if L_alloc_level == 'T':
                mycursor.execute(Q_del_tmp,(I_alloc_no,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status = 12
                mycursor.execute(L_ins_3,(I_alloc_no, I_alloc_no, I_alloc_no))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #conn.commit()
            #Changes from shubham for style/diff start#
            else:
                O_status = 12.0
                mycursor.execute(Q_del_tmp,(I_alloc_no,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                O_status = 12
                mycursor.execute(Q_insert_alloc_qty_limits_temp,(I_alloc_no,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #Changes from shubham for style/diff start#

            if I_mode != "VIEW":
                #status
                O_status=13
                result1, err_msg1 = P360_GET_QTY_LIMITS(conn,I_alloc_no)
                if result1 == False:
                    conn.rollback()
                    #status
                    O_status=14
                    print(O_status)
                    conn.rollback()
                    conn.cursor().close()
                    return emp,err_msg1          
                else:
                    #status
                    O_status=15
                    conn.commit()
            conn.commit()
            df_qty_limit_data = pd.read_sql(Q_get_qty_limit,conn,params=(I_alloc_no,)) 
            conn.cursor().close()
            return df_qty_limit_data,""
    except Exception as error:
        print(error)
        #status
        O_status=16
        err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return emp,err_return  #empty list

##################################################################################
        
def P360_GET_QTY_LIMITS(conn,I_alloc_no):
    try: 
        O_status = [0]
        L_func_name = "P360_GET_QTY_LIMITS" #Changes from shubham for style/diff#
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_qty_limits_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            C_alloc_level = queries['get_qty_limits']['C_alloc_level']
            Q_alloc_rule = queries['get_qty_limits']['Q_alloc_rule']     #Changes from shubham for style/diff#
            L_del_1 = queries['get_qty_limits']['L_del_1']
            L_ins_2 = queries['get_qty_limits']['L_ins_2']
            L_ins_2_style = queries['get_qty_limits']['L_ins_2_style']   #Changes from shubham for style/diff#
            L_mer_1 = queries['get_qty_limits']['L_mer_1']
            mycursor = conn.cursor()
            #status
            O_status=1
            df_alloc_level = pd.read_sql(C_alloc_level,conn,params=(I_alloc_no,))
            L_alloc_level = df_alloc_level.alloc_level[0]
            #Changes from shubham for style/diff start#
            O_status=2
            df_alloc_rule = pd.read_sql(Q_alloc_rule,conn,params=(I_alloc_no,))
            L_net_need_ind = df_alloc_rule.net_need_ind[0]
            L_exact_ind = df_alloc_rule.exact_ind[0]
            #Changes from shubham for style/diff end#
            #status
            O_status=3
            #conn.commit()
            mycursor.execute(L_del_1,(I_alloc_no,))
            #status
            O_status=4
            #conn.commit()
            
            if L_alloc_level == 'D':
               #Changes from shubham for style/diff start#
               mycursor.execute(L_ins_2_style,(I_alloc_no,))
               print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
               #Changes from shubham for style/diff end#
            else:
               #Changes from shubham for style/diff start#
               mycursor.execute(L_ins_2,(L_net_need_ind,L_exact_ind,I_alloc_no,I_alloc_no,I_alloc_no))
               print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount) #modified query by shubham
               #Changes from shubham for style/diff start#
               #status
               O_status=5
            #conn.commit()   
            
            if L_alloc_level == 'T':
                mycursor.execute(L_mer_1,(I_alloc_no,)) 
                #status
                O_status=6
                #conn.commit()
            conn.commit()    
            conn.cursor().close()
            return True,""
        
    except Exception as error:
        print(error)
        #status
        err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return 

##################################################################################
         
def P360_INSERT_QTY_LIMITS(conn,
                           I_alloc_no):
    try:   
        O_status = [0]
        L_func_name = "P360_INSERT_QTY_LIMITS" #Changes from shubham for style/diff start#
        rows_affected=0
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/setup_qty_limits_queries.yaml') as fh:
            queries = yaml.load(fh, Loader=yaml.SafeLoader)
            C_alloc_level  = queries['insert_qty_limits']['C_alloc_level']
            C_get_rule_rec = queries['insert_qty_limits']['C_get_rule_rec']
            L_del_1        = queries['insert_qty_limits']['L_del_1']
            C_temp_tbl2    = queries['retreive_quantity_limits']['C_temp_tbl2']
            L_mer_1        = queries['insert_qty_limits']['L_mer_1']
            L_mer_2        = queries['insert_qty_limits']['L_mer_2'] #Changes from shubham for style/diff start#
            Q_upd_inputs   = queries['insert_qty_limits']['Q_upd_inputs']

            print("Executing Function :",L_func_name)
            mycursor = conn.cursor()
            #mycursor.execute(C_temp_tbl2)  #Changes from shubham for style/diff start#
            #status
            O_status=(1)
            #updating inputs
            mycursor.execute(Q_upd_inputs,(I_alloc_no,))
            #Changes from shubham for style/diff start#
            rows_affected = mycursor.rowcount
            print(L_func_name,"-",O_status,"-","rows_affected: ",rows_affected)
            #Changes from shubham for style/diff end#
            df_alloc_level = pd.read_sql(C_alloc_level,conn,params=(I_alloc_no,))
            L_alloc_level = df_alloc_level.alloc_level[0]
            #status
            O_status=(2)
            if L_alloc_level == "T":
                #mycursor.execute(L_del_1,(I_alloc_no,)) --using upsert in L_mer_1
                if rows_affected >0: 
                    #status
                    O_status=(3)
                    mycursor.execute(L_mer_1,(I_alloc_no,I_alloc_no,I_alloc_no)) 
                #Changes from shubham for style/diff start#
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            else:
                if rows_affected >0:  
                    O_status=(4)
                    mycursor.execute(L_mer_2,(I_alloc_no,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #Changes from shubham for style/diff end#
            #conn.commit()
            if rows_affected > 0:
                L_status = list()
                I_input_data = list()
                result1, err_msg1 = update_alloc(conn, O_status, I_alloc_no, None, None,'Y', I_input_data)
                if result1 == False:
                    #status
                    O_status=(5)
                    print(O_status)
                    conn.rollback()
                    conn.cursor().close()
                    return False, err_msg1
            #df_rule_row = pd.read_sql(C_get_rule_rec,conn,params=(I_alloc_no,)) 
            #L_rule_row = df_rule_row.*[0]
            result2, err_msg2 = P360_GET_QTY_LIMITS(conn,I_alloc_no)
            if result2 == False:
                #status
                O_status=(6)
                print(O_status)
                conn.rollback()
                conn.cursor().close()
                return False,err_msg2
            #mycursor.execute(C_drop_tbl,) #for testing purposes
            conn.commit()    
            conn.cursor().close()
            return True, ""
        
    except Exception as error:
        print(error)
        #status
        err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(error)
        print(err_return)
        conn.rollback()
        conn.cursor().close()
        return False, err_return 

##################################################################################        