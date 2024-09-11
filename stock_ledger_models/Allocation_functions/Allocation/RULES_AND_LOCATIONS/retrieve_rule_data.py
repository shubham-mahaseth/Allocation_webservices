from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
import pandas as pd
import yaml

##################################################################################
#Created By - Priyanshu Pandey                                                   #
#File Name - retrieve_rule.py                                                    #
#Purpose - To populate rules in RL screen                                        #
##################################################################################


#--------------------------------------------------------------
# Function to populate stores
#--------------------------------------------------------------
def retrieve_rule (conn,
                   I_alloc,
                   O_status):
    L_func_name ="retrieve_rule"
    O_status = 0
    emp_list = list()
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/populate_rules_location_queries.yaml') as fh:
            queries             = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_cre_tmp_tbl       = queries['retrieve_rule']['Q_cre_tmp_tbl']
            Q_ins_rule          = queries['retrieve_rule']['Q_ins_rule']
            Q_del_rule          = queries['retrieve_rule']['Q_del_rule']
            Q_fetch_alloc_rule  = queries['retrieve_rule']['Q_fetch_alloc_rule']
            #status
            O_status = 1
            #mycursor = conn.cursor()
            ##creating table
            #mycursor.execute(Q_cre_tmp_tbl)

            ##status
            #O_status = 2
            ##refreshing tmp table
            #mycursor.execute(Q_del_rule,(I_alloc,))
            #print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            ##status
            #O_status = 3
            ##inserting rule data
            #mycursor.execute(Q_ins_rule,(I_alloc,))
            #print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 4
            #returning DF
            df_rule = pd.read_sql(Q_fetch_alloc_rule,conn,params=(I_alloc,))
            return df_rule, ""

    except Exception as error:
        err_return = ""
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while creating temp table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while creating temp table :"+ str(error)
        elif O_status==2:
            print(L_func_name,":",O_status,":","Exception occured while tuncating temp table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while tuncating temp table :"+ str(error)
        elif O_status==3:
            print(L_func_name,":",O_status,":","Exception occured while inserting rule data: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting rule data :"+ str(error)
        elif O_status==4:
            print(L_func_name,":",O_status,":","Exception occured while returning rule data: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while returning rule data :"+ str(error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
            err_return = L_func_name+": "+"Exception occured :"+ str(error)
        conn.rollback()
        return emp_list,err_return