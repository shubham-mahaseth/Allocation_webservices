from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
import pandas as pd
import yaml

##################################################################################
#Created By - Priyanshu Pandey                                                   #
#File Name - populate_store   .py                                                #
#Purpose - To populate stores in RL screen                                       #
##################################################################################


#--------------------------------------------------------------
# Function to populate stores
#--------------------------------------------------------------
def populate_store (conn
                    ,I_search_criteria
                    ,O_status):
    L_func_name ="populate_store"
    O_status = 0
    emp_list = list()
    item_loc_status = 'L'
    L_alloc_criteria = 'W'
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/populate_rules_location_queries.yaml') as fh:
            print('DEBUG CONN - CONNECTION IN populate_store: ',conn)
            queries           = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_fetch_loc       = queries['populate_store']['Q_fetch_loc']
            Q_del_loc         = queries['populate_store']['Q_del_loc']
            Q_rtv_store_tmp   = queries['populate_store']['Q_rtv_store_tmp']
            Q_del_store_temp  = queries['populate_store']['Q_del_store_temp']
            Q_ins_store       = queries['populate_store']['Q_ins_store']
            Q_ins_store_list  = queries['populate_store']['Q_ins_store_list']
            Q_ins_store_trait = queries['populate_store']['Q_ins_store_trait']
            Q_ins_all_store   = queries['populate_store']['Q_ins_all_store']
            Q_ins_store_tmp   = queries['populate_store']['Q_ins_store_tmp']
            Q_cre_store_temp  = queries['populate_store']['Q_cre_store_temp']
            Q_alloc_criteria  = queries['populate_store']['Q_get_alloc_criteria']
            #Q_cre_temp_tbl    = queries['populate_store']['Q_cre_temp_tbl'] #for testing
            
            mycursor = conn.cursor()
            print('DEBUG CONN - CONN NOT NULL')
            #mycursor.execute(Q_cre_temp_tbl)#only for testing
            mycursor.execute(Q_cre_store_temp)

            #status
            O_status = 1

            L_alloc_no       = I_search_criteria["ALLOC_NO"]
            L_all_store_ind  = I_search_criteria["ALL_STORE"]
            L_location       = I_search_criteria["LOCATION"]
            L_location_list  = I_search_criteria["LOCATION_LIST"]
            L_location_trait = I_search_criteria["LOCATION_TRAIT"]
            L_excl_location  = I_search_criteria["EXCLUDE_LOCATION"]

            #GET item_loc_status
            df_alloc_criteria = pd.read_sql(Q_alloc_criteria,conn,params=(L_alloc_no,))
            if len(df_alloc_criteria)>0:                
                L_alloc_criteria = df_alloc_criteria["ALLOC_CRITERIA"][0]
            if L_alloc_criteria == 'F':
                item_loc_status = 'A'
            else:
                item_loc_status = 'L'
            #appending None to execute IN operator in queries
            if len(L_location) == 1:
                L_location.append(-1)
            if len(L_location_list) == 1:
                L_location_list.append(-1)
            if len(L_location_trait) == 1:
                L_location_trait.append(-1)
            if len(L_excl_location) == 1:
                L_excl_location.append(-1)
            if len(L_excl_location) ==0:
                L_excl_location.append(-1)
                L_excl_location.append(-1)

            #convert list into tuple
            L_location       = convert_numpy(L_location)
            L_location_list  = convert_numpy(L_location_list)
            L_location_trait = convert_numpy(L_location_trait)
            L_excl_location  = convert_numpy(L_excl_location)

            #status
            O_status = 2
            mycursor.execute(Q_del_store_temp,(L_alloc_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 2.1
            mycursor.execute(Q_rtv_store_tmp,(L_alloc_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 2.2            
            #mycursor.execute(Q_del_loc,(L_alloc_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status = 3
            if L_all_store_ind =='Y':
                #status
                O_status = 4 
                mycursor.execute(Q_ins_all_store.format(L_alloc_no,item_loc_status,L_excl_location,L_alloc_no))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #status
                O_status = 5
                mycursor.execute(Q_ins_store_tmp,(L_alloc_no,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            else:
                #status
                O_status = 6 
                if len(L_location)>0:
                    #status
                    O_status = 7
                    mycursor.execute(Q_ins_store.format(L_alloc_no,item_loc_status,L_location,L_excl_location,L_alloc_no))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    #status
                    O_status = 8
                    mycursor.execute(Q_ins_store_tmp,(L_alloc_no,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #status
                O_status = 9  
                if len(L_location_list)>0:
                    #status
                    O_status = 10
                    mycursor.execute(Q_ins_store_list.format(L_alloc_no,item_loc_status,L_location_list,L_excl_location,L_alloc_no))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    #status
                    O_status = 11
                    mycursor.execute(Q_ins_store_tmp,(L_alloc_no,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #status
                O_status = 12 
                if len(L_location_trait)>0:
                    #status
                    O_status = 13
                    mycursor.execute(Q_ins_store_trait.format(L_alloc_no,item_loc_status,L_location_trait,L_excl_location,L_alloc_no))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    #status
                    O_status = 14
                    mycursor.execute(Q_ins_store_tmp,(L_alloc_no,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            
            #status
            O_status = 15
            conn.commit()
            df_location = pd.read_sql(Q_fetch_loc,conn,params=(L_alloc_no,))
            conn.cursor().close()
            return df_location,""

    except Exception as error:
        err_return = ""
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while processing input data: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing input data:"+ str(error)
        elif O_status>=2 and O_status<=3:
            print(L_func_name,":",O_status,":","Exception occured while deleting locations: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting locations :"+ str(error)
        elif O_status>=3 and O_status <=5:
            print(L_func_name,":",O_status,":","Exception occured while processing all store data: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing all store data :"+ str(error)
        elif O_status>=6 and O_status <=8:
            print(L_func_name,":",O_status,":","Exception occured while processing location input: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing location input :"+ str(error)
        elif O_status>=9 and O_status <=11:
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing location list input :"+ str(error)
            print(L_func_name,":",O_status,":","Exception occured while processing location list input: ", error)
        elif O_status>=12 and O_status <=14:
            print(L_func_name,":",O_status,":","Exception occured while processing location trait input: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing location trait input :"+ str(error)
        elif O_status==15:
            print(L_func_name,":",O_status,":","Exception occured while processing output: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing output :"+ str(error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
            err_return = L_func_name+": "+"Exception occured:"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return emp_list, err_return