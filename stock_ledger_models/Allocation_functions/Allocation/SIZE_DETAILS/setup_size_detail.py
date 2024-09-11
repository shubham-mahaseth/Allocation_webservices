import numpy as np
import pandas as pd
import yaml

##################################################################################
#Created By - Priyanshu Pandey                                                   #
#File Name - setup_size_detail.py                                                #
#Purpose - To process on click functionalities                                   #
##################################################################################


#------------------------------------------------------------------
# Function to populate size profile from allocation detail screen
#------------------------------------------------------------------
def get_alloc_size_profile (conn,
                            I_alloc,
                            I_wh,
                            I_order_no,
                            I_item_id,
                            I_diff_id,
                            I_location):
    L_func_name ="get_alloc_size_profile"
    O_status = 0
    emp_list = list()
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/size_profile_queries.yaml') as fh:
            queries            = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_cre_size_prf_tbl = queries['size_profile_queries']['Q_cre_size_prf_tbl']
            Q_del_size_prf     = queries['size_profile_queries']['Q_del_size_prf']
            Q_ins_size_prof    = queries['size_profile_queries']['Q_ins_size_prof']
            Q_fetch_size_prof  = queries['size_profile_queries']['Q_fetch_size_prof']

            mycursor = conn.cursor()
            #status
            O_status = 1
            mycursor.execute(Q_cre_size_prf_tbl)

            #status
            O_status = 2
            mycursor.execute(Q_del_size_prf,(I_alloc,I_item_id,I_diff_id,I_location))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 3
            print(Q_ins_size_prof.format(I_alloc,I_item_id,I_diff_id,I_wh,I_order_no,I_location))
            mycursor.execute(Q_ins_size_prof.format(I_alloc,I_item_id,I_diff_id,I_wh,I_order_no,I_location))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()

            #status
            O_status = 4
            df_size_profile = pd.read_sql(Q_fetch_size_prof,conn,params=(I_alloc,))

            if len(df_size_profile)>0:
                conn.cursor().close()
                return df_size_profile, ""
            else:
                err_msg = L_func_name+":"+str(O_status)+": "+ "No record found for item and location"
                print(err_msg)
                conn.cursor().close()
                return emp_list, err_msg

    except Exception as error:
        err_return =''
        if O_status==1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while creating temp table:  "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while creating temp table: ", error)
        elif O_status==2:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while refreshing size profile table:  "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while refreshing size profile table: ", error)
        elif O_status==3:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting size profile data:  "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while inserting size profile data: ", error)
        elif O_status==4:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while fetching size profile data:  "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while fetching size profile data: ", error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured :  "+ str(error)
            #print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
        conn.rollback()
        conn.cursor().close()
        return emp_list, err_return
#--------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                                                                                                                 

def retreive_alloc_size_details (conn,
                                 I_alloc,
                                 I_wh,
                                 I_order_no,
                                 I_item_id,
                                 I_diff_id,
                                 I_location):
    L_func_name ="retreive_alloc_size_details"
    O_status = 0
    emp_list = list()
    L_chk_manl_rule = 0
    df_tran = pd.DataFrame() #will have one tran-store combination
    df_tran_wh = pd.DataFrame()                   
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/size_profile_queries.yaml') as fh:
            queries            = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_source_dtl    = queries['retreive_alloc_size_details']['Q_source_dtl']
            Q_get_tran_dtl  = queries['retreive_alloc_size_details']['Q_get_tran_dtl']
            Q_get_tran_dtl1 = queries['retreive_alloc_size_details']['Q_get_tran_dtl1']
            Q_tran_dtl      = queries['retreive_alloc_size_details']['Q_tran_dtl']
            Q_tran_new_df   = queries['retreive_alloc_size_details']['Q_tran_new_df']
            Q_result        = queries['retreive_alloc_size_details']['Q_result']
            Q_merged        = queries['retreive_alloc_size_details']['Q_merged']

            mycursor = conn.cursor()
            mycursor.execute("set sql_mode='';")

            size_query = Q_source_dtl.format(I_alloc,I_item_id,I_diff_id,I_location,I_wh,I_order_no)
            #status
            O_status = 1
            df_source_item = pd.read_sql(size_query,conn)
            #------------------------------------------------------------------
            #put source detail into a dummy table
            conn.cursor().execute("DROP TABLE IF EXISTS alloc_src_dtl_dummy;")
            table_name = 'alloc_src_dtl_dummy'
            column_names = list(df_source_item.columns)
            column_types = ['VARCHAR (100)' for dtype in df_source_item.dtypes.tolist()]
            #print("column_types: ",column_types)
            create_table_sql = f"CREATE TEMPORARY TABLE IF NOT EXISTS {table_name} ({','.join([f'{name} {dtype}' for name, dtype in zip(column_names, column_types)])});"
                    
            mycursor.execute(create_table_sql)
            #insert
            sql = f"INSERT INTO {table_name} ({','.join(column_names)}) VALUES ({','.join(['%s']*len(column_names))})"
            data = df_source_item.to_records(index=False).tolist()
            conn.cursor().executemany(sql, data)
            #----------------------------------------------------------------------------
            
            L_source_item  = list(df_source_item["SOURCE_ITEM"])
            L_source_item = np.unique(L_source_item)
           
            #status
            O_status = 2
            if len(L_source_item)>0:
                #status
                O_status = 3
                #at source item level
                for i in L_source_item:
                    #status
                    O_status = 4
                    V_source_item = i
                    df_tran_dtl   = pd.read_sql(Q_get_tran_dtl.format(I_alloc,I_item_id,I_diff_id,I_location,I_wh,I_order_no,V_source_item),conn)
                    L_tran_item   = list(df_tran_dtl["TRAN_ITEM"])
                    
                    #for each tran item
                    for x in L_tran_item:
                        #status
                        O_status = 5
                        L_item = x
                        df_tran_dtl1 = pd.read_sql(Q_get_tran_dtl1.format(I_alloc,I_item_id,I_diff_id,I_location,I_wh,I_order_no,V_source_item,L_item),conn)
                        
                        
                        L_length = len(df_tran_dtl1)
                        temp_df = pd.DataFrame()
                        for r in range(len(df_tran_dtl1)): #at location level
                            #status
                            O_status = 6
                            #df_tran    = pd.DataFrame()
                            L_trn_item = df_tran_dtl1["TRAN_ITEM"][r]
                            L_diff_id  = df_tran_dtl1["DIFF_ID"][r]
                            L_to_loc   = df_tran_dtl1["TO_LOC"][r]
                            L_wh_id    = df_tran_dtl1["WH_ID"][r]
                            #if len(L_to_loc) == 1:
                            #    L_to_loc = tuple(L_to_loc)
                            #    L_to_loc = L_to_loc + (-1,)
                            #else:
                            #    L_to_loc = tuple(df_tran_dtl1['TO_LOC'].values)

                            df_tran_init = pd.read_sql(Q_tran_dtl.format('W',I_alloc,L_trn_item,L_diff_id,L_to_loc,L_wh_id),conn)
                            print(df_tran_init)
                            O_status = 7
                            if len(temp_df)==0:
                                temp_df = df_tran_init
                            else:
                                temp_df = pd.concat([temp_df, df_tran_init])
                            
                            if L_length == (r+1):
                                if len(df_tran)==0:
                                    df_tran = temp_df
                                else:
                                    df_tran = pd.concat([df_tran, temp_df], axis=1)
                        
                        #rename columns
                        #new_cols = {col: col + str(r) for col in df_tran.columns}
                        #df_tran = df_tran.rename(columns=new_cols)
                        df_tran = df_tran.rename(columns={'OH_FF'     : 'OH_FF'   +'_'+str(x),
                                                          'NET_NEED'  : 'NET_NEED'+'_'+str(x),
                                                          'CALC_QTY'  : 'CALC_QTY'+'_'+str(x),
                                                          'REMAIN_QTY': 'REMAIN_QTY'+'_'+str(x)
                                                          })
                        
                        #if len(df_tran_wh)==0:
                        #    #status
                        #    O_status = 7
                        #    df_tran_wh = df_tran
                        #else:
                        #    #status
                        #    O_status = 8
                        #    L_result = [df_tran_wh,df_tran]
                        #    #df_tran_wh = pd.concat([df_tran_wh,df_tran],axis=0)
                        #    df_tran_wh = pd.merge(df_tran_wh, df_tran, how='outer')
                        
                # get list of columns to drop
                duplicates = df_tran.columns[df_tran.columns.duplicated(keep='first')].tolist()
                O_status = 8
                # drop duplicate columns
                df_tran = df_tran.loc[:, ~df_tran.columns.duplicated(keep='first')]

                #------------------------------------------------------------------
                #put tran detail into a dummy table
                conn.cursor().execute("DROP TABLE IF EXISTS alloc_tran_dtl_dummy;")
                table_name = 'alloc_tran_dtl_dummy'
                column_names = list(df_tran.columns)
                column_types = ['VARCHAR (100)' for dtype in df_tran.dtypes.tolist()]
                #print("column_types: ",column_types)
                create_table_sql = f"CREATE TEMPORARY TABLE IF NOT EXISTS {table_name} ({','.join([f'{name} {dtype}' for name, dtype in zip(column_names, column_types)])});"
                    
                mycursor.execute(create_table_sql)
                #-----------------------------------------------
                #insert
                sql = f"INSERT INTO {table_name} ({','.join(column_names)}) VALUES ({','.join(['%s']*len(column_names))})"
                data = df_tran.to_records(index=False).tolist()
                conn.cursor().executemany(sql, data)
                #----------------------------------------------------------------------------
                #print("#################")
                #print(pd.read_sql("select * from alloc_tran_dtl_dummy;",conn).to_dict("records"))
                                
                result = pd.read_sql(Q_result,conn)
                
                #if len(df_tran)>0 and len(df_source_item)>0:
                #    df_source_item = df_source_item.sort_values(by='TO_LOC')
                #    df_source_item = df_source_item.drop_duplicates()
                #    df_source_item = df_source_item.reset_index()
                #    df_source_item = df_source_item.drop(['index'], axis=1)

                #    df_tran = df_tran.sort_values(by='TO_LOC')
                #    df_tran = df_tran.rename(columns={'TO_LOC': 'TO_LOCATION'})
                #    df_tran = df_tran.drop_duplicates()
                #    df_tran = df_tran.reset_index()
                #    df_tran = df_tran.drop(['index'], axis=1)

                #    result = pd.concat([df_source_item,df_tran],axis=1)
                O_status = 9
                #----------------------------------------------------------
                conn.cursor().execute("DROP TABLE IF EXISTS alloc_size_tran_dtl_temp;")
                table_name = 'alloc_size_tran_dtl_temp'
                column_names = list(result.columns)
                column_types = ['VARCHAR (100)' for dtype in result.dtypes.tolist()]
                #print("column_types: ",column_types)
                create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join([f'{name} {dtype}' for name, dtype in zip(column_names, column_types)])});"
                    
                mycursor.execute(create_table_sql)
                mycursor.execute("delete from alloc_size_tran_dtl_temp;")
                #-----------------------------------------------
                #insert
                sql = f"INSERT INTO {table_name} ({','.join(column_names)}) VALUES ({','.join(['%s']*len(column_names))})"
                data = result.to_records(index=False).tolist()
                conn.cursor().executemany(sql, data)
                #-------------------------------------------------------------
                conn.cursor().close()
                conn.commit()

                df_new_tran = pd.read_sql(Q_tran_new_df,conn,params=(I_alloc,))
                result = pd.read_sql(Q_merged,conn)
                return result,df_new_tran,""
                #else:
                #    return emp_list,emp_list

    except Exception as error:
        err_return = ""
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while fetching source item detail: ", error)
            err_return = L_func_name+":"+str(O_status)+": Exception occured while fetching source item detail: "+ str(error)
        elif O_status>=2 or O_status<=4:
            print(L_func_name,":",O_status,":","Exception occured while fetching tran items: ", error)
            err_return = L_func_name+":"+str(O_status)+": Exception occured while fetching tran items: "+ str(error)
        elif O_status>=5 or O_status<=6:
            print(L_func_name,":",O_status,":","Exception occured while fetching tran item detail: ", error)
            err_return = L_func_name+":"+str(O_status)+": Exception occured while fetching tran item detail: "+ str(error)
        elif O_status>=7 or O_status<=8:
            print(L_func_name,":",O_status,":","Exception occured while processing output: ", error)
            err_return = L_func_name+":"+str(O_status)+": Exception occured while processing output: "+ str(error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
            err_return = L_func_name+": Exception occured in: "+ str(error)
        conn.rollback()
        conn.cursor().close()
        return emp_list,emp_list,err_return