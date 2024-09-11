from ..GLOBAL_FILES.get_vdate import get_vdate
import pandas as pd
import yaml

def no_of_skus(conn,
               O_status,
               I_item,
               I_diff_id,
               I_alloc_no,
               I_item_list,
               I_size_prof_ind,
               I_release_date,
               I_uda1,
               I_uda1_val,
               I_uda2,
               I_uda2_val,
               I_uda3,
               I_uda3_val):  
    L_func_name = "no_of_skus"
    try:
        print("inside sku")
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/no_skus_queries.yaml') as fh:
            queries             = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_last_sunday       = queries['no_of_skus']['Q_last_sunday']
            Q_size_prof_week    = queries['no_of_skus']['Q_size_prof_week']
            Q_get_skus_no       = queries['no_of_skus']['Q_get_skus_no']
            Q_diff_temp_1       = queries['no_of_skus']['Q_diff_temp_1']
            O_status            = 0
            mycursor = conn.cursor()

            L_vdate = get_vdate(conn)
            
            df_lst_sunday = pd.read_sql(Q_last_sunday,conn,params=(L_vdate,L_vdate,L_vdate,L_vdate)) #need to get last sunday of the get_vdate 
            L_get_lst_sunday = df_lst_sunday.last_sunday[0]
            print("last sunday",L_get_lst_sunday)

            df_sp_week = pd.read_sql(Q_size_prof_week,conn)
            L_sp_week = int(df_sp_week.CODE_DESC[0])

            mycursor.execute(Q_diff_temp_1,(I_alloc_no,))
            
            df_lst_sunday = pd.read_sql(Q_get_skus_no,conn,params=(I_size_prof_ind,
                                                                   L_get_lst_sunday,
                                                                   L_sp_week,
                                                                   I_release_date,
                                                                   I_release_date,
                                                                   L_get_lst_sunday,
                                                                   I_size_prof_ind,
                                                                   L_get_lst_sunday,
                                                                   I_release_date,
                                                                   I_release_date,
                                                                   L_get_lst_sunday,
                                                                   I_size_prof_ind,
                                                                   I_item,
                                                                   I_item,
                                                                   I_diff_id,
                                                                   I_alloc_no,
                                                                   I_item_list,
                                                                   I_item_list,
                                                                   I_size_prof_ind,
                                                                   I_uda1,
                                                                   I_uda1,
                                                                   I_uda1_val,
                                                                   I_uda2,
                                                                   I_uda2,
                                                                   I_uda2_val,
                                                                   I_uda3,
                                                                   I_uda3,
                                                                   I_uda3_val,))
            L_diff_desc = df_lst_sunday.diff_desc[0]
            L_no_of_skus = df_lst_sunday.no_of_sizes[0]

            if I_item == None and I_diff_id != None:
                L_err = "Invalid Diff group"
                return [],L_func_name+": "+L_err
            else:
                return [L_diff_desc,L_no_of_skus],''
                
    except Exception as error:
        err_return= ''
        if O_status == 1:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while creating temp table: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while creating temp table: ", error)
        elif O_status == 2:
            err_return = L_func_name+":"+str(O_status)+": Exception occured while inserting data into alloc_pack_comp_dtl_temp table : "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured while inserting data into alloc_pack_comp_dtl_temp table : ", error)
        else:
            err_return = L_func_name+":"+str(O_status)+": Exception occured in: "+ str(error)
            #print(L_func_name,":",O_status,":","Exception occured: ", error)
        conn.rollback()
        return [],err_return

