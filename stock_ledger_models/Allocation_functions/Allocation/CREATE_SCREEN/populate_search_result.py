import pandas as pd
import numpy as np
import yaml
from ..GLOBAL_FILES.convert_numpy_64 import convert_numpy
#from ..Global_Files.null_handler import none_to_null
from ..WHATIF_SUMMARY.alloc_wisummary import *
from ..SCHEDULE.setup_schedule_alloc import chk_dup_styl_clr

#############################################################
# Created By - Priyanshu Pandey                             #
# File Name - populate_search_result.py                     #
# Purpose - pupolate items on create screen                 #
#############################################################

#--------------------------------------------------------------
# Function to search items
#--------------------------------------------------------------

def search(conn
           ,I_search_criteria
           ,O_status):
    L_func_name ="search"
    O_status = 0
    emp_lis = list()
    print("EXECUTING: ",L_func_name)
    try:
        L_whatif_source_type_ind = I_search_criteria["WHATIF_SOURCE_TYPE_IND"]
        L_alloc_no = I_search_criteria["ALLOC_NO"]

        if isinstance(L_alloc_no, str) == True or  L_alloc_no==None:
            print("Input allocation number should be numeric")
            return emp_lis,L_func_name+": Input allocation number should be numeric."
        #status
        O_status = 1
        if L_whatif_source_type_ind == 1:
            #status
            O_status = 2
            result1,err_msg1 = common_search(conn,I_search_criteria,True,True, O_status)
            if result1 == False:
                #status
                O_status = 3
                print(O_status,"--common_search")
                return emp_lis ,err_msg1
        else:
            result2,err_msg2 = common_search(conn,I_search_criteria,True,False, O_status)
            if result2 == False:
                #status
                O_status = 4
                print(O_status,"--common_search")
                return emp_lis, err_msg2
        conn.commit()
        #status
        O_status = 5
        L_fetch_inventory,err_msg3 = fetch_inventory(conn,I_search_criteria,O_status)
        if len(L_fetch_inventory) == 0:
            #status
            O_status = 6
            print(O_status,"No records found while executing fetch_inventory")
            conn.rollback()
            return emp_lis ,L_func_name+":"+str(O_status)+": No records found while executing fetch_inventory."
        else:
            conn.commit()
            return L_fetch_inventory,""

    except Exception as error:
        err_return = ""
        if O_status<=2:
            print(L_func_name,":",O_status,":","Exception occured while processing whatif common_search: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing whatif common_search :"+ str(error)
        elif O_status>=3 and O_status<=4:
            print(L_func_name,":",O_status,":","Exception occured while processing common_search: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing common_search :"+ str(error)
        elif O_status<=5 and O_status>=6:
            print(L_func_name,":",O_status,":","Exception occured while processing fetch_inventory: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing fetch_inventory :"+ str(error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(error)
        conn.rollback()        
        return emp_lis,err_return

#--------------------------------------------------------------
# Function to get search items
#--------------------------------------------------------------

def common_search(conn
                  ,I_search_criteria
                  ,I_expand_to_pack
                  ,I_what_if_ind
                  ,O_status):
    L_func_name ="common_search"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        #status
        O_status = 1
        result1,err_msg1 = setup_warehouse(conn,I_search_criteria,I_what_if_ind,O_status)
        if result1 == False:
            #status
            O_status =2 
            print(O_status,"--setup_warehouse")
            return False, err_msg1
        #status
        O_status = 3

        L_pos                 = I_search_criteria["PO"]
        L_asns                = I_search_criteria["ASN"]
        L_tsfs                = I_search_criteria["TSF"]
       
        # po
        O_status = 8
        if (len(L_pos) != 0 or len(L_asns) != 0 or len(L_tsfs) != 0):
            result2,err_msg2 = get_item_locs_from_txns_whs(conn,I_search_criteria, I_expand_to_pack,O_status)
            if result2 == False:
                print(O_status,"--get_item_locs_from_txns_whs")
                conn.cursor().close()
                return False,err_msg2
        else:
        # po
            #status
            O_status = 4
            #find item/loc combinations based on item criteria
            #get txn level items covered by the item criteria and whs
            #will always populate txn level items
            result3,err_msg3 = get_item_locs_from_items_whs(conn,I_search_criteria,O_status)
            if result3 == False:
                #status
                O_status =5
                print(O_status,"--get_item_locs_from_items_whs")
                conn.cursor().close()
                return False,err_msg3
            print("called get_item_locs_from_items_whs")
        if I_expand_to_pack == True:
            result4,err_msg4 = pack_coverd_by_style(conn,O_status)
            if result4 == False:
                O_status = 9
                print(O_status,"--pack_coverd_by_style")
                conn.cursor().close()
                return False,err_msg4
        print("success get_item_locs_from_items_whs")

        result5,err_msg5 = remove_multi_parant_packs(conn,O_status)
        if result5 == False:
            O_status = 9
            print(O_status,"--remove_multi_parant_packs")
            conn.cursor().close()
            return False,err_msg5
        print("success get_item_locs_from_items_whs")

        L_alloc_level = I_search_criteria["ALLOC_LEVEL"]
        #status
        O_status = 6
        #MARK_ROLLUP_GROUP_PACK
        result6,err_msg6 = mark_rollup_group(conn,L_alloc_level,O_status)
        if result6 == False:
            #status
            O_status =7
            print(O_status,"--mark_rollup_group")
            conn.cursor().close()
            return False,err_msg6
        conn.cursor().close()
        return True, ""

    except Exception as error:
        err_return = ""
        if O_status == 8:
            print(L_func_name,":",O_status,":","Exception occured while processing get_item_locs_from_txns_whs: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing get_item_locs_from_txns_whs :"+ str(error)
        elif O_status<=2:
            print(L_func_name,":",O_status,":","Exception occured while processing setup_warehouse: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing setup_warehouse :"+ str(error)
        elif O_status>=3 and O_status<=5:
            print(L_func_name,":",O_status,":","Exception occured while processing get_item_locs_from_items_whs: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing get_item_locs_from_items_whs :"+ str(error)
        elif O_status<=6 and O_status>=7:
            print(L_func_name,":",O_status,":","Exception occured while processing mark_rollup_group: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing mark_rollup_group :"+ str(error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

#--------------------------------------------------------------
# Function to setup warehouse
#--------------------------------------------------------------

def setup_warehouse(conn
                    ,I_search_criteria
                    ,I_what_if_ind
                    ,O_status):
    L_func_name ="setup_warehouse"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/populate_search_result_queries.yaml') as fh:
            queries                = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_create_wh_srch_table = queries['setup_warehouse']['Q_create_wh_srch_table']
            Q_chk_wh_srch_table    = queries['setup_warehouse']['Q_chk_wh_srch_table']
            Q_ins_wh_srch_tbl      = queries['setup_warehouse']['Q_ins_wh_srch_tbl']
            Q_fetch_wh             = queries['setup_warehouse']['Q_fetch_wh']

            mycursor = conn.cursor()
            mycursor.execute("set sql_mode = '';")
            L_input_wh = I_search_criteria["WH"]
            L_input_wh = convert_numpy(L_input_wh)
            #status
            O_status =1

            #Q_chk_wh_srch_table
            df_chk = pd.read_sql(Q_chk_wh_srch_table,conn)
            L_chk = df_chk.chk[0]

            #status
            O_status =2

            if L_chk == 1:
                #status
                O_status =3
                print("alloc_whs_search_temp table already exists")
                print(O_status,L_func_name)

            else: 
                #status
                O_status =4
                mycursor.execute(Q_create_wh_srch_table)

            #status
            O_status =5

            if len(L_input_wh) !=0:
                #status
                O_status =6
                for i in range(len(L_input_wh)):
                    L_wh = L_input_wh[i]

                    mycursor.execute(Q_ins_wh_srch_tbl,(L_wh,))
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            else:
                #status
                O_status =7
                df_wh = pd.read_sql(Q_fetch_wh,conn)
                if len(df_wh)>0:
                    for i in range(len(df_wh)):
                        L_wh = df_wh.wh[i]
                        mycursor.execute(Q_ins_wh_srch_tbl,(L_wh,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            #status
            O_status =8
            conn.commit()
            conn.cursor().close()
            return True, ""


    except Exception as error:
        err_return = ""
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_chk_wh_srch_table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_chk_wh_srch_table :"+ str(error)
        elif O_status==3:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_drop_wh_srch_tbl: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_drop_wh_srch_tbl :"+ str(error)
        elif O_status==4:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_create_wh_srch_table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_create_wh_srch_table :"+ str(error)
        elif O_status>4 and O_status<=7:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_ins_wh_srch_tbl: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_ins_wh_srch_tbl :"+ str(error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured :"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

#--------------------------------------------------------------
# Function to get all items based on input
#--------------------------------------------------------------

def get_item_locs_from_items_whs(conn,
                                 I_search_criteria,
                                 O_status):
    L_func_name ="get_item_locs_from_items_whs"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/populate_search_result_queries.yaml') as fh:
            queries                = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_hier3_sku     = queries['get_item_locs_from_items_whs']['Q_hier3_sku']
            Q_hier2_sku     = queries['get_item_locs_from_items_whs']['Q_hier2_sku']
            Q_hier1_sku     = queries['get_item_locs_from_items_whs']['Q_hier1_sku']
            Q_input_item    = queries['get_item_locs_from_items_whs']['Q_input_item']
            Q_tran_sku      = queries['get_item_locs_from_items_whs']['Q_tran_sku']
            #pack_no
            Q_pack_no       = queries['get_item_locs_from_items_whs']['Q_pack_no']
            Q_itemlist_sku  = queries['get_item_locs_from_items_whs']['Q_itemlist_sku']
            Q_supplier_sku  = queries['get_item_locs_from_items_whs']['Q_supplier_sku']
            Q_supp_site_sku = queries['get_item_locs_from_items_whs']['Q_supp_site_sku']
            Q_vpn_sku       = queries['get_item_locs_from_items_whs']['Q_vpn_sku']
            Q_uda_sku       = queries['get_item_locs_from_items_whs']['Q_uda_sku']
            Q_excl_uda_sku  = queries['get_item_locs_from_items_whs']['Q_excl_uda_sku']
            Q_uda_val_sku   = queries['get_item_locs_from_items_whs']['Q_uda_val_sku']
            Q_excl_uda_val  = queries['get_item_locs_from_items_whs']['Q_excl_uda_val']

            #style/diff
            Q_itemlist_style = queries['get_item_locs_from_items_whs']['Q_itemlist_style']
            Q_vpn_style      = queries['get_item_locs_from_items_whs']['Q_vpn_style']
            Q_uda_style      = queries['get_item_locs_from_items_whs']['Q_uda_style']
            Q_uda_val_style  = queries['get_item_locs_from_items_whs']['Q_uda_val_style']


            #table 
            Q_create_item_loc_srch_table  = queries['get_item_locs_from_items_whs']['Q_create_item_loc_srch_table']
            Q_chk_item_loc_srch_table     = queries['get_item_locs_from_items_whs']['Q_chk_item_loc_srch_table']
            Q_ins_search_item_loc         = queries['get_item_locs_from_items_whs']['Q_ins_search_item_loc']
            Q_create_item_srch_table      = queries['get_item_locs_from_items_whs']['Q_create_item_srch_table']
            Q_chk_item_srch_table         = queries['get_item_locs_from_items_whs']['Q_chk_item_srch_table']
            Q_ins_search_item             = queries['get_item_locs_from_items_whs']['Q_ins_search_item']
            Q_fetch_item_loc_srch         = queries['get_item_locs_from_items_whs']['Q_fetch_item_loc_srch']

            mycursor = conn.cursor()

            L_item_rec         = list()
            L_hier_item_rec    = list() #for hier
            L_sup_item_rec     = list() #for L_supplier
            L_supsite_item_rec = list() #for L_supplier_site
            L_pack_item_rec    = list() #for L_pack_no
            L_parent_item_rec  = list() #for L_item_parent/L_item_grandparent/L_sku
            L_sku_item_rec     = list() #for L_sku
            L_pack_item_rec    = list() #for L_pack_no
            L_list_item_rec    = list() #for L_item_list
            L_vpn_item_rec     = list() #for L_vpn
            L_uda_item_rec     = list() #for L_uda
            L_uda_val_item_rec = list() #for L_uda_value


            #status
            O_status = 1 

            L_alloc_level      = I_search_criteria["ALLOC_LEVEL"]
            L_hier1            = I_search_criteria["HIER1"]
            L_hier2            = I_search_criteria["HIER2"]
            L_hier3            = I_search_criteria["HIER3"]
            L_supplier         = I_search_criteria["SUPPLIER"]
            L_supplier_site    = I_search_criteria["SUPPLIER_SITE"]
            L_pack_no          = I_search_criteria["PACK_NO"]
            L_item_parent      = I_search_criteria["ITEM_PARENT"]
            L_sku              = I_search_criteria["SKU"]
            L_item_grandparent = I_search_criteria["ITEM_GRANDPARENT"]
            L_item_list        = I_search_criteria["ITEM_LIST"]
            L_vpn              = I_search_criteria["VPN"]
            L_uda              = I_search_criteria["UDA"]
            L_uda_value        = I_search_criteria["UDA_VALUE"]
            L_excl_uda         = I_search_criteria["EXCLUDE_UDA"]
            L_excl_uda_val     = I_search_criteria["EXCLUDE_UDA_VALUE"]

            #appending None to execute IN operator in queries
            if len(L_hier1) == 1:
                L_hier1.append(-1)
            if len(L_hier2) == 1:
                L_hier2.append(-1)
            if len(L_hier3) == 1:
                L_hier3.append(-1)
            if len(L_supplier) == 1:
                L_supplier.append(-1)
            if len(L_supplier_site) == 1:
                L_supplier_site.append(-1)
            #pack_no
            if len(L_pack_no) == 1:
                L_pack_no.append(-1)
            if len(L_item_parent) == 1:
                L_item_parent.append(-1)
            if len(L_sku) == 1:
                L_sku.append(-1)
            if len(L_item_grandparent) == 1:
                L_item_grandparent.append(-1)
            if len(L_item_list) == 1:
                L_item_list.append(-1)
            if len(L_vpn) == 1:
                L_vpn.append(-1)
            if len(L_uda) == 1:
                L_uda.append(-1)
            if len(L_uda_value) == 1:
                L_uda_value.append(-1)
            if len(L_excl_uda) == 1:
                L_excl_uda.append(-1)
            if len(L_excl_uda_val) == 1:
                L_excl_uda_val.append(-1)

            #convert list into tuple
            L_hier1            = convert_numpy(L_hier1)
            L_hier2            = convert_numpy(L_hier2)
            L_hier3            = convert_numpy(L_hier3)
            L_supplier         = convert_numpy(L_supplier)
            L_supplier_site    = convert_numpy(L_supplier_site)
            L_item_parent      = convert_numpy(L_item_parent)
            L_sku              = convert_numpy(L_sku)
            #pack_no
            L_pack_no          = convert_numpy(L_pack_no)
            L_item_grandparent = convert_numpy(L_item_grandparent)
            L_item_list        = convert_numpy(L_item_list)
            L_vpn              = convert_numpy(L_vpn)
            L_uda              = convert_numpy(L_uda)
            L_uda_value        = convert_numpy(L_uda_value)
            L_excl_uda         = convert_numpy(L_excl_uda)
            L_excl_uda_val     = convert_numpy(L_excl_uda_val)

            #convert none to null
            #L_hier1            = none_to_null(L_hier1)
            #L_hier2            = none_to_null(L_hier2)
            #L_hier3            = none_to_null(L_hier3)
            #L_supplier         = none_to_null(L_supplier)
            #L_supplier_site    = none_to_null(L_supplier_site)
            #L_item_parent      = none_to_null(L_item_parent)
            #L_sku              = none_to_null(L_sku)
            #L_item_grandparent = none_to_null(L_item_grandparent)
            #L_item_list        = none_to_null(L_item_list)
            #L_vpn              = none_to_null(L_vpn)
            #L_uda              = none_to_null(L_uda)
            #L_uda_value        = none_to_null(L_uda_value)
            #L_excl_uda         = none_to_null(L_excl_uda)
            #L_excl_uda_val     = none_to_null(L_excl_uda_val)
            #status
            O_status = 2
            
            #if L_alloc_level =='T':  #style/diff
            if len(L_hier1) != 0 and len(L_hier2) !=0 and len(L_hier3) != 0:
                #status
                O_status = 3

                #Q_hier3_sku
                mycursor.execute(Q_hier3_sku.format(L_hier1,L_hier2,L_hier3))
                L_result = mycursor.fetchall()

                #status
                O_status = 4
                if len(L_result)>0:
                    #status
                    O_status = 5
                    for i in range(len(L_result)):
                        Li_item = L_result[i]
                        Li_item = Li_item[0]
                        L_hier_item_rec.append(Li_item)

            elif len(L_hier1) != 0 and len(L_hier2) !=0:
                #status
                O_status = 6

                #Q_hier2_sku
                mycursor.execute(Q_hier2_sku.format(L_hier1,L_hier2))
                L_result = mycursor.fetchall()

                #status
                O_status = 7
                if len(L_result)>0:
                    #status
                    O_status = 8
                    for i in range(len(L_result)):
                        Li_item = L_result[i]
                        Li_item = Li_item[0]
                        L_hier_item_rec.append(Li_item)
                    
            elif len(L_hier1) != 0:

                #status
                O_status = 9
                #Q_hier1_sku
                mycursor.execute(Q_hier1_sku.format(L_hier1))
                L_result = mycursor.fetchall()
                #status
                O_status = 10

                if len(L_result)>0:
                    for i in range(len(L_result)):
                        Li_item = L_result[i]
                        Li_item = Li_item[0]
                        L_hier_item_rec.append(Li_item)

            #status
            O_status = 11
            L_item_rec = L_hier_item_rec

            if len(L_item_parent) !=0 or len(L_item_grandparent) != 0 or len(L_sku) != 0:

                L_null = -1
                if len(L_sku) == 0:
                    L_sku = L_sku + (L_null,) + (L_null,)
                if len(L_item_parent) == 0:
                    L_item_parent =  L_item_parent + (L_null,) + (L_null,)
                if len(L_item_grandparent) == 0:
                    L_item_grandparent =  (L_null,) + (L_null,)
                
                #status
                O_status = 12
                #Q_input_item
                mycursor.execute(Q_input_item.format(L_sku,L_item_parent,L_item_grandparent,L_item_parent))
                L_result = mycursor.fetchall()
                #status
                O_status = 13

                if len(L_result)>0:
                    for i in range(len(L_result)):
                        Li_item = L_result[i]
                        Li_item = Li_item[0]
                        L_parent_item_rec.append(Li_item)
                #intersection
                if len(L_item_rec)==0:
                    L_item_rec = L_parent_item_rec
                else:
                    L_item_rec = list(set(L_hier_item_rec).intersection(set(L_parent_item_rec)))

            #status
            O_status = 14

            if len(L_sku) != 0 and L_alloc_level == 'T': #style/diff
                #status
                O_status = 12.1
                #Q_input_item
                mycursor.execute(Q_tran_sku.format(L_sku,L_sku))
                L_result = mycursor.fetchall()
                #status
                O_status = 13.1

                if len(L_result)>0:
                    for i in range(len(L_result)):
                        Li_item = L_result[i]
                        Li_item = Li_item[0]
                        L_sku_item_rec.append(Li_item)
                #intersection
                if len(L_item_rec)==0:
                    L_item_rec = L_sku_item_rec
                else:
                    L_item_rec = list(set(L_hier_item_rec).intersection(set(L_sku_item_rec)))

            #PackNo
            if len(L_pack_no) != 0 and L_alloc_level == 'T': #style/diff
                #status
                O_status = 12.1
                #Q_input_item
                print("input data ::",Q_pack_no.format(L_pack_no))
                mycursor.execute(Q_pack_no.format(L_pack_no))
                L_result = mycursor.fetchall()
                #status
                O_status = 13.1

                if len(L_result)>0:
                    for i in range(len(L_result)):
                        Li_item = L_result[i]
                        Li_item = Li_item[0]
                        L_pack_item_rec.append(Li_item)
                #intersection
                if len(L_item_rec)==0:
                    L_item_rec = L_pack_item_rec
                else:
                    L_item_rec = list(set(L_hier_item_rec).intersection(set(L_pack_item_rec)))

            #status
            O_status = 14.1

            if len(L_item_list) !=0 and L_alloc_level == 'T':  #style/diff
                #status
                O_status = 15
                #Q_itemlist_sku
                mycursor.execute(Q_itemlist_sku.format(L_item_list))
                L_result = mycursor.fetchall()
                #status
                O_status = 16

                if len(L_result)>0:
                    for i in range(len(L_result)):
                        Li_item = L_result[i]
                        Li_item = Li_item[0]
                        L_list_item_rec.append(Li_item)
                #intersection
                if len(L_item_rec)==0:
                    L_item_rec = L_list_item_rec
                else:
                    L_item_rec = list(set(L_item_rec).intersection(set(L_list_item_rec)))

            elif len(L_item_list) !=0 and L_alloc_level == 'D':  #style/diff
                 #status
                 O_status = 15
                 #Q_itemlist_sku
                 mycursor.execute(Q_itemlist_style.format(L_item_list,L_item_list,L_item_list,L_item_list,L_item_list))
                 L_result = mycursor.fetchall()
                 #status
                 O_status = 16
                 
                 if len(L_result)>0:
                     for i in range(len(L_result)):
                         Li_item = L_result[i]
                         Li_item = Li_item[0]
                         L_list_item_rec.append(Li_item)
                 #intersection
                 if len(L_item_rec)==0:
                     L_item_rec = L_list_item_rec
                 else:
                     L_item_rec = list(set(L_item_rec).intersection(set(L_list_item_rec)))

            #status
            O_status = 17

            if len(L_supplier) !=0:
                #status
                O_status = 18
                #Q_supplier_sku
                mycursor.execute(Q_supplier_sku.format(L_supplier))
                L_result = mycursor.fetchall()
                #status
                O_status = 19

                if len(L_result)>0:
                    for i in range(len(L_result)):
                        Li_item = L_result[i]
                        Li_item = Li_item[0]
                        L_sup_item_rec.append(Li_item)

                #intersection
                if len(L_item_rec)==0:
                    L_item_rec = L_sup_item_rec
                else:
                    L_item_rec = list(set(L_item_rec).intersection(set(L_sup_item_rec)))

            #status
            O_status = 20
            if len(L_supplier_site) !=0:
                #status
                O_status = 21
                #Q_supp_site_sku
                mycursor.execute(Q_supp_site_sku.format(L_supplier_site))
                L_result = mycursor.fetchall()
                #status
                O_status = 22

                if len(L_result)>0:
                    for i in range(len(L_result)):
                        Li_item = L_result[i]
                        Li_item = Li_item[0]
                        L_supsite_item_rec.append(Li_item)

                #intersection
                if len(L_item_rec)==0:
                    L_item_rec = L_supsite_item_rec
                else:
                    L_item_rec = list(set(L_item_rec).intersection(set(L_supsite_item_rec)))

            #status
            O_status = 23
            if len(L_vpn) !=0 and L_alloc_level == 'T':    #style/diff
                #status
                O_status = 24
                #Q_vpn_sku
                mycursor.execute(Q_vpn_sku.format(L_vpn))
                L_result = mycursor.fetchall()
                #status
                O_status = 25

                if len(L_result)>0:
                    for i in range(len(L_result)):
                        Li_item = L_result[i] 
                        Li_item = Li_item[0]
                        L_vpn_item_rec.append(Li_item)

                #intersection
                if len(L_item_rec)==0:
                    L_item_rec = L_vpn_item_rec
                else:
                    L_item_rec = list(set(L_item_rec).intersection(set(L_vpn_item_rec)))
            elif len(L_vpn) !=0 and L_alloc_level == 'D':  #style/diff
                 #status
                 O_status = 24
                 #Q_vpn_sku
                 mycursor.execute(Q_vpn_style.format(L_vpn))
                 L_result = mycursor.fetchall()
                 #status
                 O_status = 25
                 
                 if len(L_result)>0:
                     for i in range(len(L_result)):
                         Li_item = L_result[i]
                         Li_item = Li_item[0]
                         L_vpn_item_rec.append(Li_item)
                 
                 #intersection
                 if len(L_item_rec)==0:
                     L_item_rec = L_vpn_item_rec
                 else:
                     L_item_rec = list(set(L_item_rec).intersection(set(L_vpn_item_rec)))

            #status
            O_status = 26
            if len(L_uda) !=0 and L_alloc_level == 'T':  #style/diff
               #status
               O_status = 27
               #Q_uda_sku
               mycursor.execute(Q_uda_sku.format(L_uda))
               L_result = mycursor.fetchall()
               #status
               O_status = 28

               if len(L_result)>0:
                   for i in range(len(L_result)):
                       Li_item = L_result[i]
                       Li_item = Li_item[0]
                       L_uda_item_rec.append(Li_item)

               #intersection
               if len(L_item_rec)==0:
                   L_item_rec = L_uda_item_rec
               else:
                    L_item_rec = list(set(L_item_rec).intersection(set(L_uda_item_rec)))
            elif len(L_uda) !=0 and L_alloc_level == 'D':  #style/diff
                 #status
                 O_status = 27
                 #Q_uda_sku
                 mycursor.execute(Q_uda_style.format(L_uda))
                 L_result = mycursor.fetchall()
                 #status
                 O_status = 28
                 
                 if len(L_result)>0:
                     for i in range(len(L_result)):
                         Li_item = L_result[i]
                         Li_item = Li_item[0]
                         L_uda_item_rec.append(Li_item)
                 
                 #intersection
                 if len(L_item_rec)==0:
                     L_item_rec = L_uda_item_rec
                 else:
                      L_item_rec = list(set(L_item_rec).intersection(set(L_uda_item_rec)))

            #status
            O_status = 29
            if len(L_uda_value) !=0 and L_alloc_level == 'T':  #style/diff
                #status
                O_status = 30
                #Q_uda_val_sku
                mycursor.execute(Q_uda_val_sku.format(L_uda,L_uda_value))
                L_result = mycursor.fetchall()
                #status
                O_status = 31

                if len(L_result)>0:
                    for i in range(len(L_result)):
                        Li_item = L_result[i]
                        Li_item = Li_item[0]
                        L_uda_val_item_rec.append(Li_item)

                #intersection
                if len(L_item_rec)==0:
                    L_item_rec = L_uda_val_item_rec
                else:
                    L_item_rec = list(set(L_item_rec).intersection(set(L_uda_val_item_rec)))
            elif len(L_uda_value) !=0 and L_alloc_level == 'D':  #style/diff
                 #status
                 O_status = 30
                 #Q_uda_val_sku
                 mycursor.execute(Q_uda_val_style.format(L_uda,L_uda_value))
                 L_result = mycursor.fetchall()
                 #status
                 O_status = 31
                 
                 if len(L_result)>0:
                     for i in range(len(L_result)):
                         Li_item = L_result[i]
                         Li_item = Li_item[0]
                         L_uda_val_item_rec.append(Li_item)
                 
                 #intersection
                 if len(L_item_rec)==0:
                     L_item_rec = L_uda_val_item_rec
                 else:
                     L_item_rec = list(set(L_item_rec).intersection(set(L_uda_val_item_rec)))

            #take common items
            #L_item_rec = list(set(list1).intersection(set(list2)).intersection(set(list3)))
            #status
            O_status = 32
            if L_alloc_level == 'T' and (len(L_excl_uda) !=0 or len(L_excl_uda_val) ==0):  #style/diff
                if len(L_excl_uda) !=0 and len(L_excl_uda_val) ==0:
                    #status
                    O_status = 33
                    #Q_excl_uda_sku
                    mycursor.execute(Q_excl_uda_sku.format(L_excl_uda))
                    L_result = mycursor.fetchall()
                    #status
                    O_status = 34

                    if len(L_result)>0:
                        for i in range(len(L_result)):
                            Li_item = L_result[i]
                            Li_item = Li_item[0]
                            if Li_item in L_item_rec:
                                L_item_rec.remove(Li_item)

                #status
                O_status = 35
                if len(L_excl_uda_val) !=0:
                    #status
                    O_status = 36
                    mycursor.execute(Q_excl_uda_val.format(L_excl_uda,L_excl_uda_val))
                    L_result = mycursor.fetchall()
                    #status
                    O_status = 37

                    if len(L_result)>0:
                        for i in range(len(L_result)):
                            Li_item = L_result[i]
                            Li_item = Li_item[0]
                            if Li_item in L_item_rec:
                                L_item_rec.remove(Li_item)

            #creating tables to insert all data
            #Q_chk_item_loc_srch_table
            df_chk = pd.read_sql(Q_chk_item_loc_srch_table,conn)
            L_chk = df_chk.chk[0]

            #status
            O_status =38

            if L_chk == 1:
                #status
                O_status =39
                print("alloc_search_criteria_itm_temp table already exists")
                print(O_status,L_func_name)
                conn.cursor().close()
                return False, L_func_name+":"+str(O_status)+": "+"alloc_search_criteria_itm_temp table already exists."

            else: 
                #status
                O_status =40
                mycursor.execute(Q_create_item_loc_srch_table)

            #Q_chk_item_srch_table
            df_chk = pd.read_sql(Q_chk_item_srch_table,conn)
            L_chk = df_chk.chk[0]

            #status
            O_status =41

            if L_chk >0:
                #status
                O_status =42
                print("alloc_items_srch_temp table already exists")
                print(O_status,L_func_name)
                return False,L_func_name+":"+str(O_status)+": "+"alloc_items_srch_temp table already exists."

            else: 
                #status
                O_status =43

                mycursor.execute(Q_create_item_srch_table)

            #insering data
            L_unique_item_rec = np.unique(L_item_rec)
            if len(L_unique_item_rec)>0:
                #status
                O_status =44
                #changes by shubham start#
                #for i in range(len(L_unique_item_rec)): 
                #    L_insert = L_unique_item_rec[i]
                #    mycursor.execute(Q_ins_search_item,(L_insert,))
                #    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                df = pd.DataFrame(L_unique_item_rec)
                data = df.to_records(index=False).tolist()
                mycursor.executemany(Q_ins_search_item, data)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                #changes by shubham end#



                #Q_fetch_item_loc_srch
                #status
                O_status =45
                #changes by shubham start#
                #df_item_loc = pd.read_sql(Q_fetch_item_loc_srch,conn)
                #if len(df_item_loc)>0:
                #    for i in range(len(df_item_loc)):
                #        L_item = df_item_loc.item[i]
                #        L_loc = df_item_loc.wh[i]
                #        #Q_ins_search_item_loc
                #        mycursor.execute(Q_ins_search_item_loc,(L_item,L_loc))
                #        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                mycursor.execute(Q_ins_search_item_loc)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                #changes by shubham end#
            else:
                print("else")
                #status
                O_status =46
                print(L_func_name,O_status,": No record found")
                print(O_status)

            conn.commit()
            conn.cursor().close()
            return True, ""

    except Exception as error:
        err_return = ""
        if O_status<=4:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_hier3_sku: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_hier3_sku :"+ str(error)
        elif O_status>=5 and O_status<=7:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_hier2_sku: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_hier2_sku :"+ str(error)
        elif O_status<=8 and O_status>=10:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_hier1_sku: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_hier1_sku :"+ str(error)
        elif O_status<=11 and O_status>=13:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_input_item: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_input_item :"+ str(error)
        elif O_status<=14 and O_status>=16:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_itemlist_sku: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_itemlist_sku :"+ str(error)
        elif O_status<=17 and O_status>=19:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_supplier_sku: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_supplier_sku :"+ str(error)
        elif O_status<=20 and O_status>=22:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_supp_site_sku: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_supp_site_sku :"+ str(error)
        elif O_status<=23 and O_status>=25:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_vpn_sku: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_vpn_sku :"+ str(error)
        elif O_status<=26 and O_status>=28:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_uda_sku: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_uda_sku :"+ str(error)
        elif O_status<=29 and O_status>=31:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_uda_val_sku: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_uda_val_sku :"+ str(error)
        elif O_status<=32 and O_status>=34:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_excl_uda_sku: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_excl_uda_sku :"+ str(error)
        elif O_status<=35 and O_status>=37:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_excl_uda_val: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_excl_uda_val :"+ str(error)
        elif O_status<=38 and O_status>=39:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_drop_item_loc_srch_tbl: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_drop_item_loc_srch_tbl :"+ str(error)
        elif O_status==40:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_create_item_loc_srch_table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_create_item_loc_srch_table :"+ str(error)
        elif O_status<=41 and O_status>=43:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_create_item_srch_table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_create_item_srch_table :"+ str(error)
        elif O_status==44:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_ins_search_item: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_ins_search_item :"+ str(error)
        elif O_status==45:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_fetch_item_loc_srch: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_fetch_item_loc_srch :"+ str(error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

#--------------------------------------------------------------
# Function to insert rollup items
#--------------------------------------------------------------

def mark_rollup_group(conn,
                      L_alloc_level,
                      O_status):
    L_func_name ="mark_rollup_group"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/populate_search_result_queries.yaml') as fh:
            queries                  = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_ins_itm_srch_tmp       = queries['mark_rollup_group']['Q_ins_itm_srch_tmp']
            Q_create_item_srch_table = queries['get_item_locs_from_items_whs']['Q_create_item_srch_table']
            Q_chk_item_srch_table    = queries['get_item_locs_from_items_whs']['Q_chk_item_srch_table']
            Q_del_itm_srch_tmp       = queries['mark_rollup_group']['Q_del_itm_srch_tmp']

            Q_ins_style_itm_srch_tmp = queries['mark_rollup_group']['Q_ins_style_itm_srch_tmp']
            #status
            O_status =1
            mycursor = conn.cursor()

            #Q_chk_item_srch_table
            df_chk = pd.read_sql(Q_chk_item_srch_table,conn)
            L_chk = df_chk.chk[0]

            #status
            O_status =2

            if L_chk >0:
                #status
                O_status =2
                print("alloc_items_srch_temp table already exists")
                print(O_status,L_func_name)
                return False, L_func_name+":"+str(O_status)+": alloc_items_srch_temp table already exists."
            else: 
                #status
                O_status =3

                mycursor.execute(Q_create_item_srch_table)

            #o_ststua
            O_status = 4
            mycursor.execute(Q_del_itm_srch_tmp)
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            if L_alloc_level == 'T': #style/diff
                mycursor.execute(Q_ins_itm_srch_tmp)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            elif L_alloc_level == 'D': #style/diff
                mycursor.execute(Q_ins_style_itm_srch_tmp)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                
            conn.commit()
            conn.cursor().close()
            return True, ""

    except Exception as error:
        err_return = ""
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_chk_item_srch_table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_chk_item_srch_table :"+ str(error)
        elif O_status==2:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_drop_item_srch_tbl: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_drop_item_srch_tbl :"+ str(error)
        elif O_status==3:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_create_item_srch_table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_create_item_srch_table :"+ str(error)
        elif O_status==4:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_ins_itm_srch_tmp: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_ins_itm_srch_tmp :"+ str(error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured :"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False, err_return

#--------------------------------------------------------------
# Function to populate item and location combination with inv.
#--------------------------------------------------------------

def fetch_inventory(conn,
                    I_search_criteria,
                    O_status):
    L_func_name ="fetch_inventory"
    O_status = 0
    L_item_in_rec  = list()
    L_style_diffid = list()
    emp_list= list()
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/populate_search_result_queries.yaml') as fh:
            queries                   = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_create_itm_srch_tbl     = queries['fetch_inventory']['Q_create_itm_srch_tbl']
            Q_fetch_diff_items        = queries['fetch_inventory']['Q_create_itm_srch_tbl']
            Q_fetch_diff_items        = queries['fetch_inventory']['Q_fetch_diff_items']
            Q_fetch_vpn_items         = queries['fetch_inventory']['Q_fetch_vpn_items']
            Q_del_ext_item            = queries['fetch_inventory']['Q_del_ext_item']
            Q_ins_itm_srch_dtl_tmp    = queries['fetch_inventory']['Q_ins_itm_srch_dtl_tmp']
            Q_ins_itm_srch_dtl_whatif = queries['fetch_inventory']['Q_ins_itm_srch_dtl_whatif']
            Q_ins_itm_srch_dtl        = queries['fetch_inventory']['Q_ins_itm_srch_dtl']
            Q_del_item_srch_dtl       = queries['fetch_inventory']['Q_del_item_srch_dtl']
            Q_del_item_srch_dtl_tmp   = queries['fetch_inventory']['Q_del_item_srch_dtl_tmp']
            Q_fetch_items             = queries['fetch_inventory']['Q_fetch_items']

            Q_crte_tbl_po_no          = queries['get_po_source_inv']['Q_crte_tbl_po_no']
            Q_ins_srch_po_no          = queries['get_po_source_inv']['Q_ins_srch_po_no']
            Q_crte_tbl_po_type        = queries['get_po_source_inv']['Q_crte_tbl_po_type']
            Q_ins_srch_po_type        = queries['get_po_source_inv']['Q_ins_srch_po_type']
            Q_ins_wi_item_loc         = queries['get_po_source_inv']['Q_ins_wi_item_loc']
            Q_ins_item_search         = queries['get_po_source_inv']['Q_ins_item_search']
            Q_ins_item_srch_wia       = queries['get_po_source_inv']['Q_ins_item_srch_wia']
            Q_ins_item_srch_pos       = queries['get_po_source_inv']['Q_ins_item_srch_pos']
            Q_fetch_srch_po_no        = queries['get_po_source_inv']['Q_fetch_srch_po_no']     
            Q_del_srch_po_no          = queries['get_po_source_inv']['Q_del_srch_po_no']

            Q_ins_item_srch_tsf       = queries['get_tsf_source_inv']['Q_ins_item_srch_tsf']

            Q_ins_item_srch_asn       = queries['get_asn_source_inv']['Q_ins_item_srch_asn']
            Q_del_srch_asn_no         = queries['get_po_source_inv']['Q_del_srch_asn_no']

            #style/diff
            Q_style_sku                  = queries['get_item_locs_from_items_whs']['Q_style_sku']
            Q_ins_style_itm_srch_dtl_tmp = queries['fetch_inventory']['Q_ins_style_itm_srch_dtl_tmp']
            Q_ins_style_itm_srch_dtl_wis = queries['fetch_inventory']['Q_ins_style_itm_srch_dtl_wis']
            Q_ins_style_item_srch_tsf    = queries['get_tsf_source_inv']['Q_ins_style_item_srch_tsf']
            Q_ins_style_item_srch_asn    = queries['get_asn_source_inv']['Q_ins_style_item_srch_asn']
            Q_ins_style_item_srch        = queries['get_po_source_inv']['Q_ins_style_item_srch']
            Q_ins_style_item_srch_wia    = queries['get_po_source_inv']['Q_ins_style_item_srch_wia']
            Q_ins_style_item_srch_pos    = queries['get_po_source_inv']['Q_ins_style_item_srch_pos']

            #pack_ind
            Q_del_non_pack_items         = queries['fetch_inventory']['Q_del_non_pack_items']

            mycursor = conn.cursor()
            #status
            O_status = 1

            L_wh_source_type_ind  = I_search_criteria["WH_SOURCE_TYPE_IND"]
            L_wi_source_type_ind  = I_search_criteria["WHATIF_SOURCE_TYPE_IND"]
            L_po_source_type_ind  = I_search_criteria["PO_SOURCE_TYPE_IND"]
            L_asn_source_type_ind = I_search_criteria["ASN_SOURCE_TYPE_IND"]
            L_tsf_source_type_ind = I_search_criteria["TSF_SOURCE_TYPE_IND"]
            L_alloc_no            = I_search_criteria["ALLOC_NO"]
            L_pos                 = I_search_criteria["PO"]
            L_asns                = I_search_criteria["ASN"]
            L_tsfs                = I_search_criteria["TSF"] 
            L_po_type             = I_search_criteria["PO_TYPE"]
            L_alloc_criteria      = I_search_criteria["ALLOC_CRITERIA"]
            L_alloc_type          = I_search_criteria["ALLOC_TYPE"]
            L_alloc_level         = I_search_criteria["ALLOC_LEVEL"]
            L_clearance_ind       = I_search_criteria["CLEARANCE_IND"]
            L_alloc_vpn           = I_search_criteria["VPN"]
            L_min_avail_qty       = I_search_criteria["MIN_AVAIL_QTY"]
            L_max_avail_qty       = I_search_criteria["MAX_AVAIL_QTY"]
            L_diff_id             = I_search_criteria["DIFF_ID"]
            # po
            L_start_date          = I_search_criteria["START_DATE"]
            L_end_date            = I_search_criteria["END_DATE"]
            L_esid_start_date     = I_search_criteria["EISD_START_DATE"]
            L_esid_end_date       = I_search_criteria["EISD_END_DATE"]
                                  
            L_style_sku           = I_search_criteria["SKU"]

            #pack_ind
            L_pack_ind            = I_search_criteria["PACK_IND"]

            if len(L_style_sku) == 1:
                L_style_sku.append(-1)
				
            L_style_sku  = convert_numpy(L_style_sku)

            mycursor.execute(Q_crte_tbl_po_no) 

            if len(L_pos) != 0:
                print(L_pos,"L_pos") 
                for i in range(len(L_pos)):
                    L_alloc_pos = L_pos[i]
                    O_status = 20
                    mycursor.execute(Q_ins_srch_po_no.format(L_alloc_pos,))  
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            if len(L_tsfs) != 0:
                print(L_tsfs,"L_tsfs") 
                for i in range(len(L_tsfs)):
                    L_alloc_tsfs = L_tsfs[i]
                    O_status = 21
                    mycursor.execute(Q_ins_srch_po_no.format(L_alloc_tsfs,))  
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            if len(L_asns) != 0:
                print(L_asns,"L_asns") 
                for i in range(len(L_asns)):
                    L_alloc_asns = L_asns[i]
                    O_status = 22
                    print(L_alloc_asns,"L_alloc_asns")
                    mycursor.execute(Q_ins_srch_po_no.format(L_alloc_asns,))  
                    print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            mycursor.execute(Q_crte_tbl_po_type)
            mycursor.execute(Q_ins_srch_po_type,(L_po_type,)) 
            

            #status
            O_status = 2            
            #creating table
            mycursor.execute(Q_create_itm_srch_tbl)

            if L_alloc_level == 'D' and len(L_style_sku) > 0:
                df_style_sku = pd.read_sql(Q_style_sku.format(L_style_sku),conn)
                if len(df_style_sku) > 0:
                    for i in range(len(df_style_sku)):
                        L_style_diff = df_style_sku.diff_id[i]
                        L_style_diffid.append(L_style_diff)
                        print(L_style_diffid,"L_style_diffid")

            if len(L_style_diffid)>0:
                L_unique_diff = np.unique(L_style_diffid)
                L_unique_diff = convert_numpy(L_style_diffid)
                if len(L_unique_diff) == 1:
                    L_unique_diff =  L_unique_diff + (-1,)

            O_status = 3
            if len(L_alloc_vpn) !=0:
                #status
                O_status = 4
                for i in range(len(L_alloc_vpn)):
                    L_vpn = L_alloc_vpn[i]
                    #Q_fetch_vpn_items
                    df_vpn_item = pd.read_sql(Q_fetch_vpn_items,conn,params=(L_vpn,))
                    if len(df_vpn_item)>0:
                        for i in range(len(df_vpn_item)):
                            L_item = df_vpn_item.item[i]
                            L_item_in_rec.append(L_item)
                            
            if len(L_diff_id) !=0:
                #status
                O_status = 5
                for i in range(len(L_diff_id)):
                    L_diff = L_diff_id[i]
                    #Q_fetch_diff_items
                    df_diff_item = pd.read_sql(Q_fetch_diff_items,conn,params=(L_diff,))
                    if len(df_diff_item)>0:
                        for i in range(len(df_diff_item)):
                            L_item = df_diff_item.item[i]
                            L_item_in_rec.append(L_item)
            
            #status
            O_status = 5.5
            if len(L_item_in_rec)>0:
                L_unique_item = np.unique(L_item_in_rec)
                L_unique_item= convert_numpy(L_unique_item)
                if len(L_unique_item) == 1:
                    L_unique_item =  L_unique_item + (-1,)
                #Q_del_ext_item
                #status
                O_status = 6
                mycursor.execute(Q_del_ext_item.format(L_unique_item))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)                        
                conn.commit()
#######################################inventory setup execution############################################
            mycursor.execute(Q_del_item_srch_dtl_tmp,(L_alloc_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            if L_alloc_level == 'T':
                if L_po_source_type_ind == 1:
                    
                    if doc_type == 'F':
                        O_status = 12
                        mycursor.execute(Q_ins_wi_item_loc,(order_no,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                        O_status = 13  
                        mycursor.execute(Q_ins_item_search)
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                        O_status = 14  
                        mycursor.execute(Q_ins_item_srch_wia,(L_alloc_no,L_alloc_type,order_no))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    else:
                        O_status = 19
                        mycursor.execute(Q_ins_item_srch_pos,(L_alloc_no,L_alloc_type,L_start_date,L_start_date,L_end_date,L_end_date,
                                                              L_esid_start_date,L_esid_start_date,L_esid_end_date,L_esid_end_date,
                                                              L_clearance_ind,L_clearance_ind,L_max_avail_qty))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_ins_item_srch_pos") 
                elif L_tsf_source_type_ind == 1:
                     O_status = 23
                     mycursor.execute(Q_ins_item_srch_tsf,(L_alloc_no,L_alloc_type,L_clearance_ind))
                     print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

                elif L_asn_source_type_ind == 1:
                     print(L_asn_source_type_ind,"L_asn_source_type_ind")

                     O_status = 24
                     mycursor.execute(Q_ins_item_srch_asn,(L_alloc_no,L_alloc_type,L_clearance_ind))
                     print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                elif L_wh_source_type_ind ==1 or L_wi_source_type_ind==1:
                    #status
                    O_status = 7
                        #-------------------------
                        #Fetching Warehouse Items
                        #-------------------------
                    if L_alloc_criteria =='W' and L_wh_source_type_ind ==1:
                        #status
                        O_status = 8
                        #Q_ins_itm_srch_dtl 
                        mycursor.execute(Q_ins_itm_srch_dtl_tmp,(L_alloc_no,L_alloc_criteria,L_alloc_type,L_clearance_ind,L_min_avail_qty,L_max_avail_qty))
                        conn.commit()
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    #----------------------
                    #Fetching Whatif Items
                    #----------------------
                    elif L_alloc_criteria =='F' and L_wi_source_type_ind==1:
                        #status
                        O_status = 9 
                        mycursor.execute(Q_ins_itm_srch_dtl_whatif,(L_alloc_no,L_alloc_criteria,L_alloc_type,L_clearance_ind))
                        conn.commit()
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            elif L_alloc_level == 'D':  #style/diff
                if L_wh_source_type_ind ==1 or L_wi_source_type_ind==1:
                    #-------------------------
                    #Fetching Warehouse Items
                    #-------------------------
                    if L_alloc_criteria =='W' and L_wh_source_type_ind ==1:
                        #status
                        O_status = 8
                        #Q_ins_itm_srch_dtl 
                        mycursor.execute(Q_ins_style_itm_srch_dtl_tmp,(L_alloc_no,L_alloc_criteria,L_alloc_type,L_clearance_ind,L_min_avail_qty,L_max_avail_qty))
                        conn.commit()
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    #----------------------
                    #Fetching Whatif Items
                    #----------------------
                    elif L_alloc_criteria =='F' and L_wi_source_type_ind==1:
                        #status
                        O_status = 9 
                        mycursor.execute(Q_ins_style_itm_srch_dtl_wis,(L_alloc_no,L_alloc_criteria,L_alloc_type,L_clearance_ind))
                        conn.commit()
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                elif L_tsf_source_type_ind == 1:
                     print(L_tsf_source_type_ind,"L_tsf_source_type_ind")

                     mycursor.execute(Q_ins_style_item_srch_tsf,(L_alloc_no,L_alloc_type,L_clearance_ind))
                     print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                elif L_asn_source_type_ind == 1:
                     O_status = 24
                     mycursor.execute(Q_ins_style_item_srch_asn,(L_alloc_no,L_alloc_type,L_clearance_ind))
                     print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                elif L_po_source_type_ind == 1:
                    
                    if doc_type == 'F':
                        O_status = 12
                        mycursor.execute(Q_ins_wi_item_loc,(order_no,))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    
                        O_status = 13  
                        mycursor.execute(Q_ins_style_item_srch)
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    
                        O_status = 14  
                        mycursor.execute(Q_ins_style_item_srch_wia,(L_alloc_no,L_alloc_type,order_no))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                    else:
                        O_status = 19
                        mycursor.execute(Q_ins_style_item_srch_pos,(L_alloc_no,L_alloc_type,L_start_date,L_start_date,L_end_date,L_end_date,
                                                                    L_esid_start_date,L_esid_start_date,L_esid_end_date,L_esid_end_date,
                                                                    L_clearance_ind,L_clearance_ind,L_max_avail_qty))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount,"Q_ins_item_srch_pos")

            df_fetch_srch_po_no = pd.read_sql(Q_fetch_srch_po_no,conn)
            if len(df_fetch_srch_po_no) > 0 and L_asn_source_type_ind == 1:
                O_status = 20
                mycursor.execute(Q_del_srch_asn_no)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            elif len(df_fetch_srch_po_no) > 0:
                O_status = 21
                mycursor.execute(Q_del_srch_po_no)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)            
            
            #pack_ind
            if L_alloc_level =='T' and L_pack_ind == 'Y':
                mycursor.execute(Q_del_non_pack_items,(L_alloc_no,))
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 10
            mycursor.execute("UNLOCK TABLES;")
            mycursor.execute(Q_del_item_srch_dtl,(L_alloc_no,))
            conn.commit()
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            #status
            O_status = 11
            mycursor.execute(Q_ins_itm_srch_dtl,(L_alloc_no,))
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            conn.commit()
            if L_alloc_type == 'S':
                result1,err_msg1 = chk_dup_styl_clr (conn, L_alloc_no, L_alloc_level)
                if result1 == False:
                    return emp_list,err_msg1
            df_result = pd.read_sql(Q_fetch_items,conn,params=(L_alloc_no,))

            conn.cursor().close() #RC

            return df_result,""
                      
    except Exception as error:
        err_return = ""
        if O_status<=4:
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_fetch_vpn_items :"+ str(error)
            print(L_func_name,":",O_status,":","Exception occured while processing Q_fetch_vpn_items: ", error)
        elif O_status==5:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_fetch_diff_items: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_fetch_diff_items :"+ str(error)
        elif O_status==6:
            print(L_func_name,":",O_status,":","Exception occured while processing Q_del_ext_item: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing Q_del_ext_item :"+ str(error)
        elif O_status==7:
            print(L_func_name,":",O_status,":","Exception occured while refreshing temp table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while refreshing temp table :"+ str(error)
        elif O_status==8:
            print(L_func_name,":",O_status,":","Exception occured while fetching WH inventory: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while fetching WH inventory :"+ str(error)
        elif O_status==9:
            print(L_func_name,":",O_status,":","Exception occured while fetching WhatIF inventory: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while fetching WhatIF inventory :"+ str(error)
        elif O_status==10:
            print(L_func_name,":",O_status,":","Exception occured while refreshing main table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while refreshing main table :"+ str(error)
        elif O_status==11:
            print(L_func_name,":",O_status,":","Exception occured while inserting data into physical table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting data into physical table :"+ str(error)
        elif O_status==12:
            print(L_func_name,":",O_status,":","Exception occured while inserting wif data into alloc_search_criteria_itm_temp table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting wif data into alloc_search_criteria_itm_temp table :"+ str(error)
        elif O_status==13:
            print(L_func_name,":",O_status,":","Exception occured while inserting wif data into alloc_items_srch_temp: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting wif data into alloc_items_srch_temp :"+ str(error)
        elif O_status==14:
            print(L_func_name,":",O_status,":","Exception occured while inserting wif data into alloc_itm_search_dtl_temp: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting wif data into alloc_itm_search_dtl_temp :"+ str(error)
        elif O_status==15:
            print(L_func_name,":",O_status,":","Exception occured while looping vpn records: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while looping vpn records :"+ str(error)
        elif O_status==16:
            print(L_func_name,":",O_status,":","Exception occured while looping item records: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while looping item records :"+ str(error)
        elif O_status==17:
            print(L_func_name,":",O_status,":","Exception occured while deleting duplicate records: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting duplicate records :"+ str(error)
        elif O_status==18:
            print(L_func_name,":",O_status,":","Exception occured while deleting data from physical table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting data from physical table :"+ str(error)
        elif O_status==19:
            print(L_func_name,":",O_status,":","Exception occured while inserting data into physical table: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting data into physical table :"+ str(error)
        elif O_status==20:
            print(L_func_name,":",O_status,":","Exception occured while inserting data into alloc_srch_po_no table for pos: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting data into alloc_srch_po_no table for pos :"+ str(error)
        elif O_status==21:
            print(L_func_name,":",O_status,":","Exception occured while inserting data into alloc_srch_po_no table for tsfs: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting data into alloc_srch_po_no table for tsfs :"+ str(error)
        elif O_status==22:
            print(L_func_name,":",O_status,":","Exception occured while inserting data into alloc_srch_po_no table for asns: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting data into alloc_srch_po_no table for asns :"+ str(error)
        elif O_status==23:
            print(L_func_name,":",O_status,":","Exception occured while inserting data into alloc_itm_search_dtl_temp table for tsfs: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting data into alloc_itm_search_dtl_temp table for tsfs :"+ str(error)
        elif O_status==24:
            print(L_func_name,":",O_status,":","Exception occured while inserting data into alloc_itm_search_dtl_temp table for asns: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting data into alloc_itm_search_dtl_temp table for asns :"+ str(error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(error)
        conn.rollback()
        conn.cursor().close() #RC
        return emp_list,err_return

#--------------------------------------------------------------
# Function to get all items based on input
#--------------------------------------------------------------

def get_item_locs_from_txns_whs(conn,
                                I_search_criteria,
                                I_expand_to_pack,
                                O_status):
    L_func_name ="get_item_locs_from_txns_whs"
    O_status = 0
    print("EXECUTING: ",L_func_name)
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/populate_search_result_queries.yaml') as fh:
            queries              = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_pos                = queries['get_item_locs_from_txns_whs']['Q_pos']
            Q_tsfs               = queries['get_item_locs_from_txns_whs']['Q_tsfs']
            Q_asns               = queries['get_item_locs_from_txns_whs']['Q_asns']
            Q_ins_item_loc       = queries['get_item_locs_from_txns_whs']['Q_ins_item_loc']
            Q_sel_item_loc       = queries['get_item_locs_from_txns_whs']['Q_sel_item_loc']
            Q_del_item_loc_data1 = queries['get_item_locs_from_txns_whs']['Q_del_item_loc_data1']
            Q_del_item_loc_data  = queries['get_item_locs_from_txns_whs']['Q_del_item_loc_data']
            Q_ins_item_loc_data  = queries['get_item_locs_from_txns_whs']['Q_ins_item_loc_data']

            Q_create_item_loc_srch_table1  = queries['get_item_locs_from_txns_whs']['Q_create_item_loc_srch_table1']
            Q_chk_item_loc_srch_table1     = queries['get_item_locs_from_txns_whs']['Q_chk_item_loc_srch_table1']

            Q_create_srch_temp_1       = queries['get_item_locs_from_txns_whs']['Q_create_srch_temp_1']
            Q_create_srch_temp_2       = queries['get_item_locs_from_txns_whs']['Q_create_srch_temp_2']
            Q_merge_pack_criteria_temp = queries['get_item_locs_from_txns_whs']['Q_merge_pack_criteria_temp']
            Q_drop_criteria_temp_1     = queries['get_item_locs_from_txns_whs']['Q_chk_item_loc_srch_table1']
            Q_drop_criteria_temp_2     = queries['get_item_locs_from_txns_whs']['Q_chk_item_loc_srch_table1']

            mycursor = conn.cursor()

            L_pos           = I_search_criteria["PO"]
            L_asns          = I_search_criteria["ASN"]
            L_tsfs          = I_search_criteria["TSF"] 

            if len(L_pos) == 1:
               L_pos.append(-1)

            if len(L_asns) == 1:
               L_asns.append(-1)

            if len(L_tsfs) == 1:
               L_tsfs.append(-1)

            L_pos           = convert_numpy(L_pos)
            L_asns          = convert_numpy(L_asns)
            L_tsfs          = convert_numpy(L_tsfs)


            df_chk = pd.read_sql(Q_chk_item_loc_srch_table1,conn)
            L_chk = df_chk.chk[0]

            if L_chk == 1:
                print("Please drop table alloc_search_criteria_itm_temp1.")
                print(O_status,L_func_name)
                conn.cursor().close()
                return False,L_func_name+":"+str(O_status)+": "+"Please drop table alloc_search_criteria_itm_temp1."
            else:
                mycursor.execute(Q_create_item_loc_srch_table1)
            
            O_status = 1
            if len(L_pos) != 0:
                df_pos = pd.read_sql(Q_pos.format(L_pos),conn)
                if len(df_pos) > 0:
                    for index, row in df_pos.iterrows():
                        L_item          = f"{row['item']}"
                        L_loc           = f"{row['location']}"
                        mycursor.execute(Q_ins_item_loc,(L_item,L_loc))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            if len(L_tsfs) != 0:
                df_tsfs = pd.read_sql(Q_tsfs.format(L_tsfs),conn)
                if len(df_tsfs) > 0:
                    for index, row in df_tsfs.iterrows():
                        L_item          = f"{row['item']}"
                        L_loc           = f"{row['location']}"
                        mycursor.execute(Q_ins_item_loc,(L_item,L_loc))
                        print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            if len(L_asns) != 0:
                df_asns = pd.read_sql(Q_asns.format(L_asns),conn)
                if len(df_asns) > 0:
                    for index, row in df_asns.iterrows():
                        L_item          = f"{row['item']}"
                        L_loc           = f"{row['location']}"
                        mycursor.execute(Q_ins_item_loc,(L_item,L_loc))


            print(pd.read_sql("select * from alloc_search_criteria_itm_temp1;",conn))

            O_status = 2
            result1,err_msg1 = get_item_locs_from_items_whs(conn,I_search_criteria,O_status)
            if result1 == False:
                #status
                O_status =5
                print(O_status,"--get_item_locs_from_items_whs: ",err_msg1)
                return False,err_msg1

            df_sel_item_loc = pd.read_sql(Q_sel_item_loc,conn)
            if len(df_sel_item_loc) > 0:

                O_status = 3
                mycursor.execute(Q_del_item_loc_data1)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
    
                O_status = 4
                mycursor.execute(Q_del_item_loc_data)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
            
            O_status = 5
            mycursor.execute(Q_ins_item_loc_data)
            print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)

            if I_expand_to_pack == True: #code incorporated for packs start

                mycursor.execute(Q_create_srch_temp_1)
                mycursor.execute(Q_create_srch_temp_2)

                mycursor.execute(Q_merge_pack_criteria_temp)
                print(L_func_name,"-",O_status,"-","rows_affected: ",mycursor.rowcount)
                
                mycursor.execute(Q_drop_criteria_temp_1)
                mycursor.execute(Q_drop_criteria_temp_2) #code incorporated for packs end

            conn.cursor().close()
            return True, ""

    except Exception as error:
        err_return = ""
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while looping L_pos: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while looping L_pos :"+ str(error)
        elif O_status==2:
            print(L_func_name,":",O_status,":","Exception occured while processing get_item_locs_from_items_whs: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while processing get_item_locs_from_items_whs :"+ str(error)
        elif O_status==3:
            print(L_func_name,":",O_status,":","Exception occured while deleting item_loc data1: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting item_loc data1 :"+ str(error)
        elif O_status==4:
            print(L_func_name,":",O_status,":","Exception occured while deleting item_loc data: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting item_loc data :"+ str(error)
        elif O_status==5:
            print(L_func_name,":",O_status,":","Exception occured while inserting item_loc data: ", error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while inserting item_loc data :"+ str(error)
        else:
            print(L_func_name,":",O_status,"Exception occured in: ",L_func_name,error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured:"+ str(error)
        conn.rollback()
        conn.cursor().close()
        return False, err_return


    #----------------------------------------
    #function add_packs_covered_by_style
    #----------------------------------------

def pack_coverd_by_style(conn,O_status):
    L_func_name = "pack_coverd_by_style"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/populate_search_result_queries.yaml') as fh:
            queries      = yaml.load(fh, Loader=yaml.SafeLoader)
            Q_create_srch_temp_1    = queries['add_pack_coverd_by_style']['Q_create_srch_temp_1']
            Q_create_srch_temp_2    = queries['add_pack_coverd_by_style']['Q_create_srch_temp_2']
            Q_merge_gtt             = queries['add_pack_coverd_by_style']['Q_merge_gtt']
            Q_drop_criteria_temp_1  = queries['add_pack_coverd_by_style']['Q_drop_criteria_temp_1']
            Q_drop_criteria_temp_2  = queries['add_pack_coverd_by_style']['Q_drop_criteria_temp_2']

            mycursor = conn.cursor()
            mycursor.execute(Q_create_srch_temp_1)
            mycursor.execute(Q_create_srch_temp_2)

            O_status = 1
            mycursor.execute(Q_merge_gtt)
            print("excuted successfully")
            mycursor.execute(Q_drop_criteria_temp_1)
            mycursor.execute(Q_drop_criteria_temp_2)
            conn.cursor().close()
            return True , ""

    except Exception as error:
        err_return = ""
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while executing Q_merge_gtt query  : ", error) 
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while executing Q_merge_gtt query :"+ str(error)
        else: 
            print("Exception occured in: ",L_func_name.format(error),error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured :"+ str(error)
        conn.rollback()
        return False, err_return

#-------------------------------------------
#function REMOVE_MULTI_PARENT_PACKS
#------------------------------------------

def remove_multi_parant_packs(conn,O_status):
    L_func_name = "remove_multi_parant_packs"
    try:
        with open(r'./stock_ledger_models/Allocation_functions/Allocation/GLOBAL_FILES/populate_search_result_queries.yaml') as fh:
            queries      = yaml.load(fh, Loader=yaml.SafeLoader)
        Q_create_srch_temp_1   = queries['remove_multi_parent_packs']['Q_create_srch_temp_1']
        Q_delete_gtt           = queries['remove_multi_parent_packs']['Q_delete_gtt']
        Q_drop_criteria_temp_1 = queries['remove_multi_parent_packs']['Q_drop_criteria_temp_1']

        mycursor = conn.cursor()
        mycursor.execute(Q_create_srch_temp_1)

        O_status = 1
        mycursor.execute(Q_delete_gtt)
        mycursor.execute(Q_drop_criteria_temp_1)
        conn.cursor().close()
        return True,""

    except Exception as error:
        err_return = ""
        if O_status==1:
            print(L_func_name,":",O_status,":","Exception occured while deleting from Q_delete_gtt query  : ", error) 
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured while deleting from Q_delete_gtt query :"+ str(error)
        else: 
            print("Exception occured in: ",L_func_name.format(error),error)
            err_return = L_func_name+":"+str(O_status)+": "+"Exception occured :"+ str(error)
        conn.rollback()
        return False,err_return
