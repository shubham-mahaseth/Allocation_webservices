import json
import csv
from pickle import FALSE
from statistics import mode                        
import pandas as pd
from django.db import IntegrityError
from django.http import JsonResponse,HttpResponse,StreamingHttpResponse
from django.core import serializers
from datetime import datetime,date
from django.views.decorators.csrf import csrf_exempt
from django.utils.crypto import get_random_string
from django.shortcuts import render
from django.db.models import Q
import time
import decimal
from decimal import Decimal
from decimal import *
from django.core.serializers.python import Serializer
from pandas.tseries.offsets import QuarterBegin
import numpy as np
from django.db import connection

from .connect import get_mysql_conn
from .views_alloc_criteria import retrieve_LIM
from .views_alloc_criteria import df_conversion #, establish_connection
from .views_Qlimits import Alloc_qty_limits_retrive_table_function
from .Allocation_functions.Allocation.INVENTORY_SETUP.wrapper_load_item import wrapper_load_source
from .Allocation_functions.Allocation.INVENTORY_SETUP.wrapper_upd_alloc_ext import update_alloc_wrapper
from .Allocation_functions.Allocation.CREATE_SCREEN.create_screen_add_button_wrapper import populate_item
from .Allocation_functions.Allocation.CREATE_SCREEN.refresh_grid_wrapper import refresh_all
from .Allocation_functions.Allocation.CREATE_SCREEN.error_report_wrapper import pop_error

from .Allocation_functions.Allocation.CALCULATION.calculation_wrapper import do_calculation
from .Allocation_functions.Allocation.CALCULATION.whatif_calculation_wrapper import do_whatif_calc

from .Allocation_functions.Allocation.ALLOCATION_STATUS.approve_wrapper import approve_wrapper
from .Allocation_functions.Allocation.ALLOCATION_STATUS.reserve_wrapper import reserve_wrapper
from .Allocation_functions.Allocation.ALLOCATION_STATUS.wrapper_worksheet import worksheet_wrapper

from .Allocation_functions.Allocation.ALLOCATION_SUMMARY.switch_window_wrapper import switch_window
from .Allocation_functions.Allocation.ALLOCATION_SUMMARY.copy_allocation_wrapper import to_call_ca_fun
from .Allocation_functions.Allocation.ALLOCATION_SUMMARY.searchB import to_call_fun
from .Allocation_functions.Allocation.ALLOCATION_SUMMARY.sel_approve_wrapper import sel_approve_wrapper

from .Allocation_functions.Allocation.SCHEDULE.update_alloc_parms_wrapper import update_alloc_parms_wrapper
from .Allocation_functions.Allocation.INVENTORY_SETUP.update_latest_inv_wrapper import update_inv_wrapper
from .Allocation_functions.Allocation.SCHEDULE.retreive_schedule_data_wrapper import retreive_schedule_data
from .Allocation_functions.Allocation.CREATE_SCREEN.split_button_wrapper import split_func


conn_global =None
conn_dict = {}
'''
        ********************************
        ********************************
             SAVE DATA & GLOBAL CONN
        ********************************
        ********************************
    
'''

def establish_connection(key=0):
    global conn_global
    #if conn_global is None:
    #    conn_generator =get_mysql_conn([])
    #    conn_global =  conn_generator.__enter__()
    I_db_connect_status = list()
    I_db_connect_status.append(0)
    conn_generator   = get_mysql_conn(I_db_connect_status)
    conn_global      = conn_generator.__enter__()
    #if key != 0: 
    #    conn_dict[key] = conn_global
    #    print("\n\n CONNECTION DICTIONARY::: ",conn_dict)
def close_connection():
    global conn_global
    if conn_global is not None:
        conn_global.__exit__(None, None, None)
        conn_global = None

@csrf_exempt
def Alloc_Commit_Data_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            req_type = data[0] if len(data) >0 else []
            U_query = '''UPDATE alloc_head
                            SET ALLOC_DESC = %s
                            WHERE ALLOC_NO = %s;'''
            mycursor = connection.cursor()
            if "HEADER" in req_type and len(req_type["HEADER"]) > 0:
                Alloc_Data = req_type["HEADER"]
                L_alloc_no = Alloc_Data["ALLOC_NO"]
                Alloc_head_Data = pd.read_sql("select * from alloc_head where ALLOC_NO = '{}'".format(L_alloc_no)+";",connection)
                Alloc_head_Data = (Alloc_head_Data.to_dict("records"))[0]
                alloc_desc = ''
                if Alloc_Data["ALLOC_DESC"] !=  Alloc_head_Data["ALLOC_DESC"]:
                    alloc_desc = Alloc_Data["ALLOC_DESC"]
                if len(alloc_desc) > 0 :
                    print("UPDATED DESC...")
                    mycursor.execute(U_query,(alloc_desc,L_alloc_no))
                connection.commit()
            # if "DEL_ITEMS" in req_type and len(req_type["DEL_ITEMS"])>0:
            #     for row in req_type["DEL_ITEMS"]:
            #         query="update alloc_itm_search_dtl set SEL_IND = 'N' where SEL_IND= 'Y' and "
            #         for col in row:
            #             if col != 'SR_NO':
            #                 query=query+ col +" = '"+ str(row[col]) +"' and "
            #         query = query[:-4]+";"
            #         mycursor.execute(query)
            #     connection.commit()
            if "SELECTED_TOTALDATA" in req_type:
                result,error_msg = update_ind(req_type["SELECTED_TOTALDATA"])
                if result:
                    query="delete from alloc_itm_search_dtl where SEL_IND = 'N' and ALLOC_NO = '{}'".format(L_alloc_no)+";"
                    mycursor.execute(query)
                    connection.commit()
                else:
                    return JsonResponse({"status": 500, "message": str(error_msg)})    
            # print("ALLOC_COMMIT_DATA_TABLE** ",data,conn_global )
            if conn_global != None and "ASM" not in data:
                # print("asm")
                conn_global.commit()
                conn_global.close()
                close_connection()
            else:
                connection.commit()
            return JsonResponse({"status": 200, "message":"COMMIT SUCCESS"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        finally:
            connection.close()


'''
        ********************************
        ********************************
            CREATE ALLOCATION START
        ********************************
        ********************************
    
'''



#Fetching all the column values from HIER table:
@csrf_exempt
def Alloc_result_PO_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object=json_object[0]                        
            res_list=[]
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message": "NO DATA FOUND"})
            else:
                return JsonResponse(res_list, content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        finally:
                connection.close()

#Fetching all the column values from HIER table:
@csrf_exempt
def Alloc_result_WH_table(request):
    if request.method == 'POST':
        try:
            json_data = json.loads(request.body)
            #print("json_data: ",json_data)
            mycursor = connection.cursor()
            json_data = json_data[0]
            json_object = json_data["CREATE_DATA"]
            #json_data = json_data[1]
            #json_object=data[0]
            #data.pop(0)  
            I_query       = "INSERT INTO active_alloc_sessions(ALLOC_NO,SESSION_MODE,CREATE_ID,CREATE_TIME) VALUES(%s,'CREATE',%s,current_timestamp); "
            S_query       = "SELECT * FROM active_alloc_sessions WHERE ALLOC_NO = %s; "

            if "CHANGE_CREATE_AVAIL_GRID" in json_data:
                if len(json_data["CHANGE_CREATE_AVAIL_GRID"])>0:
                    update_CreateGrid_Avail_table(json_data["CHANGE_CREATE_AVAIL_GRID"])

            result=pd.read_sql("select * from alloc_itm_search_dtl where ALLOC_NO = '{}'".format(json_object["ALLOC_NO"]),connection)
               
            if len(result)>0:
                if len(json_data["UPDATE"])>0:
                    result2,err_msg = update_ind(json_data["UPDATE"])
                    #if result2 == False and len(err_msg) > 0:
                    #    return JsonResponse({"status": 500, "message":str(err_msg)})
                else:
                    query1="update alloc_itm_search_dtl set SEL_IND ='N' where ALLOC_NO = '{}'".format(json_object["ALLOC_NO"])+";"
                    mycursor.execute(query1)
                if len(json_data["CHANGE_CREATE_GRID"])> 0 :
                    result3 = update_CreateGrid_table(json_data["CHANGE_CREATE_GRID"])
            
            json_object["CREATE_DATETIME"] = str(datetime.now())
            for col in json_object:
                if col == "ALLOC_CRITERIA":
                    if json_object[col]=="Warehouse":
                        json_object[col]="W"
                    if json_object[col]=="Purchase Order":
                        json_object[col]="P"
                    if json_object[col]=="Pre Buy":
                        json_object[col]="F"
                    if json_object[col]=="Transfer":
                        json_object[col]="T"
                    if json_object[col]=="ASN":
                        json_object[col]="A"

            for key in json_object:
                if json_object[key] == "" or json_object[key] == "NULL":
                    json_object[key] = None
            connection.commit()
            
            result1,err_msg1   = update_alloc_wrapper(connection,json_object["ALLOC_NO"],json_object["ALLOC_LEVEL"],json_object["RELEASE_DATE"],json_object["RECALC_IND"],json_object)
            if result1 == False:
                return JsonResponse({"status": 500, "message":str(err_msg1)})

            results55,err_msg2 = populate_item(connection,json_object)
            if len(results55) > 0 and len(err_msg2) > 0:
                return JsonResponse({"status": 500, "message":str(err_msg2)})

            res_list=[]
            rec={}
            if len(results55) > 0:
                results55 =  results55.replace(np.NaN, "NULL", regex=True)
                for val2 in results55.values:
                    count=0
                    for col4 in results55.columns:
                        rec[col4] = val2[count]
                        count = count + 1
                    for col in rec:
                        if rec[col] == None or rec[col] == "NULL" or rec[col] == "NaN":
                            rec[col]=""
                        if col == "ALLOC_CRITERIA":
                            if rec[col]  == "W":
                                rec[col]  = "Warehouse"
                            if rec[col]  == "P":
                                rec[col]  = "Purchase Order"
                            if rec[col]  == "F":
                                rec[col]  = "Pre Buy"
                            if rec[col]  == "T":
                                rec[col]  = "Transfer"
                            if rec[col]  == "A":
                                rec[col]  = "ASN"
                    res_list.append(rec.copy())                 
                if len(res_list)==0:
                    return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
                else:
                    mycursor.execute(S_query,(json_object["ALLOC_NO"],))
                    # print( "LOCKING IN ADD FUNCTIONALITY :: ",mycursor.rowcount)
                    if mycursor.rowcount == 0:
                        mycursor.execute(I_query,(json_object["ALLOC_NO"],json_object["CREATE_ID"],))
                    return JsonResponse(res_list,content_type="application/json",safe=False)
            else:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        finally:
                connection.close()



def update_CreateGrid_Avail_table(data):
    O_status = 0
    L_fun ="UPDATE CREATE AVAIL GRID"
    try:        
        mycursor = connection.cursor()
        D_keys=[]
        #status
        O_status = 1 
        ALLOC_NO =""

        D_keys=[]
        for row in data:
            for col in row:
                ALLOC_NO = row["ALLOC_NO"]    
                if (row[col]==None or row[col]=="NULL" or row[col]=="NaN" or row[col]=="") and  col != 'AVAIL_QTY':
                    D_keys.append(col)
            for key in D_keys:
                row.pop(key)
            D_keys.clear()

        list1=[]
        for row in data:
            for key in row:
                if key not in ['ALLOC_NO', 'ALLOC_CRITERIA', 'ITEM', 'DIFF_ID', 'HIER1', 'HIER2', 'HIER3', 'VPN','LOC','REF_1','REF_2','AVAIL_QTY']:
                    list1.append(key)
            for k in list1:
                row.pop(k)
            list1.clear()

        for json_object in data:
            for col in json_object:
                if col=="ALLOC_CRITERIA":
                    if json_object[col]=="Warehouse":
                        json_object[col]="W"
                    if json_object[col]=="Purchase Order":
                        json_object[col]="P"
                    if json_object[col]=="Pre Buy":
                        json_object[col]="F"
                    if json_object[col]=="Transfer":
                        json_object[col]="T"
                    if json_object[col]=="ASN":
                        json_object[col]="A"

        O_status = 2   
        for row in data:
            if "AVAIL_QTY" in row:
                avail_qty = row["AVAIL_QTY"]
                if avail_qty in ["NULL", "NaN", None, ""]:
                    avail_qty = "null"
                else:
                    avail_qty = "'{}'".format(avail_qty)
                query="update alloc_itm_search_dtl set AVAIL_QTY = {}".format(avail_qty)+" where "
                for col in row:
                    if col != "AVAIL_QTY":
                        
                        query=query+ col +" = '"+ str(row[col]) +"' and "
                query = query[:-4]+";"
                mycursor.execute(query)
        connection.commit()
        return True
    except Exception as error:
        if O_status==1: 
            print(L_fun,":",O_status,":","Exception raised during updating sel_ind to 'N' :", error)
            return True
        elif O_status==2:
            print(L_fun,":",O_status,":","Exception raised during updating sel_ind to 'Y':", error)
            return True

def update_CreateGrid_table(data):
    O_status = 0
    L_fun ="UPDATE CREATE GRID"
    try:        
        mycursor = connection.cursor()
        D_keys=[]
        O_status = 1 
        #print("UPDATE CREATE GRID ::  ",data)
        # D_keys=[]
        # for row in data:
        #     for col in row:
        #         if row[col]==None or row[col]=="NULL" or row[col]=="NaN" or row[col]=="":
        #             D_keys.append(col)
        #     for key in D_keys:
        #         row.pop(key)
        #     D_keys.clear()

        list1=[]
        for row in data:
            for key in row:
                if key not in ['ALLOC_NO', 'ALLOC_CRITERIA', 'ITEM', 'DIFF_ID', 'HIER1', 'HIER2', 'HIER3', 'VPN','LOC','REF_1','REF_2','HOLDBACK_TYPE', 'HOLDBACK_QTY', 'SOM_TYPE','AVAIL_QTY','INNER_SIZE','CASE_SIZE']:
                    if row[key]==None or row[key]=="NULL" or row[key]=="NaN" or row[key]=="" or key=='SR_NO'  or key=='ITEM_DESC':
                        list1.append(key)
            for k in list1:
                row.pop(k)
            list1.clear()
        for json_object in data:
            for col in json_object:
                if col=="ALLOC_CRITERIA":
                    if json_object[col]=="Warehouse":
                        json_object[col]="W"
                    if json_object[col]=="Purchase Order":
                        json_object[col]="P"
                    if json_object[col]=="Pre Buy":
                        json_object[col]="F"
                    if json_object[col]=="Transfer":
                        json_object[col]="T"
                    if json_object[col]=="ASN":
                        json_object[col]="A"
                        
        O_status = 2                   
        for row in data:
            if 'HOLDBACK_TYPE' in row or 'HOLDBACK_QTY' in row or 'SOM_TYPE' in row:
                query = "update alloc_itm_search_dtl set "
                if "HOLDBACK_TYPE" in row and row["HOLDBACK_TYPE"] != '':
                    query = query + " HOLDBACK_TYPE = '{}'".format(str(row["HOLDBACK_TYPE"]))+" ,"
                else:
                    query = query + " HOLDBACK_TYPE = NULL ,"
                if "HOLDBACK_QTY" in row and row["HOLDBACK_QTY"] != '':
                    query = query + " HOLDBACK_QTY = '{}'".format(row["HOLDBACK_QTY"])+" ,"
                else:
                    query = query + " HOLDBACK_QTY = NULL ,"
                if "SOM_TYPE" in row:
                    query = query + " SOM_TYPE = '{}'".format(str(row["SOM_TYPE"]))+" ,"
                    if row["SOM_TYPE"] == 'I':
                        if row["INNER_SIZE"] == "":
                            query = query + " SOM_QTY = '{}'".format(1)+" ,"
                        else:
                            query = query + " SOM_QTY = '{}'".format(str(row["INNER_SIZE"]))+" ,"
                    if row["SOM_TYPE"] == 'C':
                        if row["CASE_SIZE"] == "":
                            query = query + " SOM_QTY = '{}'".format(1)+" ,"
                        else:
                            query = query + " SOM_QTY = '{}'".format(str(row["CASE_SIZE"]))+" ,"
                    if row["SOM_TYPE"] == 'E': 
                         query = query + " SOM_QTY = '{}'".format(1)+" ,"
                    
                query = query[:-1] + "where "
                list1=[]
                for key in row:                    
                    if key in ['HOLDBACK_TYPE', 'HOLDBACK_QTY', 'SOM_TYPE','AVAIL_QTY']:
                        list1.append(key)
                    elif row[key]==None or row[key]=="NULL" or row[key]=="NaN" or row[key]=="" or key=='SR_NO':
                        list1.append(key)
                for k in list1:
                    row.pop(k)
                list1.clear()
                for col in row:
                    query=query+ col +" = '"+ str(row[col]) +"' and "
                query = query[:-4]+";"             
                mycursor.execute(query)
                connection.commit()
        return True
    except Exception as error:
        if O_status==1: 
            print(L_fun,":",O_status,":","Exception raised during updating UPDATE CREATE GRID to 'N' :", error)
            return True
        elif O_status==2:
            print(L_fun,":",O_status,":","Exception raised during updating UPDATE CREATE GRID:", error)
            return True



#Fetching all the column values from HIER table:
@csrf_exempt
def Alloc_result_ASN_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            #print("Alloc_result_PO_table",json_object)
            json_object=json_object[0]                        
            res_list=[]
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message": "NO DATA FOUND"})
            else:
                return JsonResponse(res_list, content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        finally:
                connection.close()




#Fetching all the column values from HIER table:
@csrf_exempt
def Alloc_result_TSF_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            #print("Alloc_result_PO_table",json_object)
            json_object=json_object[0]                        
            res_list=[]
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message": "NO DATA FOUND"})
            else:
                return JsonResponse(res_list, content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        finally:
                connection.close()



#FETCHING ALL THE COLUMN VALUES FROM HIER TABLE:
@csrf_exempt
def Alloc_update_SelInd_Create_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            mycursor = connection.cursor()
            D_keys=[]
            # print("col:",json_object)
            for row in json_object:
                query1="update alloc_itm_search_dtl set SEL_IND ='N' where ALLOC_NO = '{}'".format(row["ALLOC_NO"])+";"
                mycursor.execute(query1)

            for row in json_object:
                query="update alloc_itm_search_dtl set "
                for key in row:    
                    if row[key]=="" or row[key]=="NULL":
                        D_keys.append(key) 
                for key in D_keys:
                    row.pop(key)
                D_keys.clear()
                for key in row:
                    if key=="SEL_IND":
                        query=query+key+" = 'Y'"
                query=query+ "where ALLOC_NO = '{}'".format(row["ALLOC_NO"]) +" and ITEM = '{}'".format(row["ITEM"])+";"
                # print(query)
                mycursor.execute(query)
            connection.commit()
            return JsonResponse({"status": 201, "message": "Data Updated"})
        except IntegrityError:
            return JsonResponse({"status": 500, "message": "TRAN_SEQ_NO must be unique"})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        finally:
            mycursor.close()
            connection.close()


@csrf_exempt
def Alloc_delete_Create_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            # print("json_object:: ",json_object)
            mycursor = connection.cursor()
            if len(json_object) > 0:
                check, err_msg = update_ind(json_object)
                if check == False and len(err_msg) > 0:
                    return JsonResponse({"status": 500, "message":str(err_msg)})
                json_object = json_object[0]
                query="delete from alloc_itm_search_dtl where sel_ind = 'Y' and ALLOC_NO = {}".format(json_object["ALLOC_NO"]) +";"
                mycursor.execute(query)
                connection.commit()
                return JsonResponse({"status": 201, "message": "Data Deleted"})
            else:
                return JsonResponse({"status": 500, "message": "Please select the records"})
        except IntegrityError:
            return JsonResponse({"status": 500, "message": "TRAN_SEQ_NO must be unique"})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        finally:
            connection.close()



#FETCHING ALL THE COLUMN VALUES FROM HIER TABLE:
@csrf_exempt
def Alloc_Refresh_grid_Create_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)            
            if "ALLOC_NO" in json_object: 
                result,err_msg = refresh_all(connection,json_object["ALLOC_NO"])
                if result:
                    return JsonResponse({"status": 201, "message": "FUNCTION RUNS SUCCESSFULLY"})
                else:
                    return JsonResponse({"status": 500, "message": str(err_msg)})
                    #return JsonResponse({"status": 500, "message": "FUNCTION IS FAILED"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        finally:
                connection.close()


#FETCHING ALL THE COLUMN VALUES FROM HIER TABLE:
@csrf_exempt
def err_report(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data = data[0]
            if "ALLOC_NO" in data:
                I_alloc = data["ALLOC_NO"]
                I_err1 = ''
                I_err2 = ''
                I_err3 = ''
                I_err4 = ''
                I_err5 = ''
                I_to_date = None
                I_from_date = None
                #print(234567)
                query="select max(err_ind) as err_ind from alloc_itm_search_dtl where alloc_no=%s;"
                valid=pd.read_sql(query,connection,params=(I_alloc,))
                
                if valid["err_ind"][0] != None:
                    result,err_msg = pop_error(connection,I_alloc,I_err1,I_err2,I_err3,I_err4,I_err5,I_to_date,I_from_date)
                    #print("webservice error report :",result)
                    if len(result) > 0:
                        #result = result.to_dict("records") 
                        result = df_conversion(result)
                        return JsonResponse({"status": 200, "message": result})
                    else:
                        return JsonResponse({"status": 500, "message": str(err_msg)})
                else: 
                    return JsonResponse({"status": 500, "message": "ALLOCATION IS NOT CALCULATED"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": "ERROR REPORT Exception occured : "+str(error)})
       

'''
        ********************************
        ********************************
            CREATE ALLOCATION END
        ********************************
        ********************************
    
'''


'''
        ********************************
        ********************************
             RULES & LOCATION START
        ********************************
        ********************************
    
'''

#Fetching all the column values from PO table:
@csrf_exempt
def Alloc_avail_qty_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object = json_object[0]
            keys=[]
            for key in json_object:
                if json_object[key] == "NULL" or json_object[key] == "":
                    json_object[key] = None
                    keys.append(key)
            for k in keys:
                json_object.pop(k)
            query="""SELECT distinct gtt.item   ITEM,
                                     gtt.diff1  DIFF1,
                                     gtt.diff2  DIFF2,
                                     gtt.diff3  DIFF3,
                                     gtt.diff4  DIFF4,
                                    #gtt.location location,
                        SUM(GREATEST ( (  GREATEST ((CASE WHEN ils.status in ('A','C') THEN COALESCE(ils.item_soh,0) ELSE 0 END), 0))
                                     - (  GREATEST ((CASE WHEN ils.status in ('A','C') THEN COALESCE(ils.reserved_qty,0) ELSE 0 END), 0)
                                        + GREATEST ((CASE WHEN ils.status in ('A','C') THEN COALESCE(ils.rtv_qty,0) ELSE 0 END), 0)
                                        + GREATEST ((CASE WHEN ils.status in ('A','C') THEN COALESCE(ils.non_sellable_qty,0) ELSE 0 END), 0)
                                        + GREATEST ((CASE WHEN ils.status in ('A','C') THEN COALESCE(ils.cust_resv_qty,0) ELSE 0 END), 0))
                                     - (SELECT IFNULL (SUM(GREATEST (COALESCE(d.distro_qty,0),0)),0)
                                          FROM alloc_head h,
										       po_item_loc ol,
											   po_dtl oh,
                                               alloc_dtl d,
                                               alloc_itm_search_dtl ad,
                                               item_location ilc1
                                         WHERE ad.item = gtt.item
                                           AND d.to_loc = ils.location
                                           AND ilc1.item = gtt.item
                                           AND ilc1.location = ils.location
                                           AND ilc1.status in ('A','C')
                                           AND h.status IN ('A', 'R')
										   AND h.po_no IS NOT NULL
										   AND ol.po_no = h.po_no
										   AND ol.item = ad.item
										   AND ol.location = d.to_loc
										   AND ol.received_qty > 0
										   AND oh.po_no = ol.po_no
										   AND oh.status IN ('A', 'C')
                                           AND ad.alloc_no = h.alloc_no
                                           AND d.alloc_no = h.alloc_no
                                           AND d.alloc_qty > 0
                                           AND d.distro_qty > 0),0))  AVAILABLE_QTY,
                        SUM(GREATEST ( (     GREATEST ((CASE WHEN ils.status in ('I') THEN COALESCE(ils.item_soh,0) ELSE 0 END), 0))
                                        - (  GREATEST ((CASE WHEN ils.status in ('I') THEN COALESCE(ils.reserved_qty,0) ELSE 0 END), 0)
                                           + GREATEST ((CASE WHEN ils.status in ('I') THEN COALESCE(ils.rtv_qty,0) ELSE 0 END), 0)
                                           + GREATEST ((CASE WHEN ils.status in ('I') THEN COALESCE(ils.non_sellable_qty,0) ELSE 0 END), 0)
                                           + GREATEST ((CASE WHEN ils.status in ('I') THEN COALESCE(ils.cust_resv_qty,0) ELSE 0 END), 0))
                                        - (SELECT IFNULL (SUM(GREATEST (COALESCE(d.distro_qty,0),0)),0)
                                             FROM alloc_head h,
                                                  po_item_loc      ol,
                                                  po_dtl     oh,
                                                  alloc_dtl d,
				                        		  alloc_itm_search_dtl ad,
				                        		  item_location ilc1
                                            WHERE ad.item = gtt.item
                                              AND d.to_loc = gtt.location
                                              AND ilc1.item = gtt.item
                                              AND ilc1.location = ils.location
                                              AND ilc1.status in ('I')
                                              AND h.status IN ('A', 'R')
                                              AND h.po_no IS NOT NULL
                                              AND ol.po_no = h.po_no
                                              AND ol.item = ad.item
                                              AND ol.location = d.to_loc
                                              AND ol.received_qty > 0
                                              AND oh.po_no = ol.po_no
                                              AND oh.status IN ('A', 'C')
				                        	  AND ad.alloc_no = h.alloc_no
                                              AND d.alloc_no = h.alloc_no
                                              AND d.alloc_qty > 0
                                              AND d.distro_qty > 0), 0))  INACTIVE_QTY	                   
                                        
                                FROM (SELECT id.item,
                                             id.diff1,
                                             id.diff2,
                                             id.diff3,
                                             id.diff4,
                                             il.location
                                       FROM item_dtl id, item_location il
                                      WHERE id.item_parent ='{}' """.format(json_object["ITEM"])+"""
                                        AND id.status ='A'
                                        AND id.item = il.item
                                        AND (id.diff1 = '{}' """.format(json_object["DIFF_ID"])+"""
                                          OR id.diff2 = '{}' """.format(json_object["DIFF_ID"])+"""
                                          OR id.diff3 = '{}' """.format(json_object["DIFF_ID"])+"""
                                          OR id.diff4 = '{}' """.format(json_object["DIFF_ID"])+"""
                                          )) gtt,
                                            item_location          ils
                               WHERE ils.item = gtt.item
                                 and ils.location =  '{}' """.format(json_object["WAREHOUSE"])+"""
                                 AND ils.location_type = 'W'
                                 AND ils.status IN ('A', 'C', 'I')
                               GROUP BY gtt.item,
                                        gtt.diff1,
                                        gtt.diff2,
                                        gtt.diff3,
                                        gtt.diff4,
                                        gtt.location"""
            
            results55 = pd.read_sql(query,connection)
            # print("ALLOC_AVAIL_QTY_TABLE: ",results55)
            res_list=[]
            rec={}
            results55 =  results55.replace(np.NaN, "NULL", regex=True)
            for val2 in results55.values:
                count=0
                for col4 in results55.columns:
                    rec[col4] = val2[count]
                    count = count+1
                for col in rec:
                    if rec[col] == "NULL" or rec[col] == "" or  rec[col] == None:
                        rec[col] = ""
                res_list.append(rec.copy()) 
            return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            print(error)
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})



@csrf_exempt
def Alloc_avail_search_table_grid(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object = json_object[0]
            keys=[]
            for key in json_object:
                if json_object[key] == "NULL" or json_object[key] == "":
                    json_object[key] = None
                    keys.append(key)
            for k in keys:
                json_object.pop(k)
            #print(json_object["ITEM"])
            query="""select distinct itd.ITEM, 
                                    itd.ITEM_DESC,
                                    aisd.HIER1, 
                                    h1.HIER1_DESC,
                                    aisd.HIER2,
                                    h2.HIER2_DESC,
                                    aisd.HIER3,
                                    h3.HIER3_DESC,
                                    aisd.DIFF_ID,
                                    df.DIFF_DESC,
                                    aisd.ref_1 PO,
                                    aisd.alloc_criteria ALLOC_CRITERIA,
                                    wh.WH,
                                    wh.WH_DESC,
                                    NULL AVAIL_QTY,
                                    NULL INACTIVE_QTY 
                    from 
                            alloc_itm_search_dtl aisd,
                            item_dtl itd,
                            hier1 h1,
                            hier2 h2,
                            hier3 h3,
                            warehouse wh,
                            diffs df
                    where 
                            aisd.item       =   itd.item and 
                            aisd.hier1      =   h1.hier1 and
                            aisd.hier2      =   h2.hier2 and 
                            aisd.hier3      =   h3.hier3 and
                            aisd.diff_id    =   df.diff 
                            and aisd.loc    =   wh.wh """

            for row in json_object:
                if row == "ALLOC_NO":
                    query = query+" and aisd.ALLOC_NO = '{}'".format(json_object[row])
                if row == "ITEM":
                    query = query+" and itd.item = '{}'".format(json_object[row])
                if row == "DIFF_ID":
                    query = query+" and aisd.diff_id = '{}'".format(json_object[row])
                if row == "WAREHOUSE":
                    query = query+" and wh.WH = '{}'".format(json_object[row])
            results55 = pd.read_sql(query,connection)

            res_list=[]
            rec={}
            results55 =  results55.replace(np.NaN, "NULL", regex=True)
            for val2 in results55.values:
                count=0
                for col4 in results55.columns:
                    rec[col4] = val2[count]
                    count = count+1
                for col in rec:
                    if rec[col] == "NULL" or rec[col] == "" or  rec[col] == None:
                        rec[col] = ""
                res_list.append(rec.copy()) 
            return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
'''
        ********************************
        ********************************
             RULES & LOCATION END
        ********************************
        ********************************
    
'''


'''
        ********************************
        ********************************
            ALLOCATION FUNCTIONS
        ********************************
        ********************************
    
'''

# FUNCTION CALL FUNCTIION SWITCHING TABS FROM CREATE SCREEEN FOR "LIKE ITEM MAPPING" AND "RULES & LOCATION"
#@csrf_exempt
#def Alloc_switchTab(request):
#    if request.method == 'POST':
#        try:
#            data = json.loads(request.body)
#            print("s_datra: ",data)
#            data=data[0]
#            if "ALLOC_NO" in data:
#                result=wrapper_load_source(connection,data["ALLOC_NO"],None)
#                return JsonResponse({"status": 200, "message":"DATA INSERTED"})           
#        except Exception as error:
#            return JsonResponse({"status": 500, "message":str(error)})
#        except ValueError:
#            return JsonResponse({"status": 500, "message": "error"})


@csrf_exempt
def Alloc_switchTab(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data = data[0]
            #print("data: ",data)
            mycursor = connection.cursor()
            if "CHANGE_CREATE_AVAIL_GRID" in data and len(data["CHANGE_CREATE_AVAIL_GRID"])>0:
                update_CreateGrid_Avail_table(data["CHANGE_CREATE_AVAIL_GRID"])

            if "CHANGE_CREATE_GRID" in data and len(data["CHANGE_CREATE_GRID"])>0:
                update_CreateGrid_table(data["CHANGE_CREATE_GRID"])

            if "RECALC_IND" in data and len(data["RECALC_IND"])>0:
                query1="update alloc_head set RECALC_IND ='Y' where ALLOC_NO = '{}'".format(data["ALLOC_NO"])+";"
                mycursor.execute(query1)

            if "SPLIT_IND" in data and len(data["SPLIT_IND"])>0:
                update_split_ind(data["SPLIT_IND"])
            else:
                query1="update alloc_itm_search_dtl set SPLIT_IND ='N' where ALLOC_NO = '{}'".format(data["ALLOC_NO"])+";"
                mycursor.execute(query1)

            if "HEADER" in data and len(data["HEADER"]) > 0:
                json_object = data["HEADER"]
                Alloc_head_Data = pd.read_sql("select * from alloc_head where ALLOC_NO = '{}'".format(json_object["ALLOC_NO"])+";",connection)
                if len(Alloc_head_Data) > 0:
                    Alloc_head_Data = Alloc_head_Data.to_dict("records")
                    json_object["CREATE_DATETIME"]=str(datetime.now())

                    for col in json_object:
                        if col=="ALLOC_CRITERIA":
                            if json_object[col]=="Warehouse":
                                json_object[col]="W"
                            if json_object[col]=="Purchase Order":
                                json_object[col]="P"
                            if json_object[col]=="Pre Buy":
                                json_object[col]="F"
                            if json_object[col]=="Transfer":
                                json_object[col]="T"
                            if json_object[col]=="ASN":
                                json_object[col]="A"
                    
                    Alloc_head_Data = Alloc_head_Data[0]
                    for col in Alloc_head_Data:
                        if Alloc_head_Data[col] == "" or Alloc_head_Data[col] == "NULL" or Alloc_head_Data[col] == None:
                            Alloc_head_Data[col] = ""
                    common_columns = ['RELEASE_DATE', 'CONTEXT', 'PROMOTION', 'ALLOC_DESC']

                    Check_header_details = False
                    for key in common_columns:
                        if Check_header_details == False:
                            if key in Alloc_head_Data and key in json_object and str(Alloc_head_Data[key]) == str(json_object[key]):
                                Check_header_details = False
                            else:
                                Check_header_details = True
                    print("CHECK_HEADER_DETAILS: ",Check_header_details)
                    if Check_header_details == True:
                        result1,err_msg1 = update_alloc_wrapper(connection,json_object["ALLOC_NO"],json_object["ALLOC_LEVEL"],json_object["RELEASE_DATE"],'Y',json_object)
                        if result1 == False and len(err_msg1) > 0:
                            return JsonResponse({"status": 500, "message":str(err_msg1)})
                        connection.commit()

            if "UPDATE" in data:
                response,err_msg  = update_ind(data["UPDATE"])
                #if response == False and len(err_msg) > 0:
                #    return JsonResponse({"status": 500, "message":str(err_msg)})
                print("SWITCH TAB UPDATE_IND:",response)
               
            result = False
            result,err_msg2  = wrapper_load_source(connection,data["ALLOC_NO"],None)
            print("##############################")
            # print("SWITCH TAB: ",result)
            #if "UPDATE" in data:
            #    response = update_ind(data["UPDATE"])
            #    print("Alloc_switchTab update_ind:",response)
               
            #    if response and "ALLOC_NO" in data:
            #        result=wrapper_load_source(connection,data["ALLOC_NO"],None)
            #        if result:
            #            if data["TAB"] == 2:
            #                lim_data=retrieve_LIM(data["ALLOC_NO"])
            #                return JsonResponse([lim_data,1], content_type="application/json",safe=False)   
            #            elif data["TAB"] == 3:
            #                qty_data= Alloc_qty_limits_retrive_table_function(data["ALLOC_NO"])
            #                return JsonResponse([qty_data,2], content_type="application/json",safe=False)
            #            elif data["TAB"] == 1:
            #                return JsonResponse([3], content_type="application/json",safe=False)
            #            else:
            #                return JsonResponse({"status": 201, "message": "SWITCH: INSERTED"})
            #        else:
            #            return JsonResponse({"status": 500, "message": "ERROR: LOAD ITEM SOURCE"})
            #    else:
            #        return JsonResponse({"status": 500, "message": "ERROR: UPDATE FAILED"})
            #else:
            #    return JsonResponse({"status": 500, "message": "ERROR: UPDATE DATA MISSING"})
            if result:
                if data["TAB"] == 1:
                    return JsonResponse({"status": 200, "message": [[],1]}) 

                elif data["TAB"] == 2:
                    lim_data,err_msg3 = retrieve_LIM(data["ALLOC_NO"])
                    if len(err_msg3) > 0:
                        return JsonResponse({"status": 500, "message":str(err_msg3)})
                    return JsonResponse({"status": 200, "message": [lim_data,2]})

                elif data["TAB"] == 3:
                    qty_data,err_msg4 = Alloc_qty_limits_retrive_table_function(data["ALLOC_NO"])
                    if len(err_msg4) > 0:
                        return JsonResponse({"status": 500, "message":str(err_msg4)})
                    return JsonResponse({"status": 200, "message": [qty_data,3]})
                else:
                    return JsonResponse({"status": 200, "message": ["SWITCH: INSERTED",0]})
            else:
                return JsonResponse({"status": 500, "message": str(err_msg2)}) # ["Allocated units is greater than the remaining units.",[data]]
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})

   
def update_ind(data):
    O_status = 0
    L_fun ="UPDATE IND"
    try:  
        print(L_fun)
        mycursor = connection.cursor()
        D_keys=[]
        #status
        O_status = 1 
        for row in data:
            query1="update alloc_itm_search_dtl set SEL_IND ='N' where ALLOC_NO = '{}'".format(row["ALLOC_NO"])+";"
            mycursor.execute(query1)
        D_keys=[]
        for row in data:
            for col in row:
                if row[col]==None or row[col]=="NULL" or row[col]=="NaN" or row[col]=="":
                    D_keys.append(col)
            for key in D_keys:
                row.pop(key)
            D_keys.clear()

        list1=[]
        for row in data:
            for key in row:
                if key not in ['ALLOC_NO', 'ALLOC_CRITERIA', 'ITEM', 'DIFF_ID', 'HIER1', 'HIER2', 'HIER3', 'VPN','LOC','REF_1','REF_2']:
                    list1.append(key)
            for k in list1:
                row.pop(k)
            list1.clear()

        for json_object in data:
            for col in json_object:
                if col=="ALLOC_CRITERIA":
                    if json_object[col]=="Warehouse":
                        json_object[col]="W"
                    if json_object[col]=="Purchase Order":
                        json_object[col]="P"
                    if json_object[col]=="Pre Buy":
                        json_object[col]="F"
                    if json_object[col]=="Transfer":
                        json_object[col]="T"
                    if json_object[col]=="ASN":
                        json_object[col]="A"

        O_status = 2                   
        for row in data:
            query="update alloc_itm_search_dtl set SEL_IND = 'Y' where "
            for col in row:
                query=query+ col +" = '"+ str(row[col]) +"' and "
            query = query[:-4]+";"
            #print(query)
            mycursor.execute(query)
        connection.commit()
        return True,""
    except Exception as error:
        err_return = ""
        if O_status==1: 
            err_return = L_fun+": "+str(O_status)+": Exception raised during updating sel_ind to 'N' : "+error
            #print(L_fun,":",O_status,":","Exception raised during updating sel_ind to 'N' :", error)
        elif O_status==2:
            err_return = L_fun+": "+str(O_status)+": Exception raised during updating sel_ind to 'Y': "+error
            #print(L_fun,":",O_status,":","Exception raised during updating sel_ind to 'Y':", error)
        return False, err_return


def update_split_ind(data):
    O_status = 0
    L_fun ="UPDATE SLPIT IND"
    try:        
        mycursor = connection.cursor()
        #print("update_split_ind data",data)
        D_keys=[]
        #status
        O_status = 1 
        for row in data:
            query1="update alloc_itm_search_dtl set SPLIT_IND ='N' where ALLOC_NO = '{}'".format(row["ALLOC_NO"])+";"
            mycursor.execute(query1)
        D_keys=[]
        for row in data:
            for col in row:
                if row[col]==None or row[col]=="NULL" or row[col]=="NaN" or row[col]=="":
                    D_keys.append(col)
            for key in D_keys:
                row.pop(key)
            D_keys.clear()

        list1=[]
        for row in data:
            for key in row:
                if key not in ['ALLOC_NO', 'ALLOC_CRITERIA', 'ITEM', 'DIFF_ID', 'HIER1', 'HIER2', 'HIER3', 'VPN','LOC','REF_1','REF_2']:
                    list1.append(key)
            for k in list1:
                row.pop(k)
            list1.clear()

        for json_object in data:
            for col in json_object:
                if col=="ALLOC_CRITERIA":
                    if json_object[col]=="Warehouse":
                        json_object[col]="W"
                    if json_object[col]=="Purchase Order":
                        json_object[col]="P"
                    if json_object[col]=="Pre Buy":
                        json_object[col]="F"
                    if json_object[col]=="Transfer":
                        json_object[col]="T"
                    if json_object[col]=="ASN":
                        json_object[col]="A"

        O_status = 2                   
        for row in data:
            query="update alloc_itm_search_dtl set SPLIT_IND = 'Y' where "
            for col in row:
                query=query+ col +" = '"+ str(row[col]) +"' and "
            query = query[:-4]+";"
            mycursor.execute(query)
        connection.commit()
        return True
    except Exception as error:
        if O_status==1: 
            print(L_fun,":",O_status,":","Exception raised during updating split_ind to 'N' :", error)
        elif O_status==2:
            print(L_fun,":",O_status,":","Exception raised during updating split_ind to 'Y':", error)

def df_conversion(data):
    res_list=[]
    rec={}
    if len(data)>0:
        data =  data.replace(np.NaN, "NULL", regex=True)
        for val2 in data.values:
            count=0
            for col4 in data.columns:
                rec[col4]=val2[count]
                count=count+1
            for col in rec:
                if rec[col]==None or rec[col]=="NULL":
                    rec[col]=""
            res_list.append(rec.copy()) 
    return res_list



@csrf_exempt
def calculation(request):
    if request.method == 'POST':
        try:
            data=json.loads(request.body)
            data=data[0]
            mycursor = connection.cursor()
            update_Q='''UPDATE alloc_head
                  SET batch_ind = 'O',
                      process_ind = NULL
               WHERE alloc_no = %s'''
            mycursor.execute(update_Q,(data["ALLOC_NO"],))
            print("UPDATE CHECK:", mycursor.rowcount)

            if "UPDATE" in data:
                print("EXECUTING UPDATE_IND ")
                check, err_msg = update_ind(data["UPDATE"]) 
                #if check == False and len(err_msg) > 0:
                #    return JsonResponse({"status": 500, "message":str(err_msg)})
                print("EXECUTED UPDATE_IND ")

            if "CHANGE_CREATE_AVAIL_GRID" in data  and len(data["CHANGE_CREATE_AVAIL_GRID"])>0:
                print("EXECUTING AVAILABLE QTY ")
                update_CreateGrid_Avail_table(data["CHANGE_CREATE_AVAIL_GRID"])
                print("EXECUTED AVAILABLE QTY ")

            if "CHANGE_CREATE_GRID" in data  and len(data["CHANGE_CREATE_GRID"])>0:
                print("EXECUTING HOLDBACK/SOM ")
                update_CreateGrid_table(data["CHANGE_CREATE_GRID"])
                print("EXECUTED HOLDBACK/SOM ")

            if "SPLIT_IND" in data and len(data["SPLIT_IND"])>0:
                print("EXECUTING SPLIT ")
                update_split_ind(data["SPLIT_IND"])
                print("EXECUTED SPLIT ")
            else:
                query1="update alloc_itm_search_dtl set SPLIT_IND ='N' where ALLOC_NO = '{}'".format(data["ALLOC_NO"])+";"
                mycursor.execute(query1)
            
            if "HEADER" in data and len(data["HEADER"]) > 0:
                print("EXECUTING HEADER")
                json_object = data["HEADER"]
                Alloc_head_Data = pd.read_sql("select * from alloc_head where ALLOC_NO = '{}'".format(json_object["ALLOC_NO"])+";",connection)
                if len(Alloc_head_Data) > 0:
                    Alloc_head_Data = Alloc_head_Data.to_dict("records")
                    json_object["CREATE_DATETIME"]=str(datetime.now())

                    for col in json_object:
                        if col=="ALLOC_CRITERIA":
                            if json_object[col]=="Warehouse":
                                json_object[col]="W"
                            if json_object[col]=="Purchase Order":
                                json_object[col]="P"
                            if json_object[col]=="Pre Buy":
                                json_object[col]="F"
                            if json_object[col]=="Transfer":
                                json_object[col]="T"
                            if json_object[col]=="ASN":
                                json_object[col]="A"
                    
                    Alloc_head_Data = Alloc_head_Data[0]
                    for col in Alloc_head_Data:
                        if Alloc_head_Data[col] == "" or Alloc_head_Data[col] == "NULL" or Alloc_head_Data[col] == None:
                            Alloc_head_Data[col] = ""
                    common_columns = ['RELEASE_DATE', 'CONTEXT', 'PROMOTION', 'ALLOC_DESC']

                    Check_header_details = False
                    for key in common_columns:
                        if Check_header_details == False:
                            if key in Alloc_head_Data and key in json_object and str(Alloc_head_Data[key]) == str(json_object[key]):
                                Check_header_details = False
                            else:
                                Check_header_details = True
                    print("CHECK_HEADER_DETAILS: ",Check_header_details)

                    if Check_header_details == True:
                        result1,err_msg1 = update_alloc_wrapper(connection,json_object["ALLOC_NO"],json_object["ALLOC_LEVEL"],json_object["RELEASE_DATE"],'Y',json_object)
                        if result1 == False:
                            return JsonResponse({"status": 500, "message":str(err_msg1)})
                print("EXECUTED HEADER")
                
            query1 = "select RELEASE_DATE from alloc_head where alloc_no = {}".format(data["ALLOC_NO"])+" ;"
            df_result1 = pd.read_sql(query1,connection)
            result1 = df_result1.to_dict("records")
            RELEASE_DATE = ""
            for row in result1:
                RELEASE_DATE = row["RELEASE_DATE"]

            if RELEASE_DATE >= date.today():
                query1 = "select * from alloc_rule where alloc_no=%s"
                mycursor.execute(query1,(str(data["ALLOC_NO"]),))
                res1   = mycursor.rowcount
                query2 = "select 1 from alloc_location where LOC_GROUP_ID in (select LOC_GROUP_ID from alloc_loc_group_detail where  alloc_no=%s)"
                mycursor.execute(query2,(str(data["ALLOC_NO"]),))
                res2   = mycursor.rowcount
                #print("after execution res12",res1,res2)
                if res1 == 1 and res2 > 0:
                    query3  = "select EXACT_IND from alloc_rule where alloc_no={}".format(data["ALLOC_NO"])
                    ext_ind = pd.read_sql(query3,connection)
                    query4  = "select MAX from alloc_quantity_limits where alloc_no=%s and max is not null"
                    result  = pd.read_sql(query4,connection,params=(str(data["ALLOC_NO"]),))
                    print("EXT IND:", ext_ind["EXACT_IND"][0],result)

                    if (ext_ind["EXACT_IND"][0] == "N" and len(result) > 0):
                        return JsonResponse({"status": 500, "message": "MAX QTY LIMITS CAN NOT BE USED WHEN USING PROPORTIONAL RULE."})
                    else:
                        query4 = "select ALLOC_CRITERIA from alloc_head where alloc_no= {}".format(data["ALLOC_NO"])
                        ALLOC_CRITERIA = pd.read_sql(query4,connection)
                        if (ALLOC_CRITERIA["ALLOC_CRITERIA"][0] == "F"):
                            print("EXECUTING DO_WHATIF_CALC")
                            result,err_msg2 = do_whatif_calc(connection,data["ALLOC_NO"])
                            print("EXECUTED DO_WHATIF_CALC",result,err_msg2)
                            if result:
                                return JsonResponse({"status": 201, "message": str(data["ALLOC_NO"])+":Calculation Successful."})
                            else:
                                if len(err_msg2) > 0:
                                    return JsonResponse({"status": 500, "message":str(err_msg2)})
                                return JsonResponse({"status": 500, "message": str(data["ALLOC_NO"])+":Calculation Failed."})
                        else:
                            print("EXECUTING DO_CALCULATION")
                            result,err_msg3 = do_calculation(connection,data["ALLOC_NO"])
                            print("EXECUTED DO_CALCULATION : ",result,err_msg3)
                            if result:
                                return JsonResponse({"status": 201, "message": str(data["ALLOC_NO"])+":Calculation Successful."})
                            else:
                                if len(err_msg3) > 0:
                                    return JsonResponse({"status": 500, "message":str(err_msg3)})
                                return JsonResponse({"status": 500, "message": str(data["ALLOC_NO"])+":Calculation Failed."})                                                                                                                                                                                                                         
                else:
                    return JsonResponse({"status": 500, "message": "Enter allocation rules/location details."})
            else:
                return JsonResponse({"status": 500, "message": "Release date cannot be older than the current date."})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})



'''
        ********************************
        ********************************
            ALLOCATION SUMMARY
        ********************************
        ********************************
    
'''
@csrf_exempt
def massApprove(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]   
            if conn_global != None:
                mycursor = conn_global.cursor()
                if "UPDATE" in data and len(data["UPDATE"])>0:  
                    query_updateAll = "UPDATE alloc_summary SET selected = 'N';"
                    query = "UPDATE alloc_summary SET selected = 'Y' WHERE alloc_no IN (%s);"
                    
                    placeholders = ', '.join(['%s'] * len(data["UPDATE"]))
                    query = query % placeholders
                    mycursor.execute(query_updateAll)
                    mycursor.execute(query,data["UPDATE"])
                    result,err_msg = sel_approve_wrapper(conn_global)
                    print("MASS APPROVE ERROR :: ",err_msg,mycursor.rowcount)
                    if isinstance(result, pd.DataFrame):
                        # Handle the case when 'data' is a DataFrame
                        if result.empty:
                            return JsonResponse({"status": 500, "message": "MASS APPROVAL: NO DATA FOUND."})
                        else: 
                            print("MASS APPROVE RESULT :: ",result,result.to_dict("records"))
                            res_data = result.to_dict("records") if len(result) >0 else []
                            return JsonResponse(res_data, content_type="application/json",safe=False) 
                    else:
                        return JsonResponse({"status": 500, "message": "MASS APPROVAL: "+ str(err_msg)})
                else:
                    return JsonResponse({"status": 500, "message": "INVALID INPUT"}) 
            else:
                return JsonResponse({"status": 500, "message": "CONNECTION LOST"}) 
        except Exception as error:
            return JsonResponse({"status": 500, "message": "MASS APPROVAL: "+str(error)})

@csrf_exempt
def AllocSumm_CopyAlloc(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if conn_global != None:
                if "ALLOC_NO" in data:            
                    result,err_msg = to_call_ca_fun(conn_global,data["ALLOC_NO"],data["ALLOCATOR"])
                    print("COPY RESULT :: ",result)
                    if result == False:
                        return JsonResponse({"status": 500, "message": str(err_msg)})
                    else:
                        return JsonResponse([result], content_type="application/json",safe=False)                 
                else:
                    return JsonResponse({"status": 500, "message": "COPY ALLOC : INVALID INPUT"})   
            else:
                return JsonResponse({"status": 500, "message": "CONNECTION LOST"}) 
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})


@csrf_exempt

def AllocSumm_switchTab(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data = data[0]
            
            if conn_global:
                result, err_msg = switch_window(conn_global)
                if len(result) == 0:                    
                    return JsonResponse({"status": 500, "message": str(err_msg)})
                else:
                    result =  result.replace(np.NaN, "", regex=True)
                    result =  result.to_dict("records")
                    for rec in result:
                        for col in rec:
                            if col == "DOC_TYPE":
                                if rec[col]  == "W":
                                    rec[col]  = "Warehouse"
                                if rec[col]  == "P":
                                    rec[col]  = "Purchase Order"
                                if rec[col]  == "F":
                                    rec[col]  = "Pre Buy"
                                if rec[col]  == "T":
                                    rec[col]  = "Transfer"
                                if rec[col]  == "A":
                                    rec[col]  = "ASN"
                            if col == "RELEASE_DATE" and len(str(rec["RELEASE_DATE"])) > 0:
                                date_object = datetime.strptime(str(rec["RELEASE_DATE"]), '%Y-%m-%d')
                                rec["RELEASE_DATE"] = date_object.strftime('%m-%d-%y')
                            if col == "CREATED_DATE" and len(str(rec["CREATED_DATE"])) > 0:
                                date_object = datetime.strptime(str(rec["CREATED_DATE"]), '%Y-%m-%d')
                                rec["CREATED_DATE"] = date_object.strftime('%m-%d-%y')
                                 
                    return JsonResponse(result, content_type="application/json",safe=False) 
            else:
                return JsonResponse({"status": 500, "message": "SWITCH : CONNECTION LOST"})
                                                                                                    
        except Exception as error:
            return JsonResponse({"status": 500, "message": "SWITCH: "+str(error)})


@csrf_exempt
def AllocSumm_search(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            print("SUMMARY SEARCH DATA : ")
            #establish_connection(data["RANDOM_KEY"])
            establish_connection()

            I_search = {
                        "I_alloc_no"          :          str(data["ALLOC_NO"])                   if len(str(data["ALLOC_NO"]))>0            else "NULL",
			            "I_alloc_desc"        :          str(data["ALLOC_DESC"])                 if len(str(data["ALLOC_DESC"]))>0          else "NULL",
			            "I_from_release_date" :   '"'+   str(data["RELEASE_DATE_FROM"]) +'"'     if len(str(data["RELEASE_DATE_FROM"]))>0   else "NULL",
			            "I_to_release_date"   :   '"'+   str(data["RELEASE_DATE_TO"])   +'"'     if len(str(data["RELEASE_DATE_TO"]))>0     else "NULL",
			            "I_from_create_date"  :   '"'+   str(data["CREATE_DATE_FROM"])  +'"'     if len(str(data["CREATE_DATE_FROM"]))>0    else "NULL",
			            "I_to_create_date"    :   '"'+   str(data["CREATE_DATE_TO"])    +'"'     if len(str(data["CREATE_DATE_TO"]))>0      else "NULL",
			            "I_create_user"       :   '"'+   str(data["ALLOCATOR"])         +'"'     if len(str(data["ALLOCATOR"]))>0           else "NULL",
			            #"I_alloc_status"      :     "NULL",#str(data["STATUS"])                if len(str(data["STATUS"]))>0               else "NULL",
			            "I_po"                :   '"'+   str(data["PO"])                +'"'     if len(str(data["PO"]))>0                  else "NULL",
			            "I_tst_no"            :   '"'+   str(data["TSF"])               +'"'     if len(str(data["TSF"]))>0                 else "NULL",
			            "I_context"           :   '"'+   str(data["CONTEXT"])           +'"'     if len(str(data["CONTEXT"]))>0             else "NULL",
			            "I_promotion"         :   '"'+   str(data["PROMOTION"])         +'"'     if len(str(data["PROMOTION"]))>0           else "NULL",
			            "I_dept"              :   '"'+   str(data["HIER1"])             +'"'     if len(str(data["HIER1"]))>0               else "NULL",
			            "I_class"             :   '"'+   str(data["HIER2"])             +'"'     if len(str(data["HIER2"]))>0               else "NULL",
			            "I_subclass"          :   '"'+   str(data["HIER3"])             +'"'     if len(str(data["HIER3"]))>0               else "NULL",
			            "I_wh"                :   '"'+   str(data["WH"])                +'"'     if len(str(data["WH"]))>0                  else "NULL",
			            "I_item_parent"       :   '"'+   str(data["ITEM_PARENT"])       +'"'     if len(str(data["ITEM_PARENT"]))>0         else "NULL",
			            "I_diff_id"           :   '"'+   str(data["DIFF_ID"])           +'"'     if len(str(data["DIFF_ID"]))>0             else "NULL",
			            "I_item_sku"          :   '"'+   str(data["SKU"])               +'"'     if len(str(data["SKU"]))>0                 else "NULL",
			            "I_vpn"               :   '"'+   str(data["VPN"])               +'"'     if len(str(data["VPN"]))>0                 else "NULL",
			            "I_alloc_type"        :   '"'+   str(data["ALLOC_TYPE"])        +'"'     if len(str(data["ALLOC_TYPE"]))>0          else "'A'",
			            "I_source"            :   '"'+   str(data["SOURCE"])            +'"'     if len(str(data["SOURCE"]))>0              else "NULL",
			            "I_asn"               :   '"'+   str(data["ASN"])               +'"'     if len(str(data["ASN"]))>0                 else "NULL",
			            "I_pack_id"           :   '"'+   str(data["PACK_NO"])           +'"'     if len(str(data["PACK_NO"]))>0             else "NULL",
			            "I_batch_calc_ind"    :   '"'+   str(data["BATCH"])             +'"'     if len(str(data["BATCH"]))>0               else "'N'"
                        }
            
            #if "ALLOC_NO" not in data:
            #if data["LAST_RANDOM_KEY"] and  data["LAST_RANDOM_KEY"] in conn_dict:
            #    conn_dict[data["LAST_RANDOM_KEY"]].close()
            #    conn_dict[data["LAST_RANDOM_KEY"]].__exit__(None, None, None)
            #    conn_dict.pop(data["LAST_RANDOM_KEY"])
            #    print("CLOSE LAST CONNECTION")
            #print("Alloc_switchTab: ",conn_dict)
            if conn_global:
                #mycursor = conn_dict[data["RANDOM_KEY"]].cursor()
                mycursor = conn_global.cursor()
                D_query = "DELETE FROM alloc_status";
                mycursor.execute(D_query);
                if len(data["STATUS"])>0:
                    I_query = "INSERT INTO alloc_status VALUES {}".format(", ".join(["('{}', 'Y')".format(v) for v in data["STATUS"]]))
                    mycursor.execute(I_query);
                result,err_msg  =to_call_fun(conn_global,I_search)
                print(err_msg)
                if len(result) > 0:
                    result =  result.replace(np.NaN, "", regex=True)
                    result = result.to_dict("records")
                    for rec in result:
                        for col in rec:
                            if col == "DOC_TYPE":
                                if rec[col]  == "W":
                                    rec[col]  = "Warehouse"
                                if rec[col]  == "P":
                                    rec[col]  = "Purchase Order"
                                if rec[col]  == "F":
                                    rec[col]  = "Pre Buy"
                                if rec[col]  == "T":
                                    rec[col]  = "Transfer"
                                if rec[col]  == "A":
                                    rec[col]  = "ASN"
                            if col == "ALLOC_LEVEL_CODE":
                                if rec[col] == "Style/Diff":
                                    rec[col] = "Style/Variant"
                            if col == "RELEASE_DATE" and len(str(rec["RELEASE_DATE"])) > 0:
                                date_object = datetime.strptime(str(rec["RELEASE_DATE"]), '%Y-%m-%d')
                                rec["RELEASE_DATE"] = date_object.strftime('%m-%d-%y')
                            if col == "CREATED_DATE" and len(str(rec["CREATED_DATE"])) > 0:
                                date_object = datetime.strptime(str(rec["CREATED_DATE"]), '%Y-%m-%d')
                                rec["CREATED_DATE"] = date_object.strftime('%m-%d-%y')

                    return JsonResponse(result, content_type="application/json",safe=False)
                else:
                    if len(str(err_msg)) >0 :
                        return JsonResponse({"status": 500, "message": str(err_msg)})
                    return JsonResponse({"status": 500, "message": "NO DATA FOUND"})
            else:
                return JsonResponse({"status": 500, "message": "CONNECTION LOST"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": "SEARCH Exception occured : "+str(error)})
            
@csrf_exempt
def approve_createScreen_table(request):
    if request.method == 'POST':
        try:
            data=json.loads(request.body)
            data=data[0]
            print("\nApprove",)
            mycursor = connection.cursor()
            if "ALLOC_NO" in data:
                query1 = "select RELEASE_DATE from alloc_head where alloc_no = {}".format(data["ALLOC_NO"])+" ;"
                df_result1 = pd.read_sql(query1,connection)
                result1 = df_result1.to_dict("records")
                RELEASE_DATE=""
                for row in result1:
                    RELEASE_DATE=row["RELEASE_DATE"]
                if RELEASE_DATE >= date.today():
                    result,err_msg = approve_wrapper(connection,data["ALLOC_NO"])
                    #result=do_calculation(connection,data["ALLOC_NO"])
                    if result == True:
                        return JsonResponse({"status": 201, "message": str(data["ALLOC_NO"])+": Allocation Approved Successful."})
                    else:
                        return JsonResponse({"status": 500, "message": str(data["ALLOC_NO"])+": Allocation Approved Failed. ("+str(err_msg)+")"}) 
                else:
                    return JsonResponse({"status": 500, "message": "Release date cannot be older than the current date."})
            return JsonResponse({"status": 201, "message": "Data Updated"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})


@csrf_exempt
def approve_valid_createScreen_table(request):
    if request.method == 'POST':
        try:
            data=json.loads(request.body)
            data=data[0]
            mycursor = connection.cursor()
            if "ALLOC_NO" in data:
                query1 = "select RECALC_IND from alloc_head where alloc_no = {}".format(data["ALLOC_NO"])+" ;"
                query2 = "select distinct TRAN_ITEM,SKU_CALC_QTY from alloc_calc_item_loc where alloc_no = {}".format(data["ALLOC_NO"])+" ;"
                df_result1 = pd.read_sql(query1,connection)
                df_result2 = pd.read_sql(query2,connection)
                result1 = df_result1.to_dict("records")
                result2 = df_result2.to_dict("records")
                List_value=[]
                Recalc_ind=""
                for row in result1:
                    Recalc_ind=row["RECALC_IND"]
                for row in result2:
                    if int(row["SKU_CALC_QTY"])>0:
                        List_value.append(int(row["SKU_CALC_QTY"]))
                if Recalc_ind=='N' and len(List_value)>0:
                    print("ALLOCATION VALID FOR APPROVED")
                    return JsonResponse({"status": 200, "message": str(data["ALLOC_NO"])+": Allocation Valid for Approved."})
                else:
                    return JsonResponse({"status": 500, "message": str(data["ALLOC_NO"])+": Allocation Invalid for Approved."})
            return JsonResponse({"status": 201, "message": "Data Updated"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})



@csrf_exempt
def createScreen_grid_table(request):
    if request.method == 'POST':
        try:
            data=json.loads(request.body)
            data=data[0]
            if "ALLOC_NO" in data:
                query="select * from alloc_itm_search_dtl where ALLOC_NO= {} ".format(data["ALLOC_NO" ])+ "order by loc ;"               
                result=df_conversion(pd.read_sql(query,connection))
                if len(result) > 0:
                    for rec in result:
                        for col in rec:
                            if col == "ALLOC_CRITERIA":
                                if rec[col]  == "W":
                                    rec[col]  = "Warehouse"
                                if rec[col]  == "P":
                                    rec[col]  = "Purchase Order"
                                if rec[col]  == "F":
                                    rec[col]  = "Pre Buy"
                                if rec[col]  == "T":
                                    rec[col]  = "Transfer"
                                if rec[col]  == "A":
                                    rec[col]  = "ASN"
                return JsonResponse(result,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
                          
                                                                    
@csrf_exempt
def ASMY_Validation(request):
    if request.method == 'POST':
        try:
            data=json.loads(request.body)
            data=data[0]
            if "ALLOC_NO" in data:
                rule_type = ""
                query1 = "select count(alloc_no) from alloc_rule where ALLOC_NO = {} ".format(data["ALLOC_NO" ])     
                query2 = "select count(alloc_no) from alloc_loc_group_detail where ALLOC_NO = {} ".format(data["ALLOC_NO" ])
                query3 = "select * from alloc_rule where ALLOC_NO = {} ".format(data["ALLOC_NO" ])
                result = pd.read_sql(query3,connection)
                if len(result)>0:
                    result_list = result.to_dict("records")[0]
                    rule_type = result_list["RULE_TYPE"]
                valid1 = ((pd.read_sql(query1,connection)).to_dict("records"))[0]
                valid2 = ((pd.read_sql(query2,connection)).to_dict("records"))[0]

                avail_update_check,err_msg = update_inv_wrapper(connection,data["ALLOC_NO" ])
                if avail_update_check == False:
                    return JsonResponse({"status": 500, "message": str(err_msg)})

                print("\n\nUPDATE_INV_WRAPPER Result  :: ",avail_update_check)
                if valid1["count(alloc_no)"] >0 and valid2["count(alloc_no)"]>0 and rule_type != "M":
                    return JsonResponse([True],content_type="application/json",safe=False)
                else:
                    return JsonResponse([False],content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})




@csrf_exempt
def reverse_createScreen_table(request):
    if request.method == 'POST':
        try:
            data=json.loads(request.body)
            data=data[0]
            mycursor = connection.cursor()
            if "ALLOC_NO" in data:
                query1 = "select RELEASE_DATE from alloc_head where alloc_no = {}".format(data["ALLOC_NO"])+" ;"
                df_result1 = pd.read_sql(query1,connection)
                result1 = df_result1.to_dict("records")
                RELEASE_DATE = ""
                for row in result1:
                    RELEASE_DATE = row["RELEASE_DATE"]
                if RELEASE_DATE >= date.today():
                    result,err_msg = reserve_wrapper(connection,data["ALLOC_NO"])
                    if result == True:
                        return JsonResponse({"status": 201, "message": str(data["ALLOC_NO"])+": Allocation Reserve Successful."})
                    else:
                        return JsonResponse({"status": 500, "message": str(data["ALLOC_NO"])+
                                             ": Allocation Reserve Failed. ("+str(err_msg)+")"}) 
                else:
                    return JsonResponse({"status": 500, "message": "Release date cannot be older than the current date."})
            return JsonResponse({"status": 201, "message": "Data Updated"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})


@csrf_exempt
def worksheet_createScreen_table(request):
    if request.method == 'POST':
        try:
            data=json.loads(request.body)
            data=data[0]
            if "ALLOC_NO" in data:
                result,err_msg = worksheet_wrapper(connection,data["ALLOC_NO"])
                if result == True:
                    return JsonResponse({"status": 201, "message": str(data["ALLOC_NO"])+": Allocation Status is changed Successful."})
                else:
                    return JsonResponse({"status": 500, "message": str(data["ALLOC_NO"])+
                                         ": Allocation Status is changed Failed. ("+str(err_msg)+")"})
            return JsonResponse({"status": 201, "message": "Data Updated"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})


'''
        ********************************
        ********************************
                SCHEDULE START
        ********************************
        ********************************
    
'''
@csrf_exempt
def schdl_save(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if len(data.keys())>0:
                I_alloc             =         str(data["ALLOC_NO"])
                I_status            =         data["STATUS"]
                I_start_date        =         data["DATE_FROM"]    
                I_end_date          =         data["DATE_TO"]      
                I_frequency         =         data["FREQUENCY"] 
                I_days_of_week      =         data["DAY"]       
                I_createId          =         data["CREATE_ID"]
                I_last_update_id    =         data["MODIFIED_BY"]
                I_next_schedule_run =         None
                I_alloc_type        =         None
              
                result,err_msg =update_alloc_parms_wrapper(connection,I_alloc,            
                                                                I_status,           
                                                                I_start_date,       
                                                                I_end_date,         
                                                                I_frequency,        
                                                                I_days_of_week ,    
                                                                I_next_schedule_run,
                                                                I_alloc_type,I_createId,I_last_update_id)   
                
                if result:
                    mycursor = connection.cursor()
                    query = "update alloc_head set Status = 'SCHD' where ALLOC_NO = '{}'".format(data["ALLOC_NO"])+";"
                    mycursor.execute(query)
                    connection.commit()
                    return JsonResponse({"status": 200, "message":" SCHEDULE SUCCESS"})
                else:
                    if len(err_msg)>0:
                        return JsonResponse({"status": 500, "message":str(err_msg)})
                    return JsonResponse({"status": 500, "message":"ERROR : SCHEDULE FAILED"})    
            else:
                return JsonResponse({"status": 500, "message":"ERROR : Invalid Input"})  
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})

@csrf_exempt
def schdl_rtv(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if "ALLOC_NO" in data:
                result,err_msg = retreive_schedule_data(connection,data["ALLOC_NO"])
                if len(result)>0:
                    result =  result.replace(np.NaN, "", regex=True)
                    return JsonResponse(result.to_dict("records"), content_type="application/json",safe=False) 
                else:
                    return JsonResponse([], content_type="application/json",safe=False) 
            return JsonResponse({"status": 500, "message":"ERROR : Invalid Input"}) 
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})

'''
        ********************************
        ********************************
                SCHEDULE END
        ********************************
        ********************************
    
'''

@csrf_exempt
def Alloc_split_button_create_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            data=json_object[0]  
            mycursor = connection.cursor()
            if "UPDATE" in data and len(data["UPDATE"])>0:
                check, err_msg = update_ind(data["UPDATE"]) 
                #if check == False and len(err_msg) > 0:
                #    return JsonResponse({"status": 500, "message":str(err_msg)})
            else:
                query1="update alloc_itm_search_dtl set SEL_IND ='N' where ALLOC_NO = '{}'".format(data["ALLOC_NO"])+";"
                mycursor.execute(query1)

            if "CHANGE_CREATE_AVAIL_GRID" in data  and len(data["CHANGE_CREATE_AVAIL_GRID"])>0:
                update_CreateGrid_Avail_table(data["CHANGE_CREATE_AVAIL_GRID"])

            if "CHANGE_CREATE_GRID" in data  and len(data["CHANGE_CREATE_GRID"])>0:
                update_CreateGrid_table(data["CHANGE_CREATE_GRID"])

            if "SPLIT_IND" in data and len(data["SPLIT_IND"])>0:
                update_split_ind(data["SPLIT_IND"])
            else:
                query1="update alloc_itm_search_dtl set SPLIT_IND ='N' where ALLOC_NO = '{}'".format(data["ALLOC_NO"])+";"
                mycursor.execute(query1)

            if "ALLOC_NO" in data:
                result,err_msg = split_func(connection,data["ALLOC_NO"],data["ALLOCATOR"])
                if result == False or result == []:
                    if len(err_msg) > 0:
                        return JsonResponse({"status": 500, "message": str(err_msg)})
                    return JsonResponse({"status": 500, "message": "Split is failed"})
                else:
                    return JsonResponse({"status": 200, "message": "New Alloction No: "+str(result)})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        finally:
                connection.close()



'''
        *********************************
         ACTIVE SESSIONS - ALLOC SUMMARY
        *********************************
    
'''

@csrf_exempt
def log_active_session(request):
    if request.method == 'POST':
        try:
            json_object         = json.loads(request.body)
            data                = json_object[0]            
            alloc_no            = data["ALLOC_NO"]
            mode                = data["MODE"]       
            user                = data["USER"]
            tab                 = data["TAB"]
            current_timestamp   = datetime.now() 
            mycursor            = connection.cursor()
            print("LOG ACTIVE")
            I_query       = "INSERT INTO active_alloc_sessions(ALLOC_NO,SESSION_MODE,CREATE_ID,CREATE_TIME) VALUES(%s,%s,%s,current_timestamp); "
            S_query       = "SELECT CREATE_ID FROM active_alloc_sessions WHERE ALLOC_NO = %s; "
            DL_query      = '''DELETE FROM active_alloc_sessions
                                WHERE CREATE_TIME < NOW() - INTERVAL 15 MINUTE;'''# and SESSION_MODE <> 'CREATE';
            mycursor.execute(DL_query)
            print("DELETED INACTIVE SESSION : ",mycursor.rowcount)

            mycursor.execute(S_query,(alloc_no,))
            if mycursor.rowcount == 0 :
                mycursor.execute(I_query,(alloc_no,mode,user,))                
                if mycursor.rowcount == 0 :
                    return JsonResponse({"status": 500, "message": "log_active_session: failed"})
                print("LOCKED :" ,alloc_no)
                return JsonResponse({"status": 200, "message": "SUCCESS",})
            r_data = mycursor.fetchall()
            current_user = r_data[0][0]            
            #print(r_data,'\n user :',current_user    )
            return JsonResponse({"status": 500, "message": "SESSION RUNNING" +(" CREATE" if tab!="ASM" else ""), "USER": current_user})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        finally:
            connection.commit()

@csrf_exempt
def delete_active_session(request):
    if request.method == 'POST':
        try:
            json_object         = json.loads(request.body)
            data                = json_object[0]    
            print("DEL SESSION DATA:: ")
            alloc_no            = data["ALLOC_NO"]
            mode                = data["MODE"]       
            user                = data["USER"]
            current_timestamp   = datetime.now()
            mycursor            = connection.cursor()
            print(mode,"DEL LOG ACTIVE ::",alloc_no,)
            D_query       = "DELETE FROM active_alloc_sessions WHERE ALLOC_NO = %s and SESSION_MODE= %s; "
            DL_query      = '''DELETE FROM active_alloc_sessions
                                WHERE CREATE_TIME < NOW() - INTERVAL 15 MINUTE ;
                            '''#--and SESSION_MODE <> 'CREATE';
            mycursor.execute(DL_query)
            print("DELETED All ACTIVE SESSION : ",mycursor.rowcount)
            mycursor.execute(D_query,(alloc_no,mode,))
            print("DELETED CURRENT SESSION : ",mycursor.rowcount)
            if mycursor.rowcount == 0 :
                print('ACTIVE LOG: ',alloc_no,mode,type(alloc_no))
                return JsonResponse({"status": 500, "message": "del_active_session: failed"})
            return JsonResponse({"status": 200, "message": "SESSION DELETED"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        finally:
            connection.commit()

@csrf_exempt
def update_actv_session(request):
    if request.method == 'POST':
        try:
            json_object         = json.loads(request.body)
            data                = json_object[0]            
            alloc_no            = data["ALLOC_NO"]
            mode                = data["MODE"]       
            #user                = data["USER"]
            mycursor            = connection.cursor()
            
            I_query       = "UPDATE active_alloc_sessions SET CREATE_TIME = current_timestamp WHERE ALLOC_NO = %s and SESSION_MODE = %s; "
            DL_query      = '''DELETE FROM active_alloc_sessions
                                WHERE CREATE_TIME < NOW() - INTERVAL 15 MINUTE ;'''
            mycursor.execute(DL_query)
            print("DELETED INACTIVE LOCKED SESSIONS : ",mycursor.rowcount)
            mycursor.execute(I_query,(alloc_no,mode,))
            print(alloc_no,": UPDATE CURRENT LOCKED SESSION : ",mycursor.rowcount)
            if mycursor.rowcount == 0 :
                
                return JsonResponse({"status": 500, "message": "UPDATE SESSION FAILED"})
            
            return JsonResponse({"status": 200, "message": "SESSION TIME UPDATED"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        finally:
            connection.commit()
        

@csrf_exempt 
def Report(request):
    if request.method == 'GET':
        try:
            query="""SELECT 
                            item_id AS ITEM_ID, 
                            SUM(allocated_qty) AS TOTAL_ALLOCATED_QTY
                       FROM 
                            alloc_xref
                      WHERE 
                            release_date BETWEEN CURDATE() - INTERVAL 7 DAY AND CURDATE()
                      GROUP BY item_id;
                        """
            query1 ="SELECT * FROM alloc_xref;"
            res_df=(pd.read_sql(query1,connection)).to_dict("records")
            
            return JsonResponse(res_df, content_type="application/json",safe=False) 
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})

@csrf_exempt
def update_alloc_status(request):
    if request.method == 'POST':
        try:
            json_object   = json.loads(request.body)
            data          = json_object[0]            
            alloc_no      = data["ALLOC_NO"]
            status        = data["STATUS"]
            mycursor      = connection.cursor()            
            I_query       = "UPDATE alloc_head SET status = %s WHERE ALLOC_NO = %s; "           
            mycursor.execute(I_query,(status,alloc_no,))
            
            if mycursor.rowcount == 0 :
                return JsonResponse({"status": 500, "message": "ALLOC STATUS NOT UPDATED"})
            
            return JsonResponse({"status": 200, "message": "Allocation Submitted Successfully" if status =="SUB" 
                                 else str(alloc_no)+": Allocation Status is changed Successful."})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        finally:
            connection.commit()        