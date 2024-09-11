import json
import csv
import pandas as pd
from django.db import IntegrityError
#from .models import LOCATION, STG_TRN_DATA,TRN_DATA,PNDG_DLY_ROLLUP,STG_TRN_DATA_DEL_RECORDS,SYSTEM_CONFIG,ERR_TRN_DATA,DAILY_SKU,DAILY_ROLLUP,trn_data_history,trn_data_rev,CURRENCY,ITEM_LOCATION,ITEM_DTL,HIER1,HIER2,HIER3
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
import numpy as np
from django.db import connection
from .Allocation_functions.Allocation.RULES_AND_LOCATIONS.load_change_weight_dates_wrapper import fetch_chng_wt
from .Allocation_functions.Allocation.RULES_AND_LOCATIONS.load_rule_dates_wrapper import fetch_load_rule_dates_wt
from .Allocation_functions.Allocation.RULES_AND_LOCATIONS.retrieve_rule_dates_weight_wrapper import fetch_retrieve_chng_wt
from .Allocation_functions.Allocation.RULES_AND_LOCATIONS.insert_rule_wrapper import insert_rule_data
from .connect import get_mysql_conn
import threading

# conn_global =None


# def establish_connection():
#     global conn_global
#     #if conn_global is None:
#     #    conn_generator =get_mysql_conn([])
#     #    conn_global =  conn_generator.__enter__()
#     conn_generator =get_mysql_conn([])
#     conn_global =  conn_generator.__enter__()


#CHANGES FOR CLOUD FOR CONNECTION START
conn_global = threading.local()


def establish_connection():
    if not hasattr(conn_global, 'connection'):
        try:
            I_db_connect_status = list()
            I_db_connect_status.append(0)
            conn_generator = get_mysql_conn(I_db_connect_status)
            print('DEBUG CONN: conn_generator :',conn_generator)
            conn_global.connection = conn_generator.__enter__()
        except Exception as e:
            print(f"Error establishing connection: {e}")
            conn_global.connection = None
    elif conn_global.connection is None:
        try:
            I_db_connect_status = list()
            I_db_connect_status.append(0)
            conn_generator = get_mysql_conn(I_db_connect_status)
            print('DEBUG CONN: conn_generator :',conn_generator)
            conn_global.connection = conn_generator.__enter__()
        except Exception as e:
            print(f"Error re-establishing connection: {e}")
            conn_global.connection = None

    if conn_global.connection is None:
        raise Exception("MySQL Connection not available.")
    
    return conn_global.connection

def close_connection():
    if hasattr(conn_global, 'connection') and conn_global.connection is not None:
        try:
            conn_global.connection.close()
        except Exception as e:
            print(f"Error closing connection: {e}")
        finally:
            conn_global.connection = None
            
#CHANGES FOR CLOUD FOR CONNECTION END

@csrf_exempt 
def alloc_rule_Data(request):
    if request.method == 'POST':
        try:
            json_object=json.loads(request.body)
            json_object=json_object[0]

            #for row in json_object:
            for col in json_object:
                if col=="RULE_LEVEL" and len(json_object["RULE_LEVEL"])>0:
                    RULE_LEVEL=json_object.get("RULE_LEVEL")
                    query2="select CODE from code_detail where CODE_TYPE='ALRL' and CODE_DESC='{}';".format(RULE_LEVEL)
                    mydata1=pd.read_sql(query2,connection)
                    l_dict1={}
                    for val in mydata1.values:
                        count=0
                        for col in mydata1.columns:
                            l_dict1[col]=val[count]
                            count=count+1
                    json_object["RULE_LEVEL"]=l_dict1["CODE"]

                if col=="RULE_TYPE" and len(json_object["RULE_TYPE"])>0:
                    RULE_TYPE=json_object.get("RULE_TYPE")
                    query2="select CODE from code_detail where CODE_TYPE='ALCR' and CODE_DESC='{}';".format(RULE_TYPE)
                    mydata1=pd.read_sql(query2,connection)
                    l_dict1={}
                    for val in mydata1.values:
                        count=0
                        for col in mydata1.columns:
                            l_dict1[col]=val[count]
                            count=count+1
                    json_object["RULE_TYPE"]=l_dict1["CODE"]

            for col in json_object:
                if json_object[col]=="NULL" or json_object[col]=="" or json_object[col]==None:
                    json_object[col]=None

            result=insert_rule_data(connection,json_object)
            return JsonResponse({"status": 200, "message": "Data Inserted"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        finally:
             connection.close()



#Fetching all the column values from PO table:
@csrf_exempt
def Alloc_change_weights_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object=json_object[0]
            keys=[]
            mycursor=connection.cursor()
            for key in json_object:
                if json_object[key]=="NULL" or json_object[key]=="":
                    json_object[key]=None
                    keys.append(key)
            for k in keys:
                json_object.pop(k)
            #print(json_object)
            for row in json_object:
                if row=="ALLOC_NO":
                    query="select week_end EOW,WEIGHT from alloc_rule_date where alloc_id='{}'".format(json_object[row])
            #print("query",query)
            results55=pd.read_sql(query,connection) 
            res_list=[]
            rec={}
            results55 =  results55.replace(np.NaN, "NULL", regex=True)
            for val2 in results55.values:
                count=0
                for col4 in results55.columns:
                    rec[col4]=val2[count]
                    count=count+1
                #print("rec::",rec)
                for k in rec:
                    if k=="WEIGHT":
                        if rec[k]=="NULL":
                            rec[k]=100
                res_list.append(rec.copy()) 
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})


#Fetching all the column values from PO table:
@csrf_exempt
def Fetch_Alloc_change_weights_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            #print("json_object::",json_object)
            #json_object=json_object[0]
            keys=[]
            mycursor=connection.cursor()
            for row in json_object:
                for key in row:
                    if row[key]=="NULL" or row[key]=="":
                        row[key]=None
                        keys.append(key)
                for k in keys:
                    row.pop(k)
            for row in json_object:
                for key in row:
                    if key =="EOW_DATE":
                        query="update alloc_quantity_limits_temp set WEIGHT={}".format(int(row["WEIGHT"])/100)+" where EOW_DATE='{}'".format(row["EOW_DATE"])+ " and ALLOC_NO='{}'".format(row["ALLOC_NO"])
                        mycursor.execute(query)
            connection.commit()
            return JsonResponse({"status": 201, "message": "Data Updated"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})




@csrf_exempt
def Alloc_load_change_weights_table(request):
    if request.method == 'POST':
        try:
            establish_connection()
            json_object = json.loads(request.body)
            #print(55555,json_object)

            # mycursor=conn_global.cursor()
            json_object=json_object[0]
            query= "select 1 from alloc_rule where alloc_no ={}".format(json_object["ALLOC_NO"])+";"
            
            #print("tatun",query)
            #mycursor.execute(query)
            #result = mycursor.fetchall()
            #print("tatun",1235)
            if "ALLOC_NO" in json_object:
                result=fetch_chng_wt(connection,json_object["ALLOC_NO"])
            #print("tatun",result)
            if result:
                results55=fetch_retrieve_chng_wt(connection,json_object["ALLOC_NO"])
                res_list=[]
                rec={}
                if len(results55)>0:
                    results55 =  results55.replace(np.NaN, "NULL", regex=True)
                    for val2 in results55.values:
                        count=0
                        for col4 in results55.columns:
                            rec[col4]=val2[count]
                            count=count+1
                        res_list.append(rec.copy()) 
                    if len(res_list)==0:
                        return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
                    else:
                        return JsonResponse(res_list,content_type="application/json",safe=False) 
                return JsonResponse({"status": 201, "message": "NO DATA FOUND"})
            else: 
                return JsonResponse({"status": 500, "message": "ERROR"})       
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})




@csrf_exempt
def Alloc_load_rule_dates_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object=json_object[0]
            if "ALLOC_NO" in json_object:
                result=fetch_load_rule_dates_wt(connection,json_object["ALLOC_NO"])
                print("result: ",result)
            if result:
                return JsonResponse({"status": 201, "message": "SUCCESSFULL"})
            else: 
                return JsonResponse({"status": 500, "message": "ERROR"})       
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})



@csrf_exempt
def Alloc_retrieve_rule_dates_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object=json_object[0]
            if "ALLOC_NO" in json_object:
                results55=fetch_retrieve_chng_wt(connection,json_object["ALLOC_NO"])
                ##print(result,json_object["ALLOC_NO"])
                res_list=[]
                rec={}
                results55 =  results55.replace(np.NaN, "NULL", regex=True)
                for val2 in results55.values:
                    count=0
                    for col4 in results55.columns:
                        rec[col4]=val2[count]
                        count=count+1
                    res_list.append(rec.copy()) 
                if len(res_list)==0:
                    return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
                else:
                    return JsonResponse(res_list,content_type="application/json",safe=False)    
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})


