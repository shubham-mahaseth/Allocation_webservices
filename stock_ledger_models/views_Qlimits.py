import json
import csv
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
import numpy as np
from django.db import connection
from .Allocation_functions.Allocation.QUANTITY_LIMITS.qty_limits_ok_button_wrapper import ins_qty_limits
from .Allocation_functions.Allocation.QUANTITY_LIMITS.qty_limits_screen_wrapper import rtv_qty_limits
from .Allocation_functions.Allocation.GLOBAL_FILES.get_connection import get_mysql_conn
#from .QUANTITY_LIMITS.qty_limits import P360_RETREIVE_QUANTITY_LIMITS
from .connect import get_mysql_conn
#from .Testing import get_mysql_connection
import threading

#CHANGES FOR CLOUD FOR CONNECTION START
# conn_global = threading.local()

# def establish_connection():
#     if not hasattr(conn_global, 'connection'):
#         try:
#             I_db_connect_status = list()
#             I_db_connect_status.append(0)
#             conn_generator = get_mysql_conn(I_db_connect_status)
#             print('DEBUG CONN: conn_generator :',conn_generator)
#             conn_global.connection = conn_generator.__enter__()
#         except Exception as e:
#             print(f"Error establishing connection: {e}")
#             conn_global.connection = None
#     elif conn_global.connection is None:
#         try:
#             I_db_connect_status = list()
#             I_db_connect_status.append(0)
#             conn_generator = get_mysql_conn(I_db_connect_status)
#             print('DEBUG CONN: conn_generator :',conn_generator)
#             conn_global.connection = conn_generator.__enter__()
#         except Exception as e:
#             print(f"Error re-establishing connection: {e}")
#             conn_global.connection = None

#     if conn_global.connection is None:
#         raise Exception("MySQL Connection not available.")
    
#     return conn_global.connection

# def close_connection():
#     if hasattr(conn_global, 'connection') and conn_global.connection is not None:
#         try:
#             conn_global.connection.close()
#         except Exception as e:
#             print(f"Error closing connection: {e}")
#         finally:
#             conn_global.connection = None

            
#CHANGES FOR CLOUD FOR CONNECTION END
        
conn_global1 =None
def establish_connection():
    global conn_global1
    #if conn_global is None:
    #    conn_generator =get_mysql_conn([])
    #    conn_global =  conn_generator.__enter__()
    I_db_connect_status = list()
    I_db_connect_status.append(0)
    conn_generator =get_mysql_conn(I_db_connect_status)
    conn_global1 = conn_generator.__enter__()
 
# Singleton pattern for the connection
# class ConnectionSingleton:
#     _instance = None

#     @classmethod
#     def get_connection(cls):
#         if cls._instance is None:
#             I_db_connect_status = [0]
#             conn_generator = get_mysql_conn(I_db_connect_status)
#             cls._instance = conn_generator.__enter__()
#         return cls._instance

#     @classmethod
#     def close_connection(cls):
#         if cls._instance is not None:
#             cls._instance.close()
#             cls._instance = None


@csrf_exempt
def Alloc_qty_limits_retrive_table_function(alloc_no):
        try:
            establish_connection()
            #conn_global1 = ConnectionSingleton.get_connection()
            print("DEBUG CONN - QTY LIMITS RETRIEVE : ",conn_global1)
            results55,err_msg1 = rtv_qty_limits(conn_global1,alloc_no,"NEW")
            res_list=[]
            rec={}
            if len(results55)>0:
                results55 =  results55.replace(np.NaN, "NULL", regex=True)
                for val2 in results55.values:
                    count=0
                    for col4 in results55.columns:
                        rec[col4]=val2[count]
                        count=count+1
                    for col in rec:
                        if rec[col]==None or rec[col]=="NULL":
                            rec[col]=""
                    res_list.append(rec.copy())
                return res_list,''
            else:
                return res_list,err_msg1
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})


#FETCHING ALL THE COLUMN VALUES FROM HIER TABLE:
@csrf_exempt
def Alloc_qty_limits_retrive_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)            
            json_object=json_object[0]
            
            if "ALLOC_NO" in json_object:
                results55=rtv_qty_limits(connection,json_object["ALLOC_NO"],"NEW")
                
                res_list=[]
                rec={}
                if len(results55)>0:
                    results55 =  results55.replace(np.NaN, "NULL", regex=True)
                    for val2 in results55.values:
                        count=0
                        for col4 in results55.columns:
                            rec[col4]=val2[count]
                            count=count+1
                        for col in rec:
                            if rec[col]==None or rec[col]=="NULL":
                                rec[col]=""
                        res_list.append(rec.copy())
                    if len(res_list)==0:
                        return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
                    else:
                        #result_json = json.dumps(res_list)
                        return JsonResponse(res_list,content_type="application/json",safe=False)
                else:
                    return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})


#QUANTITY LIMITS ON CLICK OK INSERT 
@csrf_exempt
def alloc_qty_Limits_Insert_table(request):
    if request.method == 'POST':
        try:
            data=json.loads(request.body)
            if len(data) > 0 :
                #conn_global1 = ConnectionSingleton.get_connection()
                #conn_global1 = establish_connection()
                print("DEBUG CONN - QTY LIMITS INSERT OK : ",conn_global1)
                if conn_global1 is None:
                    return JsonResponse({"status": 500, "message": "Connection lost"})
                mycursor = conn_global1.cursor()
                create_query="""CREATE TEMPORARY TABLE IF NOT EXISTS alloc_quantity_limits_temp2 (ALLOC_NO                 NUMERIC(15),    
                                                                                                  LOCATION_ID              VARCHAR(40),  
                                                                                                  LOCATION_DESC            VARCHAR(150), 
                                                                                                  LOCATION_GROUP_ID        VARCHAR(40),  
                                                                                                  LOCATION_GROUP_DESC      VARCHAR(150), 
                                                                                                  LOCATION_GROUP_TYPE      VARCHAR(1),   
                                                                                                  HIER1                    VARCHAR(40),  
                                                                                                  HIER2                    VARCHAR(40),  
                                                                                                  HIER3                    VARCHAR(40),  
                                                                                                  ITEM_ID                  VARCHAR(60),  
                                                                                                  ITEM_DESC                VARCHAR(250), 
                                                                                                  DIFF_ID                  VARCHAR(60),  
                                                                                                  SIZE_COUNT               NUMERIC(12),   
                                                                                                  MIN                      NUMERIC(12),  
                                                                                                  MAX                      NUMERIC(12),  
                                                                                                  TRESHOLD                 NUMERIC(12),  
                                                                                                  TREND                    NUMERIC(12),  
                                                                                                  WOS                      NUMERIC(12),  
                                                                                                  MIN_NEED                 NUMERIC(12),   
                                                                                                  SOM_QTY                  NUMERIC(12),
                                                                                                  PACK_IND                 VARCHAR(1),   
                                                                                                  ASSIGN_DEFAULT_WH        NUMERIC(10),
                                                                                                  PRIMARY KEY(ALLOC_NO,LOCATION_ID,ITEM_ID,DIFF_ID));"""
                mycursor.execute(create_query)
                delete_query="delete from alloc_quantity_limits_temp2"
                mycursor.execute(delete_query)
                #D_keys=[]
                # for row in data:
                #     for col in row:
                #         if row[col]=="NULL" or row[col]==None or row[col]=="" or col=="SR_NO":
                #             D_keys.append(col)
                #     for key in D_keys:
                #         row.pop(key)
                #     D_keys.clear()
                data = [{k: v for k, v in row.items() if v not in [None, "", "NULL"] and k != "SR_NO"} for row in data]
                
                for row in data:
                    cols=",".join(map(str, row.keys()))
                    v_list=[]
                    val=') VALUES('
                    for v in row.values():
                        if v== None:
                            val=val+'NULL,'
                        else:
                            v_list.append(v)
                            val=val+'%s,'
                    val=val[:-1]+')'
                    query="insert into alloc_quantity_limits_temp2(" +cols + val
                    mycursor.execute(query,v_list)
                    conn_global1.commit()
                # for row in data:
                #     cols = ",".join(map(str, row.keys()))
                #     val = ','.join(['%s'] * len(row))
                #     query = f"INSERT INTO alloc_quantity_limits_temp2 (" + cols + ") VALUES (" + val + ")"
                #     mycursor.execute(query, list(row.values()))
                #     conn_global1.commit()

                for row in data:
                    if "ALLOC_NO" in row:
                        result,err_msg = ins_qty_limits(conn_global1,row["ALLOC_NO"])
                        if result:
                            return JsonResponse({"status": 201, "message": "Setup complete"})
                        else: 
                            if len(err_msg) > 0:
                                return JsonResponse({"status": 500, "message":str(err_msg)})
                            return JsonResponse({"status": 500, "message": "Setup not complete"})
            return JsonResponse({"status": 500, "message": "NOT INSERTED"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        finally:
            if conn_global1 is not None:
                conn_global1.close()
            #close_connection()

@csrf_exempt
def qty_Limits_Rtv(request):
    if request.method == 'POST':
        try:
            data=json.loads(request.body)
            data=data[0]
            if "ALLOC_NO" in data and  "I_MODE" in data:
                result=rtv_qty_limits(data["ALLOC_NO"],data["I_MODE"])
                if result:
                    return JsonResponse({"status": 201, "message": "SUCCESSFULL"})
                else: 
                    return JsonResponse({"status": 500, "message": "ERROR"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})



#Fetching all the column values from PO table:
@csrf_exempt
def update_Alloc_quantity_limits_grid_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
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
                #for key in row:
                query="update alloc_quantity_limits_temp set {}".format(' '.join('{} = "{}" ,'.format(k,str(row[k])) for k in row))+" where ITEM='{}'".format(row["ITEM"])+ " and ALLOC_NO='{}'".format(row["ALLOC_NO"])+ " and LOCATION='{}'".format(row["LOCATION"])+";"
                mycursor.execute(query)
            connection.commit()
            return JsonResponse({"status": 201, "message": "Data Updated"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})