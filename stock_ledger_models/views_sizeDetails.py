import json
import pandas as pd
import numpy as np
from django.db import IntegrityError
from django.http import JsonResponse,HttpResponse,StreamingHttpResponse
from django.core import serializers
from datetime import datetime,date
from django.db import connection
from .connect import get_mysql_conn
from django.views.decorators.csrf import csrf_exempt
from .views_alloc_criteria import df_conversion
from .Allocation_functions.Allocation.SIZE_DETAILS.retreive_alloc_size_details_wrapper import rtv_alloc_size_dtl
from .Allocation_functions.Allocation.SIZE_DETAILS.alloc_size_hdr_wrapperpy import to_call_ls_fun
from .Allocation_functions.Allocation.SIZE_DETAILS.upd_size_profile_wrapper import do_upd_size_profile



conn_global = None
'''
        ********************************
        ********************************
             SAVE DATA & GLOBAL CONN
        ********************************
        ********************************
    
'''

def establish_connection():
    global conn_global
    #if conn_global is None:
    #    conn_generator =get_mysql_conn([])
    #    conn_global =  conn_generator.__enter__()
    I_db_connect_status = list()
    I_db_connect_status.append(0)
    conn_generator =get_mysql_conn(I_db_connect_status)
    conn_global =  conn_generator.__enter__()

def close_connection():
    global conn_global
    if conn_global is not None:
        conn_global.__exit__(None, None, None)
        conn_global = None

@csrf_exempt
def sizeDetails_Commit(request):
    if request.method == 'POST':
        try:
            req_type = json.loads(request.body)
            req_type = req_type[0]
            print("SIZE DETAILS CONNECTION : ",req_type)
            if req_type == "CLOSE":
                conn_global.close()
            else:
                if conn_global != None:
                    conn_global.commit()
                    conn_global.close()
                    close_connection()
                else:
                    connection.commit()
            return JsonResponse({"status": 200, "message":" COMMIT SUCCESS"})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        finally:
            connection.close()

@csrf_exempt
def size_details_Header_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data =data[0]
            if data["COUNT"] == 1:
                establish_connection()
            for col in data:
                if data[col] == "NULL" or data[col] == None or data[col] == "":
                    data[col] = None
            if "SOURCE_ITEM" in data:
                result = to_call_ls_fun(conn_global,data["ALLOC_NO"],data["WH_ID"],data["PO_NO"],data["SOURCE_ITEM"],data["DIFF_ID"])
                if len(result[0])> 0:
                    res_list = df_conversion(result[0])
                    return JsonResponse({"status": 200, "message": res_list})
                else:
                    return JsonResponse({"status": 500, "message": str(result[1])})
                #return JsonResponse(res_list, content_type="application/json",safe=False)
            else:
                result= to_call_ls_fun(conn_global,data["ALLOC_NO"],None,None,None,None)
                if len(result[0])> 0:
                    res_list = df_conversion(result[0])
                    return JsonResponse({"status": 200, "message": res_list})
                else:
                    return JsonResponse({"status": 500, "message": str(result[1])})
                #return JsonResponse(res_list, content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})

@csrf_exempt
def size_details_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data =data[0]
            for col in data:
                if data[col] == "" or data[col] == None:
                    data[col] = 'NULL'
            #(connection,1409,1,'NULL','021391412','WHITE','NULL')
            if len(data["ORDER_NO"]) > 0:
                result,result1,err_msg  = rtv_alloc_size_dtl(conn_global,data["ALLOC_NO"],data["WH_ID"],data["ORDER_NO"],data["SOURCE_ITEM"],data["DIFF_ID"],'NULL')
            else:
                result,result1,err_msg  = rtv_alloc_size_dtl(conn_global,data["ALLOC_NO"],data["WH_ID"],'NULL',data["SOURCE_ITEM"],data["DIFF_ID"],'NULL')
            res_list=[]
            rec={}
            if len(result)>0:
                result =  result.replace(np.NaN, "NULL", regex=True)
                for val2 in result.values:
                    count=0
                    P_keys= []
                    for col4 in result.columns:
                        rec[col4.upper()]=val2[count]
                        count=count+1
                    for col in rec:
                        if rec[col]==None or rec[col]=="NULL":
                            rec[col]=""
                    res_list.append(rec.copy()) 

            if len(result1) > 0:
                result1 = result1.to_dict("records")
            if len(res_list)>0:
                return JsonResponse({"status": 200, "message": [res_list,result1]})
                #return JsonResponse([res_list,result1], content_type="application/json",safe=False)      
            else:
                return JsonResponse({"status": 500, "message": err_msg})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})

@csrf_exempt
def size_details_Update_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data = data[0]
            for col in data:
                if data[col]==None or data[col]=="NULL" or data[col]=="NaN" or data[col]=="":
                    data[col] = None

            alloc_head_detail = pd.read_sql("select * from alloc_head where alloc_no={}".format(data["ALLOC_NO"]),conn_global)
            if len(alloc_head_detail) > 0:
                alloc_head_detail = alloc_head_detail.to_dict("records")
            else:
                alloc_head_detail = []

            result= do_upd_size_profile(conn_global,data["ALLOC_NO"],data["SOURCE_ITEM"],data["DIFF_ID"],data["ORDER_NO"],data["WH_ID"],data["TO_LOC"],data["TRAN_ITEM"],data["CALC_QTY"])

            if result[-1] == False:
                return JsonResponse({"status": 500, "message": [result[0],[data]]})
            elif result[-1] == True:
                if len(str(result[0])) == 0 and len(str(result[1])) == 0:
                    return JsonResponse({"status": 500, "message": ["Data is not updated.",[data]]})
                else:
                    alloc_head_detail = alloc_head_detail[0]
                    if (alloc_head_detail["ALLOC_CRITERIA"] == 'F'):
                        return JsonResponse({"status": 200, "message": [data]})
                    else:
                        if float(result[0]) > float(result[1]):
                            return JsonResponse({"status": 500, "message": ["Allocated units is greater than the remaining units.",[data]]})
                        else:
                            data["UPDATE_CALC_QTY"]         = result[0]
                            data["UPDATE_TOTAL_CALC_QTY"]   = result[1]
                            data["UPDATE_REMAIN_QTY"]       = result[2]
                            return JsonResponse({"status": 200, "message": [data]})
        except Exception as error:
            return JsonResponse({"status": 500, "message": [str(error),[data]]})
