import json
import csv
import pandas as pd
from django.db import IntegrityError
from django.http import JsonResponse #,HttpResponse,StreamingHttpResponse
from django.core import serializers
from datetime import datetime,date
from django.views.decorators.csrf import csrf_exempt
from django.core.serializers.python import Serializer
from django.db import connection

@csrf_exempt
def fetch_users_data(request):
    if request.method == 'POST':
        try:
            query = """
                    SELECT au.user_id,
                           au.user_name,
                           au.role_id,
                           ar.role_name,
                           'Active' AS status,
                           '09-13-24' AS create_date
                     FROM  alloc_users au
                           LEFT JOIN alloc_roles ar
                                  ON ar.role_id = au.role_id
                    WHERE user_id <> 'admin';
                    """
            result = pd.read_sql(query,connection)
            if len(result) > 0:
                result_list = result.to_dict("records")
                return JsonResponse(result_list, content_type="application/json",safe=False)
            else:
                return JsonResponse({"status": 500, "message": "No Data Found"})  
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        

@csrf_exempt
def fetch_roles_data(request):
    if request.method == 'POST':
        try:
            query = "select * from alloc_roles";
            result = pd.read_sql(query,connection)
            if len(result) > 0:
                result_list = result.to_dict("records")
                return JsonResponse(result_list, content_type="application/json",safe=False)
            else:
                return JsonResponse({"status": 500, "message": "No Data Found"})  
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
 

@csrf_exempt
def fetch_user_role(request):
    print("ROLE :::::::::",request)
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            query = """
                    SELECT au.user_id,
                           au.user_name,
                           au.role_id,
                           ar.role_name
                     FROM  alloc_users au
                           LEFT JOIN alloc_roles ar
                                  ON ar.role_id = au.role_id
                    WHERE  au.user_id = %s;
                    """
            # print(query%(data["USER"],))
            result = pd.read_sql(query,connection,params=(data["USER"],))
            if len(result) > 0:
                result_list = result.to_dict("records")
                return JsonResponse(result_list, content_type="application/json",safe=False)
            else:
                return JsonResponse({"status": 500, "message": "No Data Found"})  
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})