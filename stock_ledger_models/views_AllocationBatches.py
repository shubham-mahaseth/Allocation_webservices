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

from .Allocation_functions.Allocation.SCHEDULE.schedule_processing_wrapper import schedule_wrapper 

@csrf_exempt
def schedule_Batch(request):
    if request.method == 'POST':
        try:
          result, err_msg = schedule_wrapper(connection)
          # result = True
          # err_msg = "SCHEDULE_BATCH EXECUTED SUCCESSFULLY"
          if result :
              return JsonResponse({"status": 201, "message": "Batch completed successfully"})
          elif len(err_msg) >0:
              return JsonResponse({"status": 500, "message": str(err_msg)})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        

@csrf_exempt
def Update_Batch_Date(request):
    if request.method == 'POST':
        try:
            mycursor = connection.cursor()
            query = "update calendar_variables set SYSTEM_DATE = curdate()";
            mycursor.execute(query)
            if mycursor.rowcount == 0:
                return JsonResponse({"status": 500, "message":"Batch failure."})
            else:
                return JsonResponse({"status": 201, "message":"Batch completed successfully."})   
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})