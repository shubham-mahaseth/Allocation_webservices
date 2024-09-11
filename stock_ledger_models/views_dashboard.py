import pandas as pd
import json
import numpy as np
from django.http import JsonResponse,HttpResponse,StreamingHttpResponse
from django.core import serializers
from datetime import datetime,date
from django.views.decorators.csrf import csrf_exempt
from django.db import connection


#Fetching all the column values from HIER table:
@csrf_exempt
def Alloc_Dashboard_UserAlloc_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object=json_object[0]  
            query = """select CREATE_ID,count(ALLOC_NO) ALLOC_NO from alloc_head 
                                        where CREATE_ID in 
                                                (select distinct CREATE_ID from alloc_head 
                                                                where CREATE_ID is not null)
                                            group by create_id order by ALLOC_NO desc;"""

            result = pd.read_sql(query,connection)
            res_list = []
            res_list = df_conversion(result)
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
def Alloc_Dashboard_Release_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object = json_object[0]  
            # query = """select distinct 
            #                         RELEASE_DATE,
            #                         COUNT(CASE WHEN status = 'WS' THEN 1 END) AS Worksheet,
            #                         COUNT(CASE WHEN status = 'RSV' THEN 1 END) AS Reserved,
            #                         COUNT(CASE WHEN status = 'APV' THEN 1 END) AS Approved                                    
            #                     from alloc_head 
            #                                 where status in ('WS','APV','RSV') 
            #                                 and release_date BETWEEN curdate() 
            #                                 AND DATE_ADD(CURDATE(), INTERVAL %s DAY)
            #                     group by RELEASE_DATE
            #                     order by RELEASE_DATE;"""
            
            # query = '''SELECT DISTINCT 
            #                 RELEASE_DATE,
            #                 COUNT(CASE WHEN status = 'WS' THEN 1 END) AS Worksheet,
            #                 COUNT(CASE WHEN status = 'RSV' THEN 1 END) AS Reserved,
            #                 COUNT(CASE WHEN status = 'APV' THEN 1 END) AS Approved                                    
            #             FROM alloc_head 
            #             WHERE status IN ('WS', 'APV', 'RSV') 
            #                 AND release_date BETWEEN 
            #                     DATE_SUB(CURDATE(), INTERVAL (DAYOFWEEK(CURDATE()) - 2) DAY) 
            #                     AND DATE_ADD(DATE_SUB(CURDATE(), INTERVAL (DAYOFWEEK(CURDATE()) - 2) DAY), INTERVAL 6 DAY)
            #             GROUP BY RELEASE_DATE
            #             ORDER BY RELEASE_DATE;'''
            query = '''WITH week_dates AS (
                            SELECT 
                                DATE_SUB(CURDATE(), INTERVAL (DAYOFWEEK(CURDATE()) - 2) DAY) + INTERVAL i DAY AS release_date
                            FROM 
                                (SELECT 0 AS i UNION ALL
                                 SELECT 1 UNION ALL
                                 SELECT 2 UNION ALL
                                 SELECT 3 UNION ALL
                                 SELECT 4 UNION ALL
                                 SELECT 5 UNION ALL
                                 SELECT 6) AS days
                        )

                        SELECT 
                            wd.RELEASE_DATE AS RELEASE_DATE,
                            COUNT(CASE WHEN ah.status = 'WS' THEN 1 END) AS Worksheet,
                            COUNT(CASE WHEN ah.status = 'RSV' THEN 1 END) AS Reserved,
                            COUNT(CASE WHEN ah.status = 'APV' THEN 1 END) AS Approved
                        FROM 
                            week_dates wd
                        LEFT JOIN 
                            alloc_head ah 
                            ON wd.release_date = ah.release_date 
                            AND ah.status IN ('WS', 'APV', 'RSV')
                        GROUP BY 
                            wd.release_date
                        ORDER BY 
                            wd.release_date;
                    '''


            # result = pd.read_sql(query,connection,params=(json_object["DAYS"],))
            
            result = pd.read_sql(query,connection)
            res_list = []
            rec={}
            if len(result)>0:
                result =  result.replace(np.NaN, "NULL", regex=True)
                for val2 in result.values:
                    count=0
                    for col4 in result.columns:
                        rec[col4]=val2[count]
                        count=count+1
                    for col in rec:
                        if rec[col]==None or rec[col]=="NULL":
                            rec[col]=""
                        if col == "RELEASE_DATE" and len(str(rec["RELEASE_DATE"])) > 0:
                            date_object = datetime.strptime(str(rec["RELEASE_DATE"]), '%Y-%m-%d')
                            rec["RELEASE_DATE"] = date_object.strftime('%m-%d-%y')
                    res_list.append(rec.copy()) 
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


@csrf_exempt
def Alloc_Dashboard_AllocCount_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object = json_object[0]  
            # query = """SELECT
            #                   ALLOC_CRITERIA,
            #                   COUNT(CASE WHEN status = 'WS' THEN 1 END) AS Worksheet,
            #                   COUNT(CASE WHEN status = 'RSV' THEN 1 END) AS Reserved,
            #                   COUNT(CASE WHEN status = 'APV' THEN 1 END) AS Approved                              
            #                 FROM
            #                      alloc_head
            #                 where  status in ('WS','APV','RSV') 
            #                         and release_date BETWEEN curdate() 
            #                         AND DATE_ADD(CURDATE(), INTERVAL %s DAY) 
            #                 GROUP BY
            #                   Alloc_criteria;"""     
            query = """SELECT
                            criteria.ALLOC_CRITERIA,
                            COUNT(CASE WHEN ah.status = 'WS' THEN 1 END) AS Worksheet,
                            COUNT(CASE WHEN ah.status = 'RSV' THEN 1 END) AS Reserved,
                            COUNT(CASE WHEN ah.status = 'APV' THEN 1 END) AS Approved                              
                        FROM
                            (SELECT 'T' AS ALLOC_CRITERIA
                             UNION ALL
                             SELECT 'W'
                             UNION ALL
                             SELECT 'P'
                             UNION ALL
                             SELECT 'A'
                             UNION ALL
                             SELECT 'F') AS criteria
                        LEFT JOIN
                            alloc_head ah
                            ON criteria.ALLOC_CRITERIA = ah.ALLOC_CRITERIA
                            AND ah.status IN ('WS', 'APV', 'RSV') 
                            AND ah.release_date BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL %s DAY)
                        GROUP BY
                            criteria.ALLOC_CRITERIA;"""
            # print(query%(json_object["DAYS"],))
            result = pd.read_sql(query,connection,params=(json_object["DAYS"],))
            res_list = []
            rec={}
            if len(result)>0:
                result =  result.replace(np.NaN, "NULL", regex=True)
                for val2 in result.values:
                    count=0
                    for col4 in result.columns:
                        rec[col4]=val2[count]
                        count=count+1
                    for col in rec:
                        if rec[col]==None or rec[col]=="NULL":
                            rec[col]=""
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
                    res_list.append(rec.copy()) 
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message": "NO DATA FOUND"})
            else:
                return JsonResponse(res_list, content_type="application/json",safe=False)
        except Exception as error:
            print("DASHBOARD OVER PERIOD EXCEPTION: ",str(error))
            return JsonResponse({"status": 500, "message": str(error)})
        finally:
            connection.close()


            
def Dashboard_Allocations(request):
    if request.method == 'GET':
        try:  
            query = """SELECT
                          COUNT(CASE WHEN status IN ('APV', 'RSV') THEN 1 END) AS ACTIVE_ALLOC,
                          COUNT(CASE WHEN status = 'SCHD' THEN 1 END) AS SCHEDULE,
                          COUNT(CASE WHEN status = 'POC' THEN 1 END) AS PO_CREATE
                        FROM alloc_head
                        WHERE RELEASE_DATE >= CURDATE();"""                                    

            result = pd.read_sql(query,connection)
            res_list = result.to_dict("records")
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

