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




'''
        ********************************
               ALLOCATION HEADER
        ********************************
    
'''

#Fetching all the column values from Alloc Type table:
def Alloc_Type_table(request):
    if request.method == 'GET':
        try:
            query="select cd.CODE,cd.CODE_DESC from code_detail cd where CODE_TYPE='ALCT' ORDER BY cast(cd.CODE_DESC as unsigned);"
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                P_keys=[]
                for col in results.columns:
                    rec[col]=val1[count]
                    count=count+1
                for col in results.columns:
                    if col=="CODE_DESC":
                        P_keys.append(col)
                for key in P_keys:
                    rec["ALLOC_TYPE"]=rec.pop(key)
                for col in rec:
                    if rec["ALLOC_TYPE"] == "Ad-Hoc":
                        rec["ALLOC_TYPE"] = "Manual"
                P_keys.clear()
                res_list.append(rec)
                
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})



#Fetching CODE AND CODE_DESC values from code_detail WHERE CODE_TYPE_DESC IS  Alloc Level table:
def Alloc_Level_table(request):
    if request.method == 'GET':
        try:
            query="select cd.CODE,cd.CODE_DESC from code_detail cd where CODE_TYPE='ALLV';"
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                P_keys=[]
                for col in results.columns:
                    rec[col]=val1[count]
                    count=count+1
                for col in results.columns:
                    if col=="CODE_DESC":
                        P_keys.append(col)
                for key in P_keys:
                    rec["ALLOC_LEVEL"]=rec.pop(key)
                for col in rec:
                    if rec["ALLOC_LEVEL"] == "Style Diff":
                        rec["ALLOC_LEVEL"] = "Style Variant"
                P_keys.clear()
                res_list.append(rec)
                
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})



#FETCHING CODE AND CODE_DESC VALUES FROM CODE_DETAIL  WHOSE CODE_TYPE_DESC IS  "CONTEXT TYPE":
def Context_type_table(request):
    if request.method == 'GET':
        try:
            query="select cd.CODE,cd.CODE_DESC from code_detail cd where CODE_TYPE='CNTX' ORDER BY cd.CODE_DESC;"
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                P_keys=[]
                for col in results.columns:
                    rec[col]=val1[count]
                    count=count+1
                for col in results.columns:
                    if col=="CODE_DESC":
                        P_keys.append(col)
                for key in P_keys:
                    rec["CONTEXT"]=rec.pop(key)
                P_keys.clear()
                res_list.append(rec)
                
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})



#FETCHING CODE AND CODE_DESC VALUES FROM CODE_DETAIL  WHOSE CODE_TYPE_DESC IS  "STATUS" :

def Status_table(request):
    if request.method == 'GET':
        try:
            query="select cd.CODE,cd.CODE_DESC from code_detail cd where CODE_TYPE='ALTS' ORDER BY cd.CODE desc;"
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                P_keys=[]
                for col in results.columns:
                    rec[col]=val1[count]
                    count=count+1
                for col in results.columns:
                    if col=="CODE_DESC":
                        P_keys.append(col)
                for key in P_keys:
                    rec["STATUS"]=rec.pop(key)
                P_keys.clear()
                res_list.append(rec)
                
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})



#FETCHING ALL THE COLUMN VALUES FROM PROMOTION TABLE:
def Promotion_table(request):
    if request.method == 'GET':
        try:
            mycursor=connection.cursor();            
            query="select sp.PROMO_ID,sp.DESCRIPTION,sp.START_DATE,sp.END_DATE from store_promo sp ORDER BY cast(sp.PROMO_ID as unsigned);"
            mycursor.execute("desc store_promo")
            d_type=mycursor.fetchall()
            list_type=[]
            for col in d_type:
                if "decimal" in col[1]:
                    if "PROMO_ID" in col[0]:
                        list_type.append(col[0])
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                P_keys=[]
                for col in results.columns:
                    rec[col]=val1[count]
                    count=count+1
                #converting LOCATION ,REV_NO AND ERR_SEQ_NO to INTEGER  if DECIMAL
                for col in list_type:
                    if col in rec:
                        rec[col]=int(rec[col])
                for col in results.columns:
                    if col=="PROMO_ID":
                        P_keys.append(col)
                for key in P_keys:
                    rec["PROMOTION"]=rec.pop(key)
                P_keys.clear()
                res_list.append(rec)
                
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})



#FETCHING ALL THE COLUMN VALUES FROM STATUS TABLE:
def Criteria_table(request):
    if request.method == 'GET':
        try:
            query="select cd.CODE,cd.CODE_DESC from code_detail cd where CODE_TYPE='ASRC';"
            #print("con:",connection)
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                P_keys=[]
                for col in results.columns:
                    rec[col]=val1[count]
                    count=count+1
                for col in results.columns:
                    if col=="CODE_DESC":
                        P_keys.append(col)
                for key in P_keys:
                    rec["ALLOC_CRITERIA"]=rec.pop(key)
                P_keys.clear()
                res_list.append(rec)
                
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})



#GENERATING UNIQUE ALLOCATION NUMBER
def Alloc_No_table(request):
    if request.method == 'GET':
        try:
            mycursor=connection.cursor()
            #Q_sel="""select * from alloc_seq order by alloc_no desc limit 1;""" 
            Q_sel = '''SELECT a.ALLOC_NO, cv.SYSTEM_DATE
                        FROM (
                          SELECT *
                          FROM alloc_seq
                          ORDER BY ALLOC_NO DESC
                          LIMIT 1
                        ) a
                        CROSS JOIN (
                          SELECT SYSTEM_DATE
                          FROM calendar_variables
                          LIMIT 1
                        ) cv;'''

            Q_insert="""update alloc_seq set ALLOC_NO= (%s) """
            Alloc_no=[]
            alloc_Data=pd.read_sql(Q_sel,connection)
            if len(alloc_Data)>0:
                alloc_Data= (alloc_Data.to_dict("records"))[0]
            if "ALLOC_NO" in alloc_Data:
                Alloc_no.append(int(alloc_Data["ALLOC_NO"])+1)
            #rec={}
            #for val1 in results.values:
            #    count=0
            #    for col in results.columns:
            #        rec[col]=val1[count]
            #        count=count+1
            #Alloc_list={}
            #Alloc_no=[]
            #for k in rec:
            #    if k=="ALLOC_NO":
            #        Alloc_list["ALLOC_NO"]=int(rec["ALLOC_NO"])+1
            #        Alloc_no.append(int(rec["ALLOC_NO"])+1)
            if len(Alloc_no)>0:
                mycursor.execute(Q_insert,Alloc_no)
                connection.commit()  
            
            if len(alloc_Data)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(alloc_Data,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})



# BASED ON THE INPUT "ALLOC NO" RETRIEVES HEADER DATA FOR CREATE ALLOCATION SCREEN 
@csrf_exempt
def Alloc_no_screen_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            #print("Alloc_no_screen_table::",json_object)
            json_object=json_object[0]
            keys=[]
            for key in json_object:
                if json_object[key]=="NULL" or json_object[key]=="":
                    json_object[key]=None
                    keys.append(key)
            for k in keys:
                json_object.pop(k)
            val_lis=[]
            for row in json_object:
                val_lis.append(json_object[row])
            ##print(val_lis)
            query="SELECT temp.ALLOC_NO,temp.ALLOC_DESC,temp1.ALLOC_LEVEL,temp1.ALLOC_LEVEL_CODE,temp.RELEASE_DATE,temp.ALLOCATOR,temp.STATUS,temp.STATUS_CODE,temp2.CONTEXT,temp2.PROMOTION,temp3.ALLOC_TYPE,temp2.CONTEXT_CODE,temp3.ALLOC_TYPE_CODE,temp4.ALLOC_CRITERIA,temp.RECALC_IND FROM "
            query=query+"(SELECT ah.ALLOC_NO,cd.CODE_DESC as STATUS,cd.CODE as STATUS_CODE, ah.ALLOC_DESC,ah.RELEASE_DATE,ah.CREATE_ID ALLOCATOR,ah.RECALC_IND FROM code_detail cd,alloc_head ah WHERE CODE_TYPE='ALTS' AND cd.CODE = ah.STATUS AND ALLOC_NO='{}'".format(json_object["ALLOC_NO"])+") temp,"
            query=query+"(SELECT ah.ALLOC_NO,cd.CODE as ALLOC_LEVEL_CODE,cd.CODE_DESC AS ALLOC_LEVEL FROM code_detail cd, alloc_head ah WHERE CODE_TYPE='ALLV'AND cd.CODE = ah.ALLOC_LEVEL AND ALLOC_NO='{}'".format(json_object["ALLOC_NO"])+") temp1,"
            query=query+"(SELECT ah.ALLOC_NO,cd.CODE as ALLOC_TYPE_CODE,cd.CODE_DESC AS ALLOC_TYPE FROM code_detail cd, alloc_head ah WHERE CODE_TYPE='ALCT'AND cd.CODE = ah.ALLOC_TYPE AND ALLOC_NO='{}'".format(json_object["ALLOC_NO"])+") temp3,"
            query=query+"""(SELECT ah.ALLOC_NO,cd.CODE as CONTEXT_CODE,  cd.CODE_DESC AS CONTEXT,ah.PROMOTION FROM code_detail cd,alloc_head ah WHERE CODE_TYPE='CNTX'AND cd.CODE = ah.CONTEXT AND ALLOC_NO='{}'""".format(json_object["ALLOC_NO"])+") temp2,"
            query=query+"""(SELECT ah.ALLOC_NO,cd.CODE as ALLOC_CRITERIA_CODE,cd.CODE AS ALLOC_CRITERIA FROM code_detail cd,alloc_head ah WHERE CODE_TYPE='ASRC'AND cd.CODE = ah.ALLOC_CRITERIA AND ALLOC_NO='{}'""".format(json_object["ALLOC_NO"])+""") temp4 
                        WHERE temp1.ALLOC_NO = temp.ALLOC_NO AND temp1.ALLOC_NO = temp2.ALLOC_NO and temp3.ALLOC_NO = temp.ALLOC_NO and temp3.ALLOC_NO = temp1.ALLOC_NO and temp3.ALLOC_NO = temp2.ALLOC_NO and temp.ALLOC_NO = temp4.ALLOC_NO;"""
            #print("query",query)
            results55=pd.read_sql(query,connection) 
            result1 = pd.read_sql("select * from alloc_calc_item_loc where alloc_no = {}".format(json_object["ALLOC_NO"]),connection)
            ##print(query)
            res_list=[]
            rec={}
            results55 =  results55.replace(np.NaN, "", regex=True)
            for val2 in results55.values:
                count=0
                for col4 in results55.columns:
                    rec[col4]=val2[count]
                    count=count+1
                for col in rec:
                    if rec["ALLOC_LEVEL"] == "Style Diff":
                        rec["ALLOC_LEVEL"] = "Style Variant"
                    if rec["ALLOC_TYPE"] == "Ad-Hoc":
                        rec["ALLOC_TYPE"] = "Manual"
                    if col == "RELEASE_DATE" and len(str(rec["RELEASE_DATE"])) > 0:
                        date_object = datetime.strptime(str(rec["RELEASE_DATE"]), '%Y-%m-%d')
                        rec["RELEASE_DATE"] = date_object.strftime('%m-%d-%y')
                res_list.append(rec.copy())   
            if len(result1) > 0:
                for row in res_list:
                    row["CALC_ITEM_COUNT"] = 1
            else:
                for row in res_list:
                    row["CALC_ITEM_COUNT"] = 0  
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})


# RETRIEVE AND INSERT FOR THE COLLAB COMMENTS

@csrf_exempt
def Retrieve_Comment_Data(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object = json_object[0]
            print("json_object: ",json_object)
            query="select * from alloc_comment where alloc_no ="+format(json_object["ALLOC_NO"])+";"
            print("query:",query)
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                for col in results.columns:
                    if col =='CREATE_TIME':
                        col = 'TIME'
                    rec[col]=val1[count]
                    count=count+1
                 
                res_list.append(rec)
                
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})


@csrf_exempt
def Insert_Comment_Data(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            
            mycursor = connection.cursor();  
            for row in json_object:
                row["CREATE_TIME"] = datetime.now()
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
                query="insert into alloc_comment(" +cols + val
                mycursor.execute(query,v_list)
                connection.commit()

            print("json_object: ",json_object)
            return JsonResponse({"status": 201, "message": "Setup complete"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})        