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
from .Allocation_functions.Allocation.RULES_AND_LOCATIONS.insert_locations_wrapper import INS_LOCS
from .Allocation_functions.Allocation.RULES_AND_LOCATIONS.retreive_locations_wrapper import RTV_LOCS
from .Allocation_functions.Allocation.RULES_AND_LOCATIONS.upd_st_wh_wrapper import STORE_WH
from .Allocation_functions.Allocation.RULES_AND_LOCATIONS.del_loc_wrapper import DEL_LOCS
from .Allocation_functions.Allocation.RULES_AND_LOCATIONS.retrieve_rule_data_wrapper import rtv_rule
from .Allocation_functions.Allocation.RULES_AND_LOCATIONS.location_add_button_wrapper import pop_store
from .Allocation_functions.Allocation.RULES_AND_LOCATIONS.upd_size_prof_wrapper import SIZE_PROF
from .Allocation_functions.Allocation.RULES_AND_LOCATIONS.insert_rule_wrapper import insert_rule_data

from .Allocation_functions.Allocation.RULES_AND_LOCATIONS.load_change_weight_dates_wrapper import fetch_chng_wt
from .Allocation_functions.Allocation.RULES_AND_LOCATIONS.load_rule_dates_wrapper import fetch_load_rule_dates_wt
from .Allocation_functions.Allocation.RULES_AND_LOCATIONS.retrieve_rule_dates_weight_wrapper import fetch_retrieve_chng_wt
from .Allocation_functions.Allocation.RULES_AND_LOCATIONS.insert_rule_wrapper import insert_rule_data
from .connect import get_mysql_conn

conn_global =None


#Fetching all the column values from STORE table:
def store_table(request):
    if request.method == 'GET':
        try:
            mycursor=connection.cursor()
            mycursor.execute("desc store")
            d_type=mycursor.fetchall()
            list_type=[]
            for col in d_type:
                if "decimal" in col[1]:
                    if "STORE" in col[0]:
                        list_type.append(col[0])
            
            query='''select DISTINCT s.STORE,s.STORE_DESC from store s            
                                order by STORE;''' #where s.store in (171,172,173,174,175,176,177,178,179,180,181,182,183) 
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                for col in results.columns:
                    rec[col]=val1[count]
                    count=count+1
                #converting LOCATION ,REV_NO AND ERR_SEQ_NO to INTEGER  if DECIMAL
                for col in list_type:
                    if col in rec:
                        rec[col]=int(rec[col])
                res_list.append(rec)
                
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})



#Fetching all the column values from store_list table:
def store_list_table(request):
    if request.method == 'GET':
        try:
            mycursor=connection.cursor()
            mycursor.execute("desc store")
            d_type=mycursor.fetchall()
            list_type=[]
            for col in d_type:
                if "decimal" in col[1]:
                    if "LOC_TRAIT" in col[0]:
                        list_type.append(col[0])
            
            query="select DISTINCT sl.LOC_LIST,sl.LOC_LIST_DESC from store_list sl;"
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                for col in results.columns:
                    rec[col]=val1[count]
                    count=count+1
                #converting LOCATION ,REV_NO AND ERR_SEQ_NO to INTEGER  if DECIMAL
                for col in list_type:
                    if col in rec:
                        rec[col]=int(rec[col])
                res_list.append(rec)
                
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})



#Fetching all the column values from store_traits table:
def store_traits_table(request):
    if request.method == 'GET':
        try:
            mycursor=connection.cursor()
            mycursor.execute("desc store")
            d_type=mycursor.fetchall()
            list_type=[]
            for col in d_type:
                if "decimal" in col[1]:
                    if "LOC_TRAIT" in col[0]:
                        list_type.append(col[0])
            
            query="select  DISTINCT  st.LOC_TRAIT,st.TRAIT_DESC from store_traits st;"
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                for col in results.columns:
                    rec[col]=val1[count]
                    count=count+1
                #converting LOCATION ,REV_NO AND ERR_SEQ_NO to INTEGER  if DECIMAL
                for col in list_type:
                    if col in rec:
                        rec[col]=int(rec[col])
                res_list.append(rec)
                
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})


#Fetching all the column values from alloc_code_detail table:
def rule_type_code_detail_table(request):
    if request.method == 'GET':
        try:
            #mycursor=connection.cursor()
   
            query="select cd.CODE,cd.CODE_DESC from code_detail cd where cd.code_type ='ALCR';"
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                for col in results.columns:
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



#Fetching all the column values from hierarchy_code_detail table:
def hierarchy_code_detail_table(request):
    if request.method == 'GET':
        try:
            #mycursor=connection.cursor()
   
            query="select cd.CODE,cd.CODE_DESC from code_detail cd where cd.code_type ='ALRL';"
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                for col in results.columns:
                    rec[col]=val1[count]
                    count=count+1
                for col in rec:
                    if rec["CODE_DESC"] == "Style/Diff":
                        rec["CODE_DESC"] = "Style/Variant"
                res_list.append(rec)
                
            if len(res_list)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})


#Fetching all the column values from need_code_detail table:
def need_code_detail_table(request):
    if request.method == 'GET':
        try:
            #mycursor=connection.cursor()
   
            query="select cd.CODE,cd.CODE_DESC from code_detail cd where cd.code_type ='ALRE';"
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                for col in results.columns:
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


#Fetching all the column values from  allocate_to_code_detail table:
def allocate_to_code_detail_table(request):
    if request.method == 'GET':
        try:
            #mycursor=connection.cursor()
   
            query="select cd.CODE,cd.CODE_DESC from code_detail cd where cd.code_type ='ALRN';"
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                for col in results.columns:
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



#Fetching all the column values from  clearance_code_detail_table table:
def clearance_code_detail_table(request):
    if request.method == 'GET':
        try:
            #mycursor=connection.cursor()
   
            query="select cd.CODE,cd.CODE_DESC from code_detail cd where cd.code_type ='ALCF';"
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                for col in results.columns:
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


#Fetching all the column values from  status_code_detail_table table:
def status_code_detail_table(request):
    if request.method == 'GET':
        try:
            #mycursor=connection.cursor()
   
            query="select cd.CODE,cd.CODE_DESC from code_detail cd where cd.code_type ='ALIS';"
            results=pd.read_sql(query,connection)
            res_list=[]
            for val1 in results.values:
                count=0
                rec={}
                for col in results.columns:
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


def establish_connection():
    global conn_global
    #if conn_global is None:
    #    conn_generator =get_mysql_conn([])
    #    conn_global =  conn_generator.__enter__()
    I_db_connect_status = list()
    I_db_connect_status.append(0)
    conn_generator =get_mysql_conn(I_db_connect_status)
    conn_global =  conn_generator.__enter__()
    

#def establish_connection():
#    global conn_global
#    conn_global = None
#    with get_mysql_conn(0) as conn:
#        conn_global = conn



#FETCHING LOCATION GRID FROM ALLOC_LOCATION TABLE:
@csrf_exempt
def get_LocationGrid(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            res_data=[]
            if "ALLOC_NO" in data and data["ALLOC_NO"] !=None and data["ALLOC_NO"] !="NULL":
                query="select * from alloc_location where alloc_no= "+ str(data["ALLOC_NO"])+";"
                result=pd.read_sql(query,connection)
                for val in result.values:
                    count=0
                    l_dict={}
                    for col in result.columns:
                        l_dict[col]=val[count]
                        count=count+1
                    res_data.append(l_dict)
                if len(res_data)==0:
                       return JsonResponse({"status": 500, "message": "No Data Found"})
                else:
                    return JsonResponse(res_data, content_type="application/json",safe=False) 
            else:
                return JsonResponse({"status": 500, "message": "Invalid ALLOCATION NUMBER"}) 
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})


@csrf_exempt
def alloc_rtv_rl_data_table(request):
    if request.method == 'POST':
        try:
            establish_connection()
            json_object = json.loads(request.body)
            json_object=json_object[0]
            if "ALLOC_NO" in json_object:
                rtvloc,err_msg1  =  RTV_LOCS(conn_global,json_object["ALLOC_NO"])
                rtvrule,err_msg2 =  rtv_rule(conn_global,json_object["ALLOC_NO"])
                alloc_rule       =  df_conversion(rtvrule)
                alloc_location   =  df_conversion(rtvloc)

                check_res        =  pd.read_sql("select * from alloc_loc_group_detail where alloc_no = {}".format(json_object["ALLOC_NO"]),conn_global)
                  
                #if len(alloc_location)>0:
                #    for row in alloc_location:
                #        if row["CLEARANCE_IND"]=="NULL" or row["CLEARANCE_IND"]=="" or row["CLEARANCE_IND"]==None:
                #            row["CLEARANCE_IND"]=""
                #            row["CLEARANCE_IND_DESC"]=""
                #        else:
                #            query1="select CODE_DESC from code_detail where CODE_TYPE='ALCF' and code = '{}'".format(row["CLEARANCE_IND"])
                #            result1=pd.read_sql(query1,conn_global)
                #            row["CLEARANCE_IND_DESC"]=str(result1["CODE_DESC"][0])

                #        if row["ITEM_LOC_STATUS"]=="NULL" or row["ITEM_LOC_STATUS"]=="" or row["ITEM_LOC_STATUS"]==None:
                #            row["ITEM_LOC_STATUS"]=""
                #            row["ITEM_LOC_STATUS_DESC"]=""
                #        else:
                #            query2="select CODE_DESC from code_detail where CODE_TYPE='ALIS' and code = '{}'".format(row["ITEM_LOC_STATUS"])
                #            result2=pd.read_sql(query2,conn_global)
                #            row["ITEM_LOC_STATUS_DESC"]=str(result2["CODE_DESC"][0])
                print("DEBUG CONN RTV DATA RL :  ",conn_global)
                if len(check_res) == 0 or len(alloc_rule) == 0 or  len(alloc_location) == 0:
                    res_list = [{"DATA":0,"ALLOC_RULE": err_msg2, "ALLOC_LOCATION":err_msg1}]
                    return JsonResponse({"status": 200, "message":res_list})
                    #return JsonResponse(res_list, content_type="application/json",safe=False)

                res_list = [{"DATA":1,"ALLOC_RULE": alloc_rule, "ALLOC_LOCATION":alloc_location}]
                return JsonResponse({"status": 200, "message":res_list})
                #return JsonResponse(res_list, content_type="application/json",safe=False)
            
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})

############dataframe needed#############
@csrf_exempt
def Alloc_DEL_LOCS_table(request):
    if request.method == 'POST':
        try: 
            print("DEBUG CONN Refresh :  ",conn_global)
            json_object = json.loads(request.body)
            mycursor = conn_global.cursor()
            json_object = json_object[0]
            if "LOC_LIST" in json_object  and len(json_object["LOC_LIST"])>0:
                query ="update alloc_location_temp set DEL_IND ='Y' where ALLOC_NO ="+str(json_object["ALLOC_NO"])+ " and LOC in "
                if len(json_object["LOC_LIST"]) == 1:
                    json_object["LOC_LIST"] = (json_object["LOC_LIST"])[0]
                    query = query+" ("+str(json_object["LOC_LIST"])+");"
                else:
                    query = query+str(tuple(json_object["LOC_LIST"]))+";"
                mycursor.execute(query)
                if mycursor.rowcount > 0:
                    rtvdel,err_msg = DEL_LOCS(conn_global,json_object["ALLOC_NO"])
                    if len(err_msg) > 0:
                        return JsonResponse({"status": 500, "message":str(err_msg)})
                    return JsonResponse({"status": 200, "message":" DATA DELETED"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})



@csrf_exempt
def Alloc_INS_DATA_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            mycursor = conn_global.cursor()
            
            json_object=data[0]
            data.pop(0)
            data=data[0]
            create_query="""CREATE TEMPORARY TABLE IF NOT EXISTS alloc_location_temp2
                                                                       (ALLOC_NO             INT    
                                                                       ,LOC                  VARCHAR(25) 
                                                                       ,LOC_DESC             VARCHAR(150) 
                                                                       ,LOC_TYPE             VARCHAR(1)   
                                                                       ,DEFAULT_WH           NUMERIC(4)     
                                                                       ,GROUP_ID             VARCHAR(40)  
                                                                       ,GROUP_DESC           VARCHAR(600) 
                                                                       ,LIKE_LOC             NUMERIC(10)    
                                                                       ,LIKE_LOC_DESC        VARCHAR(150) 
                                                                       ,WEIGHT_PCT           NUMERIC(12)  
                                                                       ,CLEARANCE_IND        VARCHAR(1)   
                                                                       ,ITEM_LOC_STATUS      VARCHAR(1)   
                                                                       ,RELEASE_DATE         DATE          
                                                                       ,DEL_IND              VARCHAR(1)
                                                                       ,CONSTRAINT chk_clear_ind1 CHECK (CLEARANCE_IND IN ('Y','N','L'))
                                                                       ,CONSTRAINT pk_loc PRIMARY KEY (ALLOC_NO,LOC));"""
            mycursor.execute(create_query)
            delete_query="delete from alloc_location_temp2;"
            mycursor.execute(delete_query)
            D_keys=[]
            for row in data:
                for col in row:
                    if col=="CLEARANCE_IND_DESC" or col=="ITEM_LOC_STATUS_DESC":
                        D_keys.append(col)
                for key in D_keys:
                    row.pop(key)
                D_keys.clear()

            for row in data:
                for col in row:
                    if row[col]=="NULL" or row[col]==None or row[col]=="":
                        row[col] = None
            
            cols = ','.join(data[0].keys())
            vals = [tuple(row.values()) for row in data]
            query = f"INSERT INTO alloc_location_temp2 ({cols}) VALUES ({','.join(['%s']*len(data[0].keys()))})"
            mycursor.executemany(query, vals)
            conn_global.commit()
                
            json_object=json_object[0]
            if "ALLOC_NO" in json_object and "ENFORCE_WH_RL" in json_object and "SIZE_PROFILE_IND" in json_object:
                #for col in json_object:
                #    if col=="RULE_LEVEL" and len(json_object["RULE_LEVEL"])>0:
                #        RULE_LEVEL=json_object.get("RULE_LEVEL")
                #        query2="select CODE from code_detail where CODE_TYPE='ALRL' and CODE_DESC='{}';".format(RULE_LEVEL)
                #        mydata1=pd.read_sql(query2,connection)
                #        l_dict1={}
                #        for val in mydata1.values:
                #            count=0
                #            for col in mydata1.columns:
                #                l_dict1[col]=val[count]
                #                count=count+1
                #        json_object["RULE_LEVEL"]=l_dict1["CODE"]

                #    if col=="RULE_TYPE" and len(json_object["RULE_TYPE"])>0:
                #        RULE_TYPE=json_object.get("RULE_TYPE")
                #        query2="select CODE from code_detail where CODE_TYPE='ALCR' and CODE_DESC='{}';".format(RULE_TYPE)
                #        mydata1=pd.read_sql(query2,connection)
                #        l_dict1={}
                #        for val in mydata1.values:
                #            count=0
                #            for col in mydata1.columns:
                #                l_dict1[col]=val[count]
                #                count=count+1
                #        json_object["RULE_TYPE"]=l_dict1["CODE"]

                for col in json_object:
                    if json_object[col]=="NULL" or json_object[col]=="" or json_object[col]==None:
                        json_object[col]=None

                result,err_msg = insert_rule_data(conn_global,json_object)
                if result:
                    result1, err_msg2 = fetch_chng_wt(conn_global,json_object["ALLOC_NO"])
                    if result1:
                        rtvcheck,err_msg3 = STORE_WH(conn_global,json_object["ALLOC_NO"],json_object["ENFORCE_WH_RL"])
                        if rtvcheck:
                            rtvsize,err_msg4 = SIZE_PROF(conn_global,json_object["ALLOC_NO"],json_object["SIZE_PROFILE_IND"])
                            if  rtvsize:
                                inscheck,err_msg5 = INS_LOCS (conn_global,json_object["ALLOC_NO"])
                                if inscheck :
                                    conn_global.close()
                                    return JsonResponse({"status": 200, "message":"Setup complete"})
                                else:
                                    conn_global.close()
                                    return JsonResponse({"status": 500, "message":"INS_LOCS failed"+str(err_msg5)})
                            else:
                                conn_global.close()
                                return JsonResponse({"status": 500, "message":"SIZE_PROF failed"+str(err_msg4)})
                        else:
                            conn_global.close()
                            return JsonResponse({"status": 500, "message":"STORE_WH failed"+str(err_msg3)})
                    else:
                        conn_global.close()
                        return JsonResponse({"status": 500, "message":"Change Weights failed"+str(err_msg2)})
                else:
                    conn_global.close()
                    return JsonResponse({"status": 500, "message":str(err_msg)})
            conn_global.close()
            return JsonResponse({"status": 500, "message":"invaild input"}) 
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})


@csrf_exempt
def Alloc_pop_store_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object = json_object[0]
            key_list = ["ALLOC_NO","ALL_STORE","LOCATION","LOCATION_LIST","LOCATION_TRAIT","EXCLUDE_LOCATION"] 
            count = 0
            print("DEBUG CONN POP store ADD ",conn_global)
            for key in range(len(key_list)):
                if key_list[key] in json_object:
                    count = count+1
            if count == len(key_list):
                df_store,err_msg = pop_store(conn_global,json_object)
                #print("Alloc_pop_store_table: ",df_store,err_msg)
                if len(df_store) > 0:
                    #result=df_conversion(df_store)
                    result = df_store.to_dict("records")
                    return JsonResponse(result, content_type="application/json",safe=False)
                else:
                    if len(err_msg) > 0:
                        return JsonResponse({"status": 500, "message":str(err_msg)})
                    return JsonResponse({"status": 500, "message": "No Data Found"})
            return JsonResponse({"status": 500, "message": "invaild input"})   
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})

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




################################
#         RULES                #
################################

@csrf_exempt 
def alloc_rule_Data(request):
    if request.method == 'POST':
        try:
            json_object=json.loads(request.body)
            json_object=json_object[0]

            #for row in json_object:
            #for col in json_object:
            #    if col=="RULE_LEVEL" and len(json_object["RULE_LEVEL"])>0:
            #        RULE_LEVEL=json_object.get("RULE_LEVEL")
            #        query2="select CODE from code_detail where CODE_TYPE='ALRL' and CODE_DESC='{}';".format(RULE_LEVEL)
            #        mydata1=pd.read_sql(query2,conn_global)
            #        l_dict1={}
            #        for val in mydata1.values:
            #            count=0
            #            for col in mydata1.columns:
            #                l_dict1[col]=val[count]
            #                count=count+1
            #        json_object["RULE_LEVEL"]=l_dict1["CODE"]

            #    if col=="RULE_TYPE" and len(json_object["RULE_TYPE"])>0:
            #        RULE_TYPE=json_object.get("RULE_TYPE")
            #        query2="select CODE from code_detail where CODE_TYPE='ALCR' and CODE_DESC='{}';".format(RULE_TYPE)
            #        mydata1=pd.read_sql(query2,conn_global)
            #        l_dict1={}
            #        for val in mydata1.values:
            #            count=0
            #            for col in mydata1.columns:
            #                l_dict1[col]=val[count]
            #                count=count+1
            #        json_object["RULE_TYPE"]=l_dict1["CODE"]

            for col in json_object:
                if json_object[col]=="NULL" or json_object[col]=="" or json_object[col]==None:
                    json_object[col]=None

            result, err_msg = insert_rule_data(conn_global,json_object)
            if result:
                result1, err_msg2 = fetch_chng_wt(conn_global,json_object["ALLOC_NO"])
                if result1:
                    results55, err_msg3 = fetch_retrieve_chng_wt(conn_global,json_object["ALLOC_NO"])
                    res_list = df_conversion(results55)
                    if len(res_list) == 0:
                        return JsonResponse({"status": 500, "message":"fetch_retrieve_chng_wt python function is failed. "+str(err_msg3)})
                    else:
                        return JsonResponse(res_list,content_type="application/json",safe=False) 
                else:
                    return JsonResponse({"status": 500, "message":"fetch_chng_wt python function is failed. "+str(err_msg2)})
            else:
                return JsonResponse({"status": 500, "message": str(err_msg)})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})



@csrf_exempt
def Fetch_Alloc_change_weights_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            keys=[]
            mycursor=conn_global.cursor()
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
                        query="update alloc_rules_weight_temp set WEIGHT={}".format(int(row["WEIGHT"])/100)+" where EOW_DATE='{}'".format(row["EOW_DATE"])+ " and ALLOC_NO='{}'".format(row["ALLOC_NO"])
                        mycursor.execute(query)
            connection.commit()
            json_object=json_object[0]
            if "ALLOC_NO" in json_object:
                result,err_msg = fetch_load_rule_dates_wt(conn_global,json_object["ALLOC_NO"])
            if result:
                return JsonResponse({"status": 201, "message": "EOW DATE WEIGHT IS UPDATED SUCCESSFULL"})
            else: 
                return JsonResponse({"status": 500, "message": "ERROR"+str(err_msg)})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})




@csrf_exempt
def RL_Data_check_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data = data[0]

            rec = {"RL_LOCATIONS": 0,       "RL_RULES": 0,      "RL_RULE_DATA": [],     "LIM": 0,
                   "LIM_DATA_CHECK": 0,     "QL": 0,            "QL_DATA_CHECK": 0,
                   "CALC_ITEM_COUNT": 0,    "ALLOC_HEAD": [],   "SCHEDULE": 0}

            tables = [
                        ("alloc_loc_group_detail",   "RL_LOCATIONS"),
                        ("alloc_rule",               "RL_RULES"),
                        ("alloc_like_item_source",   "LIM"),
                        ("alloc_quantity_limits",    "QL"),
                        ("alloc_schedule_params",    "SCHEDULE")
                    ]
            
            for table, column in tables:
                query = "SELECT * FROM {} WHERE ALLOC_NO = {}".format(table, data["ALLOC_NO"])
                result = pd.read_sql(query, connection)
                if len(result) > 0:
                    rec[column] = 1
                else:
                    rec[column] = 0

            if rec["RL_RULES"] == 1:
                query1  = "select * from alloc_rule where ALLOC_NO= {} ".format(data["ALLOC_NO"])  
                result1 = pd.read_sql(query1,connection)
                result1 = result1.replace(np.NaN, None, regex=True)
                result1 = result1.to_dict("records")
                rec["RL_RULE_DATA"] = result1 if len(data) > 0 else []

            if rec["LIM"] == 1:
                query1  = "select LIKE_SOURCE_ITEM from alloc_like_item_source where ALLOC_NO= {} ".format(data["ALLOC_NO"])                   
                result1 = pd.read_sql(query1,connection)
                result1 = result1.dropna()
                rec["LIM_DATA_CHECK"] = 1 if len(result1) > 0 else 0

            if rec["QL"] == 1:
                query1  = "select MIN, MAX, TRESHOLD, TREND, WOS, MIN_NEED from alloc_quantity_limits where ALLOC_NO= {} ".format(data["ALLOC_NO"])  
                result1 = pd.read_sql(query1,connection)
                #result1 = result1.dropna(axis=1, how='all')
                #result1 = result1.replace(np.NaN, None, regex=True)
                result1 = result1.to_dict("records")
                data_res = [d for d in result1 if any(v is not None for v in d.values())]
                rec["QL_DATA_CHECK"] = 1 if len(data_res) > 0 else 0

            result1 = pd.read_sql("select * from alloc_calc_item_loc where alloc_no = {}".format(data["ALLOC_NO"]),connection)
            rec["CALC_ITEM_COUNT"] = 1 if len(result1) > 0 else 0

            query1  = "select * from alloc_head where ALLOC_NO= {} ".format(data["ALLOC_NO"])  
            result1 = pd.read_sql(query1,connection)
            result1 = result1.replace(np.NaN, None, regex=True)
            result1 = result1.to_dict("records")
            rec["ALLOC_HEAD"] = result1 if len(data) > 0 else []

            #print(rec)
            return JsonResponse({"status": 200, "message": [rec]})
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        
#################################
### RL templates starts ##
################################        
def rule_template_name_data(request):
    if request.method == 'GET':
        try:            
            query ='select DISTINCT TEMPLATE_NAME,TEMPLATE_NO from alloc_rule_template;'  
            results = pd.read_sql(query,connection)
            #res_list=[]                
            if len(results)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                res_list = results.to_dict("records")
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        
def loc_template_name_data(request):
    if request.method == 'GET':
        try:
            query ='select DISTINCT TEMPLATE_NAME,TEMPLATE_NO from alloc_location_template;'  
            results = pd.read_sql(query,connection)
            #res_list=[]                
            if len(results)==0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                res_list = results.to_dict("records")
                return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        
@csrf_exempt
def RL_ruleTemplateData(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            mycursor = connection.cursor()
            mycursor.execute("desc alloc_rule_template")
            d_type = mycursor.fetchall()
            dtype_columns = {column[0] for column in d_type}
            filtered_data = [
                {key: value for key, value in record.items() if key in dtype_columns}
                for record in data
            ]
            D_keys=[]
            for row in filtered_data:
                for col in row:
                    if row[col]=="NULL" or row[col]==None or row[col]=="" or col=="SR_NO":
                        D_keys.append(col)
                for key in D_keys:
                    row.pop(key)
                D_keys.clear()
            cols = ','.join(filtered_data[0].keys())
            vals = [tuple(row.values()) for row in filtered_data]
            query = f"INSERT INTO alloc_rule_template ({cols}) VALUES ({','.join(['%s']*len(filtered_data[0].keys()))})"
            print("query: ",query)
            mycursor.executemany(query, vals)
            connection.commit()
            return JsonResponse({"status": 201, "message": "TEMPLATE CREATED SUCCESSFULL"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        
@csrf_exempt
def RL_locTemplateData(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            mycursor = connection.cursor()
            print("data: ",data)
            D_keys=[]
            for row in data:
                for col in row:
                    if row[col]=="NULL" or row[col]==None or row[col]=="" or col=="SR_NO":
                        D_keys.append(col)
                for key in D_keys:
                    row.pop(key)
                D_keys.clear()
            cols = ','.join(data[0].keys())
            vals = [tuple(row.values()) for row in data]
            query = f"INSERT INTO alloc_location_template ({cols}) VALUES ({','.join(['%s']*len(data[0].keys()))})"
            print("query: ",query)
            mycursor.executemany(query, vals)
            connection.commit()
            return JsonResponse({"status": 201, "message": "TEMPLATE CREATED SUCCESSFULL"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        
@csrf_exempt
def Fetch_RuleTemplateData(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            print(data)
            res_data=[]
            if len(data) > 0:
                data = data[0]
                query = "select * from alloc_rule_template where TEMPLATE_NO= "+ str(data["TEMPLATE_NO"])+";"
                result = pd.read_sql(query,connection)
                for val in result.values:
                    count=0
                    l_dict={}
                    for col in result.columns:
                        l_dict[col]=val[count]
                        count=count+1
                    res_data.append(l_dict)
                for rec in res_data:
                    for col in rec:
                        if rec[col] == None or rec[col] == "NULL" or rec[col] == "NaN":
                            rec[col]=""
                if len(res_data)==0:
                    return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
                else:
                    return JsonResponse(res_data,content_type="application/json",safe=False)
            else:
                return JsonResponse(res_data,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        
@csrf_exempt
def Fetch_locTemplateData(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            res_data = []
            locations_list = []
            locations = ""
            mycursor = conn_global.cursor()
            json_object =  {'LOCATION': [], 'LOCATION_LIST': [], 'LOCATION_TRAIT': [], 'EXCLUDE_LOCATION': [], 'ALL_STORE': 'N', 'ALLOC_NO': ""}
            if len(data) > 0:
                data = data[0]
                query = "select * from alloc_location_template where TEMPLATE_NO= "+ str(data["TEMPLATE_NO"])+";"
                result = pd.read_sql(query,connection)
                if len(result) > 0:
                    for val in result.values:
                        count=0
                        l_dict={}
                        for col in result.columns:
                            l_dict[col]=val[count]
                            count=count+1
                        res_data.append(l_dict)
                    for rec in res_data:
                        for col in rec:
                            if rec[col] == None or rec[col] == "NULL" or rec[col] == "NaN":
                                rec[col]=""
                    for rec in res_data:
                        for col in rec:
                            if col == "LOCATION": 
                                locations = rec[col]                
                    if len(locations) > 0:
                        locations_list = locations.split(',')
                        
                    if len(locations_list) > 0:
                        json_object["LOCATION"] = locations_list
                        json_object["ALLOC_NO"] = data["ALLOC_NO"]
                        
                    if len(data["LOCATION_DATA"]) > 0:
                        query1 ="update alloc_location_temp set DEL_IND ='Y' where ALLOC_NO ="+str(json_object["ALLOC_NO"])+ ";"
                        mycursor.execute(query1)
                        rtvdel,err_msg1 = DEL_LOCS(conn_global,json_object["ALLOC_NO"])
                         
                    json_object["EXCLUDE_LOCATION"] = data["EXCLUDE_LOCATION"]    
                    df_store,err_msg = pop_store(conn_global,json_object)
                    if len(df_store) > 0:
                        result = df_store.to_dict("records")
                        return JsonResponse(result, content_type="application/json",safe=False)
                    else:
                        if len(err_msg) > 0:
                            return JsonResponse({"status": 500, "message":str(err_msg)})
                        return JsonResponse({"status": 500, "message": "No Data Found"})
                else:
                    return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                return JsonResponse(res_data,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})

#################################
### RL templates ends ##
################################ 