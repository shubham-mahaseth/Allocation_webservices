import json
import csv
import pandas as pd
from django.db import IntegrityError
from django.http import JsonResponse,HttpResponse,StreamingHttpResponse
from datetime import datetime,date
from django.views.decorators.csrf import csrf_exempt
from decimal import *
import numpy as np
from django.db import connection
from .Allocation_functions.Allocation.LIKE_ITEM.retrieve_like_item_map_wrapper import rtv_like_item
from .Allocation_functions.Allocation.LIKE_ITEM.like_item_del_button_wrapper import delete_mapped_item
from .Allocation_functions.Allocation.LIKE_ITEM.like_item_map_button_wrapper import map_item
from .Allocation_functions.Allocation.LIKE_ITEM.like_item_ok_button_wrapper import insert_mapped_item
from .Allocation_functions.Allocation.LIKE_ITEM.no_skus_wrapper import get_skus
from .Allocation_functions.Allocation.CREATE_SCREEN.retreive_multi_po_wrapper import multi_po
from .connect import get_mysql_conn
conn_global =None


def HIER1_table(request):
    if request.method == 'GET':
        try:
            query="select distinct h1.HIER1,h1.HIER1_DESC from hier1 h1 ORDER BY cast(h1.HIER1 as unsigned);"
            df_result=pd.read_sql(query,connection)        
            res_list=[]
            rec={}
            df_result =  df_result.replace(np.NaN, "NULL", regex=True)
            for val2 in df_result.values:
                count=0
                for col4 in df_result.columns:
                    rec[col4]=val2[count]
                    count=count+1
                res_list.append(rec.copy())  
            return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
                connection.close()
        
#FETCHING ALL THE COLUMN VALUES FROM HIER2 TABLE:
@csrf_exempt
def HIER2_table(request):
    if request.method == 'POST':
        try: 
            print(request.body)
            data = json.loads(request.body)
            data=data[0]
            if "HIER1" in data:
                query="select distinct h2.HIER1,h2.HIER2,h2.HIER2_DESC from hier2 h2 where h2.HIER1 in "
                if len(data["HIER1"])==1:
                    data["HIER1"]=(data["HIER1"])[0]
                    query=query+"("+str(data["HIER1"])+")"
                else:
                    query=query+str(tuple(data["HIER1"]))
                query=query+' ORDER BY cast(h2.HIER2 as unsigned);'
                df_result=pd.read_sql(query,connection)
                res_list=[]
                rec={}
                df_result =  df_result.replace(np.NaN, "NULL", regex=True)
                for val2 in df_result.values:
                    count=0
                    for col4 in df_result.columns:
                        rec[col4]=val2[count]
                        count=count+1
                    res_list.append(rec.copy())
                return JsonResponse(res_list,content_type="application/json",safe=False)
            else:
                return JsonResponse({"status": 500, "message":"Invalid Input"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
                connection.close()

#FETCHING ALL THE COLUMN VALUES FROM HIER TABLE:
@csrf_exempt
def HIER3_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if "HIER1" in data:
                query="select distinct h3.HIER1,h3.HIER2,h3.HIER3,h3.HIER3_DESC from hier3 h3 where  h3.HIER1 in "
                if len(data["HIER1"])==1:
                    data["HIER1"]=(data["HIER1"])[0]
                    query=query+"("+str(data["HIER1"])+")"
                else:
                    query=query+str(tuple(data["HIER1"]))
                query=query+' ORDER BY cast(h3.HIER3 as unsigned);'
                results55=pd.read_sql(query,connection)
                res_list=[]
                rec={}
                results55 =  results55.replace(np.NaN, "NULL", regex=True)
                for val2 in results55.values:
                    count=0
                    for col4 in results55.columns:
                        rec[col4]=val2[count]
                        count=count+1
                
                    res_list.append(rec.copy())
                return JsonResponse(res_list,content_type="application/json",safe=False)
            else:
                return JsonResponse({"status": 500, "message":"Invalid Input"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
                connection.close()


#FETCHING ALL THE COLUMN VALUES FROM UDA TABLE:
@csrf_exempt
def UDA_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]

            if "HIER1" in data:
                query='''select uda.HIER1,uda.HIER2,uda.HIER3,uda.ITEM as SKU,uda.ITEM_PARENT,uda.USER_ATTR_ID,uda.USER_ATTR_DESC,uda.USER_ATTR_VALUE,uda.USER_ATTR_VALUE_DESC from 
                        (select distinct itd.HIER1,itd.HIER2,itd.HIER3,itd.ITEM,itd.ITEM_PARENT,ur.USER_ATTR_ID,ur.USER_ATTR_DESC,ur.USER_ATTR_VALUE,ur.USER_ATTR_VALUE_DESC 
                                   from user_attr_item urt,user_attr ur, item_dtl itd 
                                where urt.USER_ATTR_ID=ur.USER_ATTR_ID and urt.USER_ATTR_VALUE=ur.USER_ATTR_VALUE
                                and urt.item=itd.item) uda where uda.HIER1 in '''
                if len(data["HIER1"])==1:
                    data["HIER1"]=(data["HIER1"])[0]
                    query=query+"("+str(data["HIER1"])+")"
                else:
                    query=query+str(tuple(data["HIER1"]))
           
                query=query+" ORDER BY cast(uda.USER_ATTR_ID as unsigned),cast(uda.USER_ATTR_VALUE as unsigned);"
                df_result=pd.read_sql(query,connection)
                res_list=[]
                rec={}
                df_result =  df_result.replace(np.NaN, "NULL", regex=True)
                for val2 in df_result.values:
                    count=0
                    val_list123=[]
                    for col4 in df_result.columns:
                        rec[col4]=val2[count]
                        val_list123.append(rec[col4])
                        count=count+1
                    res_list.append(rec.copy())
                val_list12=[]
                val_list123=[]
                D_keys=[]
                for rows in res_list:
                    for key in rows:
                        if rows[key]=="" or rows[key]=="NULL":
                            D_keys.append(key) 
                    for key in D_keys:
                        rows.pop(key)
                    D_keys.clear()
                    for col1 in rows:
                        if col1=="USER_ATTR_ID":
                            val_list12.append(col1)
                        if col1=="USER_ATTR_VALUE":
                            val_list123.append(col1)
                    for k in val_list12:
                        rows["UDA"]=rows.pop(k)
                    val_list12.clear()   
                    for k1 in val_list123:
                        rows["UDA_VALUE"]=rows.pop(k1)
                    val_list123.clear()
                return JsonResponse(res_list,content_type="application/json",safe=False)
            else:
                return JsonResponse({"status": 500, "message":"Invalid Input"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
                connection.close()


#FETCHING ALL THE COLUMN VALUES FROM UDA_VALUE TABLE. NO INPUT REQUIRED 
@csrf_exempt
def EXCLUDE_UDA_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            val_list1=[]

            for row in json_object:
                for col in row:
                    if col not in ["EXCLUDE_UDA","EXCLUDE_UDA_VALUE"]:
                        val_list1.append(col)
                for key in val_list1:
                    row.pop(key)
                val_list1.clear()

            query="select distinct uda.USER_ATTR_ID,uda.USER_ATTR_DESC,uda.USER_ATTR_VALUE,uda.USER_ATTR_VALUE_DESC from user_attr uda ORDER BY cast(uda.USER_ATTR_ID as unsigned),cast(uda.USER_ATTR_VALUE as unsigned);"
             
            results55=pd.read_sql(query,connection)
            res_list=[]
            rec={}
            results55 =  results55.replace(np.NaN, "NULL", regex=True)
            for val2 in results55.values:
                count=0
                val_list123=[]
                for col4 in results55.columns:
                    rec[col4]=val2[count]
                    val_list123.append(rec[col4])
                    count=count+1
                res_list.append(rec.copy()) 
            ###print(res_list)
            val_list12=[]
            val_list123=[]
            D_keys=[]
            for rows in res_list:
                for key in rows:
                    if rows[key]=="" or rows[key]=="NULL":
                        D_keys.append(key) 
                for key in D_keys:
                    rows.pop(key)
                D_keys.clear()
                for col1 in rows:
                    if col1=="USER_ATTR_ID":
                        val_list12.append(col1)
                    if col1=="USER_ATTR_VALUE":
                        val_list123.append(col1)
                for k in val_list12:
                    rows["EXCLUDE_UDA"]=rows.pop(k)
                val_list12.clear()   
                for k1 in val_list123:
                    rows["EXCLUDE_UDA_VALUE"]=rows.pop(k1)
                val_list123.clear()
            return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
                connection.close()


#FETCHING ALL THE COLUMN VALUES FROM ITEM_LIST_HEAD TABLE:
def item_list_head_table(request):
    if request.method == 'GET':
        try:
            mycursor=connection.cursor()
            mycursor.execute("desc item_list_head")
            d_type=mycursor.fetchall()
            list_type=[]
            for col in d_type:
                if "decimal" in col[1]:
                    if "ITEM_LIST_NO" in col[0]:
                        list_type.append(col[0])
            
            query="select ilh.ITEM_LIST_NO ITEM_LIST,ilh.ITEM_LIST_DESC from item_list_head ilh ORDER BY cast(ilh.ITEM_LIST_NO as unsigned);"
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
            return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
                connection.close()


#FETCHING ALL THE COLUMN VALUES FROM ITEM_PARENT TABLE:
@csrf_exempt
def ITEM_PARENT_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if "HIER1" in data:
                query="select id.HIER1,id.HIER2,id.HIER3,id.ITEM_PARENT from item_dtl id where id.ITEM_PARENT is not null and id.HIER1 in "
                if len(data["HIER1"])==1:
                    data["HIER1"]=(data["HIER1"])[0]
                    query=query+"("+str(data["HIER1"])+")"
                else:
                    query=query+str(tuple(data["HIER1"]))
                query=query+' ORDER BY id.ITEM_PARENT;'
                df_result=pd.read_sql(query,connection)
                res_list=[]
                rec={}
                df_result =  df_result.replace(np.NaN, "NULL", regex=True)
                for val2 in df_result.values:
                    count=0
                    for col4 in df_result.columns:
                        rec[col4]=val2[count]
                        count=count+1
                    res_list.append(rec.copy())
                return JsonResponse(res_list,content_type="application/json",safe=False)
            else:
                return JsonResponse({"status": 500, "message":"Invalid Input"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
                connection.close()


#Fetching all the column values from DIFF table:
@csrf_exempt
def DIFF_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if "HIER1" in data:
                query="select id.HIER1,id.HIER2,id.HIER3,id.ITEM_PARENT,id.ITEM as SKU,id.AGGR_DIFF_ID as DIFF_ID from item_dtl id where id.HIER1 in "
                if len(data["HIER1"])==1:
                    data["HIER1"]=(data["HIER1"])[0]
                    query=query+"("+str(data["HIER1"])+")"
                else:
                    query=query+str(tuple(data["HIER1"]))
                
                query=query+' ORDER BY id.DIFF1;'
                df_result=pd.read_sql(query,connection)
                df_result =  df_result.replace(np.NaN, "NULL", regex=True)
                if len(df_result) >0:
                    return JsonResponse(df_result.to_dict("records"),content_type="application/json",safe=False)
                else:
                    return JsonResponse([],content_type="application/json",safe=False)
            else:
                return JsonResponse({"status": 500, "message":"Invalid Input"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
                connection.close()


#Fetching all the column values from SKU table:
@csrf_exempt
def SKU_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if "HIER1" in data:
                query="select  id.HIER1,id.HIER2,id.HIER3,id.ITEM_PARENT,id.ITEM as SKU,id.ITEM_DESC as SKU_DESC ,id.DIFF1,id.DIFF2,id.DIFF3,id.DIFF4 from item_dtl id where id.PACK_IND='N' AND id.HIER1 in "
                if len(data["HIER1"])==1:
                    data["HIER1"]=(data["HIER1"])[0]
                    query=query+"("+str(data["HIER1"])+")"
                else:
                    query=query+str(tuple(data["HIER1"]))
                query=query+" ORDER BY id.ITEM;"
                df_result=pd.read_sql(query,connection)
                res_list=[]
                rec={}
                df_result =  df_result.replace(np.NaN, "NULL", regex=True)
                for val2 in df_result.values:
                    P_keys=[]
                    count=0
                    for col4 in df_result.columns:
                        rec[col4]=val2[count]
                        count=count+1
                    for col in df_result.columns:
                        if col=="ITEM":
                            P_keys.append(col)
                    for key in P_keys:
                        rec["SKU"]=rec.pop(key)
                    P_keys.clear()
                    res_list.append(rec.copy()) 
                temp_dict={}
                fin_res_list=[]
                D_keys=[]
                for rows in res_list:
                    row_keys=list(rows.keys())
                    row_keys.remove("DIFF1")
                    row_keys.remove("DIFF2")
                    row_keys.remove("DIFF3")
                    row_keys.remove("DIFF4")
                    for key in rows:
                        if rows[key]=="" or rows[key]=="NULL":
                            D_keys.append(key) 
                    for key in D_keys:
                        rows.pop(key)
                    D_keys.clear()
                    for col1 in rows:
                        if col1=="DIFF1"  or col1=="DIFF2" or col1=="DIFF3" or col1=="DIFF4" :
                            for rowKey in row_keys:
                                if rowKey in list(rows.keys()):
                                    temp_dict[rowKey]=rows[rowKey]
                            
                            temp_dict["DIFF_ID"]=rows[col1]
                            fin_res_list.append(temp_dict)
                            temp_dict={}
                        else: 
                            fin_res_list.append(rows)
                            
                return JsonResponse(fin_res_list,content_type="application/json",safe=False)
            else:
                return JsonResponse({"status": 500, "message":"Invalid Input"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
            connection.close()


#Fetching all the column values from Warehouse table:
def Warehouse_table(request):
    if request.method == 'GET':
        try:
            mycursor=connection.cursor()
            
            query="select wh.WH,wh.WH_DESC from warehouse wh where wh.WH is not null ORDER BY cast(wh.WH as unsigned);"
            mycursor.execute("desc warehouse")
            d_type=mycursor.fetchall()
            list_type=[]
            for col in d_type:
                if "decimal" in col[1]:
                    if "WH" in col[0]:
                        list_type.append(col[0])

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
                
            return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
            connection.close()


#Fetching all the column values from SUPPLIER table:
def SUPPLIER_table(request):
    if request.method == 'GET':
        try:
            mycursor=connection.cursor()
            mycursor.execute("desc sups")
            d_type=mycursor.fetchall()
            list_type=[]
            for col in d_type:
                if "decimal" in col[1]:
                    if "SUPPLIER" in col[0]:
                        list_type.append(col[0])
            
            query="select sp.SUPPLIER,sp.SUPPLIER_NAME from sups sp where sp.SUPPLIER is not null ORDER BY cast(sp.SUPPLIER as unsigned);"
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
                
            return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
            connection.close()

#Fetching all the column values from SUPPLIER table:
def PO_TYPE_table(request):
    if request.method == 'GET':
        try:
            query="select distinct pod.PO_TYPE from po_dtl pod where pod.PO_TYPE is not null ORDER BY cast(pod.PO_TYPE as unsigned);"
            df_result=pd.read_sql(query,connection)        
            res_list=[]
            rec={}
            df_result =  df_result.replace(np.NaN, "NULL", regex=True)
            for val2 in df_result.values:
                count=0
                for col4 in df_result.columns:
                    rec[col4]=val2[count]
                    count=count+1
                res_list.append(rec.copy())  
            return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
                connection.close()


#Fetching all the column values from SUPPLIER table:
def SUPPLIER_SITE_table(request):
    if request.method == 'GET':
        try:
            mycursor=connection.cursor()
            mycursor.execute("desc sups")
            d_type=mycursor.fetchall()
            list_type=[]
            for col in d_type:
                if "decimal" in col[1]:
                    if "SUPPLIER" in col[0]:
                        list_type.append(col[0])
            
            query="select sp.SUPPLIER,sp.SUPPLIER_NAME from sups sp where sp.SUPPLIER_PARENT is not null and sp.SUPPLIER is not null ORDER BY cast(sp.SUPPLIER as unsigned);"
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
                    if col=="SUPPLIER":
                        P_keys.append(col)
                for key in P_keys:
                    rec["SUPPLIER_SITE"]=rec.pop(key)
                P_keys.clear()
                res_list.append(rec)
                
            return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
            connection.close()


#Fetching all the column values from PACK_NO table:
@csrf_exempt
def PACK_NO_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            query="select id.HIER1,id.HIER2,id.HIER3,id.ITEM_PARENT,id.ITEM from item_dtl id where id.PACK_IND='Y' "
            query=query+' ORDER BY id.ITEM;'
            df_result=pd.read_sql(query,connection)
            res_list=[]
            rec={}
            df_result =  df_result.replace(np.NaN, "NULL", regex=True)
            for val2 in df_result.values:
                P_keys=[]
                count=0
                for col4 in df_result.columns:
                    rec[col4]=val2[count]
                    count=count+1
                for col in df_result.columns:
                    if col=="ITEM":
                        P_keys.append(col)
                for key in P_keys:
                    rec["PACK_NO"]=rec.pop(key)
                P_keys.clear()
                res_list.append(rec.copy()) 
            return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
            connection.close()


#Fetching all the column values from VPN table:
@csrf_exempt
def VPN_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if "HIER1" in data:                
                query="select id.HIER1,id.HIER2,id.HIER3,id.ITEM_PARENT,id.ITEM as SKU,its.PRODUCT_NUM from item_sups its,item_dtl id where its.item=id.item and id.HIER1 in "
                if len(data["HIER1"])==1:
                    data["HIER1"]=(data["HIER1"])[0]
                    query=query+"("+str(data["HIER1"])+")"
                else:
                    query=query+str(tuple(data["HIER1"]))
                query=query+' ORDER BY its.PRODUCT_NUM;'
                df_result=pd.read_sql(query,connection)
                res_list=[]
                rec={}
                df_result =  df_result.replace(np.NaN, "NULL", regex=True)
                for val2 in df_result.values:
                    count=0
                    val_list123=[]
                    for col4 in df_result.columns:
                        rec[col4]=val2[count]
                        val_list123.append(rec[col4])
                        count=count+1                    
                    res_list.append(rec.copy()) 
                val_dict1={}
                val_list12=[]
                D_keys=[]
                for rows in res_list:
                    for key in rows:
                        if rows[key]=="" or rows[key]=="NULL":
                            D_keys.append(key) 
                    for key in D_keys:
                        rows.pop(key)
                    D_keys.clear()
                    for col1 in rows:
                        if col1=="PRODUCT_NUM":
                            val_dict1["VPN"]=rows[col1]
                        else:
                            val_dict1[col1]=rows[col1]
                    val_list12.append(val_dict1)
                    val_dict1={}
                return JsonResponse(val_list12,content_type="application/json",safe=False)
            else:
                return JsonResponse({"status": 500, "message":"Invalid Input"})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
            connection.close()


#Fetching all the column values from PO table:
@csrf_exempt
def PO_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            json_object=json_object[0]
            keys=[]
            mycursor=connection.cursor()
            for key1 in json_object:
                if isinstance(json_object[key1], list):
                    if (len(json_object[key1]))==0:
                        json_object[key1]="NULL"
            for key in json_object:
                if json_object[key]=="NULL" or json_object[key]=="":
                    json_object[key]=None
                    keys.append(key)
            for k in keys:
                json_object.pop(k)
            mycursor.execute("desc po_dtl")
            d_type=mycursor.fetchall()
            list_type=[]
            for col in d_type:
                if "decimal" in col[1]:
                    if "PO_NO" in col[0]:
                        list_type.append(col[0])
            #checking the inputs are mutliple or not
            count=0
            for keys_2 in json_object:
                if isinstance(json_object[keys_2], list):
                    count=1
            if count==1:
                for keys1 in json_object:
                    if isinstance(json_object[keys1], list):
                        if len(json_object[keys1])>1:
                            json_object[keys1]=str(tuple(json_object[keys1]))
                    else:
                        json_object[keys1]=("('"+str(json_object[keys1])+"')")
                query="select distinct pod.PO_NO,pod.PO_TYPE from po_dtl pod WHERE {}".format(' '.join('pod.{} IN ({}) AND'.format(k,str(json_object[k])[1:-1]) for k in json_object))
            else:
                query="select distinct pod.PO_NO,pod.PO_TYPE from po_dtl pod WHERE {}".format(' '.join('pod.{} LIKE "%{}%" AND'.format(k,json_object[k]) for k in json_object))
            if len(json_object)==0:
                query=query[:-6]
            else:
                query=query[:-4]
            query=query+' ORDER BY cast(pod.PO_NO as unsigned),cast(pod.PO_TYPE as unsigned);'
            results55=pd.read_sql(query,connection) 
            res_list=[]
            rec={}
            results55 =  results55.replace(np.NaN, "NULL", regex=True)
            for val2 in results55.values:
                count=0
                P_keys=[]
                P_keys1=[]
                P_keys2=[]
                for col4 in results55.columns:
                    rec[col4]=val2[count]
                    count=count+1
                for col5 in list_type:
                    if col5 in rec:
                        if rec[col5]!=None or rec[col5]!="NULL":
                            rec[col5]=int(rec[col5])
                for col in results55.columns:
                    if col=="PO_NO":
                        P_keys.append(col)
                for key in P_keys:
                    rec["PO"]=rec.pop(key)
                P_keys.clear()
                for col in results55.columns:
                    if col=="PO_CATEGORY":
                        P_keys1.append(col)
                for key in P_keys1:
                    rec["PO_TYPE"]=rec.pop(key)
                #for row in rec:
                #    if rec[row]=="NULL" or rec[row]=="" or rec[row]==None:
                #        P_keys2.append(row)
                #for key in P_keys2:
                #    rec.pop(key)
                #P_keys1.clear()
                res_list.append(rec.copy())     
            return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
            connection.close()


#Fetching all the column values from ASN table:
@csrf_exempt
def ASN_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            val_list=[]
            val_list1=[]
            val_list2=[]
            val_list3=[]
            val_list4=[]
            k_list=[]
            k_list1=[]
            k_list2=[]
            for row in json_object:
                for col in row:
                    if col=="PACK_NO":
                        k_list.append(col)
                    if col=="SKU":
                        k_list1.append(col) 
                for k in k_list:
                    row["ITEM"]=row.pop(k)
                k_list.clear()   
                for k1 in k_list1:
                    row["ITEM"]=row.pop(k1)
                k_list1.clear()
  
            for row in json_object:
                for col in row:
                    if col=="HIER1":
                        val_list.append(row[col])
                    if col=="HIER2":
                        val_list1.append(row[col])
                    if col=="HIER3":
                        val_list2.append(row[col])
                    if col=="ITEM_PARENT":
                        val_list3.append(row[col])
                    if col=="ITEM":
                        val_list4.append(row[col])
                    if col not in ["HIER1","HIER2","HIER3","ITEM_PARENT","ITEM"]:
                    #if col=="HIER1" or col=="HIER2" or col=="HIER3" or col=="ITEM_PARENT" or col=="ITEM":
                        k_list2.append(col)
                for key in k_list2:
                    row.pop(key)
                k_list2.clear()
            if len(json_object)>=1:
                json_object=[i for i in json_object if i]
                              
            if len(json_object)==0:
                query="select distinct sdtl.ASN_ID from ship_dtl sdtl "
            else:
                #query="select distinct sdtl.ASN_ID from ship_dtl sdtl,ship_item_loc sil,item_dtl id where sdtl.SHIP_ID=sil.SHIP_ID and sil.ITEM=id.ITEM and "
                query = """SELECT distinct sh.ASN_ID                              
                                    FROM ship_dtl sh, warehouse wh,  po_dtl oh, item_dtl im, ship_item shk
                             WHERE sh.SHIP_ID = shk.SHIPMENT
                                   AND shk.item = im.item
                                   AND wh.STOCK_HOLDING_IND = 'Y'
                                   AND wh.FINISHER = 'N'
                                   AND wh.REDIST_WH_IND = 'N'
                                   AND wh.REPLENISH_IND = 'Y'
                                   AND sh.TO_LOCATION =wh.PHYSICAL_WH
                                   AND sh.PO_NO is not null
                                   AND sh.RECEIVED_DATE is null
                                   AND sh.ASN_ID is not null
                                   AND sh.SHIP_STATUS='I'
                                   AND sh.PO_NO = oh.PO_NO
                                   AND oh.status = 'A' """   

            mycursor=connection.cursor()
            mycursor.execute("desc ship_dtl")
            d_type=mycursor.fetchall()
            list_type=[]
            for col in d_type:
                if "decimal" in col[1]:
                    if "ASN_ID" in col[0]:
                        list_type.append(col[0])
            #query="select id.ITEM_PARENT from item_dtl id where "

            if(len(val_list)>0):
                val_list=str(val_list).replace('[','').replace(']','')
                query=query+"id.HIER1 IN ({}) AND ".format(val_list)

            if(len(val_list1)>0):
                val_list1=str(val_list1).replace('[','').replace(']','')
                query=query+"id.HIER2 IN ({}) AND ".format(val_list1)

            if(len(val_list2)>0):
                val_list2=str(val_list2).replace('[','').replace(']','')
                query=query+"id.HIER3 IN ({}) AND ".format(val_list2)

            if(len(val_list3)>0):
                val_list3=str(val_list3).replace('[','').replace(']','')
                query=query+"id.ITEM_PARENT IN ({}) AND ".format(val_list3)

            if(len(val_list4)>0):
                val_list4=str(val_list4).replace('[','').replace(']','')
                query=query+"id.ITEM IN ({}) AND ".format(val_list4)

            if len(val_list)==0 and len(val_list1)==0 and len(val_list2)==0 and len(val_list3)==0 and len(val_list4)==0:
                query=query
            else:
                query=query[:-4]
            query=query+' ORDER BY sdtl.ASN_ID;'
            results55=pd.read_sql(query,connection)
            res_list=[]
            rec={}
            results55 =  results55.replace(np.NaN, "NULL", regex=True)
            for val2 in results55.values:
                count=0
                val_list123=[]
                P_keys1=[]
                for col4 in results55.columns:
                    rec[col4]=val2[count]
                    val_list123.append(rec[col4])
                    count=count+1
                for col5 in list_type:
                    if col5 in rec:
                        if rec[col5]!=None or rec[col5]!="NULL":
                            rec[col5]=int(rec[col5])
                for col in results55.columns:
                    if col=="ASN_ID":
                        P_keys1.append(col)
                for key in P_keys1:
                    rec["ASN"]=rec.pop(key)
                P_keys1.clear()
                res_list.append(rec.copy()) 
            
            return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        finally:
            connection.close()

#Fetching all the column values from ASN table:
@csrf_exempt
def TSF_table(request):
    if request.method == 'POST':
        try:
            json_object = json.loads(request.body)
            val_list=[]
            val_list1=[]
            val_list2=[]
            val_list3=[]
            val_list4=[]
            k_list=[]
            k_list1=[]
            k_list2=[]
            for row in json_object:
                for col in row:
                    if col=="PACK_NO":
                        k_list.append(col)
                    if col=="SKU":
                        k_list1.append(col) 
                for k in k_list:
                    row["ITEM"]=row.pop(k)
                k_list.clear()   
                for k1 in k_list1:
                    row["ITEM"]=row.pop(k1)
                k_list1.clear()
  
            for row in json_object:
                for col in row:
                    if col=="HIER1":
                        val_list.append(row[col])
                    if col=="HIER2":
                        val_list1.append(row[col])
                    if col=="HIER3":
                        val_list2.append(row[col])
                    if col=="ITEM_PARENT":
                        val_list3.append(row[col])
                    if col=="ITEM":
                        val_list4.append(row[col])
                    if col not in ["HIER1","HIER2","HIER3","ITEM_PARENT","ITEM"]:
                    #if col=="HIER1" or col=="HIER2" or col=="HIER3" or col=="ITEM_PARENT" or col=="ITEM":
                        k_list2.append(col)
                for key in k_list2:
                    row.pop(key)
                k_list2.clear()
            if len(json_object)>=1:
                json_object=[i for i in json_object if i]
                              
            if len(json_object)==0:
                query="select distinct ts.TSF_ID from tsf_dtl ts "
            else:
                query="select distinct ts.TSF_ID from tsf_dtl ts,tsf_item_loc til,item_dtl id where ts.TSF_ID=til.TSF_ID and til.ITEM=id.ITEM and "
                    
            mycursor=connection.cursor()
            mycursor.execute("desc ship_dtl")
            d_type=mycursor.fetchall()
            list_type=[]
            for col in d_type:
                if "decimal" in col[1]:
                    if "TSF_ID" in col[0]:
                        list_type.append(col[0])
            #query="select id.ITEM_PARENT from item_dtl id where "

            if(len(val_list)>0):
                val_list=str(val_list).replace('[','').replace(']','')
                query=query+"id.HIER1 IN ({}) AND ".format(val_list)

            if(len(val_list1)>0):
                val_list1=str(val_list1).replace('[','').replace(']','')
                query=query+"id.HIER2 IN ({}) AND ".format(val_list1)

            if(len(val_list2)>0):
                val_list2=str(val_list2).replace('[','').replace(']','')
                query=query+"id.HIER3 IN ({}) AND ".format(val_list2)

            if(len(val_list3)>0):
                val_list3=str(val_list3).replace('[','').replace(']','')
                query=query+"id.ITEM_PARENT IN ({}) AND ".format(val_list3)

            if(len(val_list4)>0):
                val_list4=str(val_list4).replace('[','').replace(']','')
                query=query+"id.ITEM IN ({}) AND ".format(val_list4)

            if len(val_list)==0 and len(val_list1)==0 and len(val_list2)==0 and len(val_list3)==0 and len(val_list4)==0:
                query=query
            else:
                query=query[:-4]
            query=query+' ORDER BY ts.TSF_ID;'   
            results55=pd.read_sql(query,connection)
            res_list=[]
            rec={}
            results55 =  results55.replace(np.NaN, "NULL", regex=True)
            for val2 in results55.values:
                count=0
                val_list123=[]
                P_keys1=[]
                for col4 in results55.columns:
                    rec[col4]=val2[count]
                    val_list123.append(rec[col4])
                    count=count+1
                for col5 in list_type:
                    if col5 in rec:
                        if rec[col5]!=None or rec[col5]!="NULL":
                            rec[col5]=int(rec[col5])
                for col in results55.columns:
                    if col=="TSF_ID":
                        P_keys1.append(col)
                for key in P_keys1:
                    rec["TSF"]=rec.pop(key)
                P_keys1.clear()
                res_list.append(rec.copy()) 
            return JsonResponse(res_list,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
            connection.close()



'''
        ********************************
        ********************************
            LIKE ITEM MAPPING START
        ********************************
        ********************************
    
'''

# RETRIEVING ALLOC_ITM_SEARCH_DTL BASED ON ALLOCATION NUMBER

def establish_connection():
    global conn_global
    #if conn_global is None:
    #    conn_generator =get_mysql_conn([])
    #    conn_global =  conn_generator.__enter__()
    I_db_connect_status = list()
    I_db_connect_status.append(0)
    conn_generator =get_mysql_conn(I_db_connect_status)
    conn_global =  conn_generator.__enter__()


def retrieve_LIM(alloc_no):
    O_status = 0
    L_fun ="Retrive LIM Data"
    try:
        O_status=1
        establish_connection()
        if conn_global:
            O_status=2
            Alloc_data,Map_data, err_msg=rtv_like_item(conn_global,alloc_no)
            print("Wrap ",Alloc_data,Map_data, err_msg)
            res_list=[]
            O_status=3
            for ind in range(len([Alloc_data,Map_data])):
                if [Alloc_data,Map_data][ind].empty:
                    res_list.append([])
                else:
                    if len([Alloc_data,Map_data][ind]) > 0:
                        res_list.append([Alloc_data,Map_data][ind].replace([np.NaN, "NULL", None], "", regex=True).to_dict("records"))
                       
            if len(res_list[0])==0 and len(res_list[1])==0:
                    return [], err_msg
            else:
                return res_list, ''
        
    except Exception as error:
        err_return = L_fun+":"+str(O_status)+": "+ str(error)
        if O_status==1 :             
            print(L_fun,":",O_status,":","Exception raised when establishing connection :", error)
        elif O_status==2:
            print(L_fun,":",O_status,":","Exception raised invalid connection':", error)
        elif O_status==3:
            print(L_fun,":",O_status,":","Exception raised during conversion of dataframe:", error)
        return [], err_return

@csrf_exempt 
def getNoSkus(request):
    if request.method == 'POST':
        try:
            data=json.loads(request.body)
            data=data[0]

            I_item              = data["ITEM_PARENT"]
            I_diff_id           = data["DIFF_ID"]
            I_alloc_no          = data["ALLOC_NO"]
            I_item_list         = None
            I_size_prof_ind     = None
            I_release_date      = None
            I_uda1              = None
            I_uda1_val          = None
            I_uda2              = None
            I_uda2_val          = None
            I_uda3              = None
            I_uda3_val          = None
            if conn_global == None:
                return JsonResponse({"status": 500, "message": "NO_OF_SKUS: CONNECTION LOST"}) 
            if "ITEM_PARENT" in data and "DIFF_ID" in data:
                noSkus,err_msg=get_skus(conn_global,
                                I_item          ,
                                I_diff_id       ,
                                I_alloc_no      ,
                                I_item_list     ,
                                I_size_prof_ind ,
                                I_release_date  ,
                                I_uda1          ,
                                I_uda1_val      ,
                                I_uda2          ,
                                I_uda2_val      ,
                                I_uda3          ,
                                I_uda3_val  ) 
                print("NOSKUS : ",noSkus)
                if len(noSkus)>0:
                    return JsonResponse([noSkus[0],int(noSkus[1])], content_type="application/json",safe=False) 
                else:
                    #return JsonResponse([], content_type="application/json",safe=False) 
                    return JsonResponse({"status": 500, "message": str(err_msg)}) 
            else:
                return JsonResponse({"status": 500, "message": "NO_OF_SKUS: INVALID INPUT"}) 
        except Exception as error:
            return JsonResponse({"status": 500, "message": "NO_OF_SKUS Exception occured: "+str(error)})

@csrf_exempt 
def del_MapItems(request):
    if request.method == 'POST':
        try:
            data=json.loads(request.body)
            data=data[0]
            I_alloc = data["ALLOC_NO"]
            
            if conn_global == None:
                return JsonResponse({"status": 500, "message": "DELETE: CONNECTION LOST"}) 
            mycursor=conn_global.cursor()
            if "MAP_ITEMS" in data  and len(data["MAP_ITEMS"])>0:
                #query ="update alloc_like_item_diff_map_temp set DEL_IND ='Y' where ALLOC_NO ="+str(I_alloc)+ " and ITEM in "
                query ="update alloc_like_item_diff_map_temp set DEL_IND ='Y' where ALLOC_NO = %s and ITEM = %s and DIFF_ID = %s "
                for row in data["MAP_ITEMS"]:
                    mycursor.execute(query,(data["ALLOC_NO"],row["ITEM"],row["DIFF_ID"],))
                    print("\nUPDATE IND :: ",mycursor.rowcount)

                if mycursor.rowcount >0:
                    dataList,err_msg=delete_mapped_item(conn_global,I_alloc)
                    print("Delete ",dataList,err_msg)
                    if len(dataList) == 2:
                        Alloc_data,Map_data = dataList
                        res_list=[]
                        for ind in range(len([Alloc_data,Map_data])):
                            if [Alloc_data,Map_data][ind].empty:
                                res_list.append([])
                            else:
                                res_list.append(df_conversion([Alloc_data,Map_data][ind]))
                        if len(res_list[0])==0 and len(res_list[1])==0:
                               return JsonResponse({"status": 500, "message":  str(err_msg)})
                        else:
                            return JsonResponse(res_list, content_type="application/json",safe=False) 
                    else:
                        return JsonResponse({"status": 500, "message":  str(err_msg)})
                else:
                    return JsonResponse({"status": 500, "message": "DELETE: UPDATE DEL_IND FAILED"}) 
            else:
                return JsonResponse({"status": 500, "message": "DELETE: INVALID MAP_ITEMS"}) 
        except Exception as error:
            return JsonResponse({"status": 500, "message": "DELETE Exception occured: "+str(error)})


@csrf_exempt 
def map_AllocItems(request):
    if request.method == 'POST':
        try:
            data=json.loads(request.body)
            data=data[0]

            I_alloc             = data["ALLOC_NO"]
            I_item_list         = data["ITEM_LIST_NO"] 
            I_item_parent       = data["ITEM_PARENT"] if len(str(data["ITEM_PARENT"])) > 0 else None
            I_sku               = data["SKU"]              
            I_diff_id           = data["DIFF_ID"]  if len(str(data["DIFF_ID"]))>0 else None
            I_no_sizes          = data["NO_OF_SKUS"]
            I_weight            = data["WEIGHT"]
            I_size_prf_ind      = data["SIZE_PROFILE"]
            #print("dat)
            if conn_global == None:
                return JsonResponse({"status": 500, "message": "MAPPING: CONNECTION LOST"}) 
            mycursor = conn_global.cursor()
            
            if "ALLOC_ITEMS" in data and len(data["ALLOC_ITEMS"])>0:
                query ="update alloc_like_item_diff_temp set SEL_IND ='Y' where ALLOC_NO = %s and ITEM = %s and DIFF_ID = %s "
                for row in data["ALLOC_ITEMS"]:
                    mycursor.execute(query,(data["ALLOC_NO"],row["ITEM"],row["DIFF_ID"],))
                    print("\nUPDATE IND :: ",mycursor.rowcount)
                if mycursor.rowcount >0:
                    rt_list,err_msg=map_item(conn_global,I_alloc
                                                    ,I_item_list
                                                    ,I_item_parent         #for style diff
                                                    ,I_sku                 
                                                    ,I_diff_id             #for style diff
                                                    ,I_no_sizes            
                                                    ,I_weight              
                                                    ,I_size_prf_ind )
                    if len(rt_list) == 2:
                        Alloc_data,Map_data =rt_list
                        res_list=[]
                        for ind in range(len([Alloc_data,Map_data])):
                            if [Alloc_data,Map_data][ind].empty:
                                res_list.append([])
                            else:
                                res_list.append(df_conversion([Alloc_data,Map_data][ind]))
                        if len(res_list[0])==0 and len(res_list[1])==0:
                                return JsonResponse({"status": 500, "message": "MAPPING:No Data Found"})
                        else:
                            return JsonResponse(res_list, content_type="application/json",safe=False) 
                    else:
                        query ="update alloc_like_item_diff_temp set SEL_IND ='N' where ALLOC_NO = %s and ITEM = %s and DIFF_ID = %s ;"
                        for row in data["ALLOC_ITEMS"]:
                            mycursor.execute(query,(data["ALLOC_NO"],row["ITEM"],row["DIFF_ID"],))
                            print("\nUPDATE IND REVERT:: ",mycursor.rowcount)
                        return JsonResponse({"status": 500, "message": err_msg })
                else:
                    return JsonResponse({"status": 500, "message": "MAPPING: No Data Found"}) 
            else:
                return JsonResponse({"status": 500, "message": "MAPPING: INVALID ALLOC ITEMS"}) 
        except Exception as error:
            return JsonResponse({"status": 500, "message": "MAPPING Exception occured: "+str(error)})

@csrf_exempt
def Like_item_Insert(request):
    if request.method == 'POST':
        try:
            data=json.loads(request.body)
            data=data[0]
            if conn_global == None:
                return JsonResponse({"status": 500, "message": "CONNECTION LOST"}) 
            if len(data["MAPDATA"]) > 0 :
                mycursor = conn_global.cursor()

                # Define the columns to match
                match_columns = ['ITEM', 'DIFF_ID', 'LIKE_ITEM', 'LIKE_ITEM_DIFF_ID']

                # Read the dataframes
                df_Mapped1 = pd.DataFrame(data["MAPDATA"])
                df_Mapped2 = pd.read_sql("SELECT * FROM alloc_like_item_diff_map_temp WHERE alloc_no = %s" % str(data["ALLOC_NO"]), conn_global)

                # Merge the dataframes based on the matching columns
                merged_df = pd.merge(df_Mapped1, df_Mapped2, on=match_columns)
                # Remove rows where MAP_SIZE_PROF_IND_x == MAP_SIZE_PROF_IND_y
                merged_df = merged_df[merged_df['MAP_SIZE_PROF_IND_x'] != merged_df['MAP_SIZE_PROF_IND_y']]
                # Remove the 'MAP_SIZE_PROF_IND_y' column
                merged_df.drop('MAP_SIZE_PROF_IND_y', axis=1, inplace=True)

                # Rename the 'MAP_SIZE_PROF_IND_x' column to 'MAP_SIZE_PROF_IND'
                merged_df.rename(columns={'MAP_SIZE_PROF_IND_x': 'MAP_SIZE_PROF_IND'}, inplace=True)

                # Select necessary columns for the final dataframe
                final_columns = match_columns + ['MAP_SIZE_PROF_IND']  # Add other columns as needed
                final_df = merged_df[final_columns]
                print("FINAL_DF:\n", final_df.to_dict("records"))

                # Define the UPDATE query template
                update_query = '''UPDATE alloc_like_item_diff_map_temp SET MAP_SIZE_PROF_IND = %(MAP_SIZE_PROF_IND)s 
                                    WHERE ITEM = %(ITEM)s 
                                    AND DIFF_ID = %(DIFF_ID)s 
                                    AND LIKE_ITEM = %(LIKE_ITEM)s 
                                    AND LIKE_ITEM_DIFF_ID = %(LIKE_ITEM_DIFF_ID)s'''

                # Prepare the data for bulk update
                update_data = final_df[['MAP_SIZE_PROF_IND', 'ITEM', 'DIFF_ID', 'LIKE_ITEM', 'LIKE_ITEM_DIFF_ID']].to_dict('records')
                #print("update_data\n\n ",update_data)
                # Execute the bulk update
                mycursor.executemany(update_query, update_data)
                mycursor.close()

                test =pd.read_sql("select * from alloc_like_item_diff_map_temp where alloc_no=%s" % str(data["ALLOC_NO"]), conn_global)
                print("UPDATED TABLE : \n",test)

            if data == "CLOSE":
                return JsonResponse({"status": 200, "message": ""})  
            if "ALLOC_NO" in data:
                result,err_msg=insert_mapped_item(conn_global,data["ALLOC_NO"])
                #print("LIKE ITEM INSERT (ok) : ", result)
                if result:
                    return JsonResponse({"status": 200, "message": "Setup complete"})
                else:
                     return JsonResponse({"status": 500, "message": "INSERT : "+ str(err_msg)})   
        except Exception as error:
            return JsonResponse({"status": 500, "message": "INSERT Exception occured: "+ str(error)})
        finally:
            if conn_global != None:
                print("LIKE ITEM MAPPING : CONN CLOSED.")
                conn_global.close()

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

'''
        ********************************
        ********************************
            ALLOCATION SUMMARY
        ********************************
        ********************************
    
'''
@csrf_exempt 
def Alloc_no_ASY(request):
    if request.method == 'POST':
        try:
            data=json.loads(request.body)
            data=data[0]
            if "ALLOC_NO" in data:
                query=" select ALLOC_NO, ALLOC_DESC from alloc_head where alloc_no like %s order by alloc_no limit 50;"
                Alloc_No_data=(pd.read_sql(query,connection, params=( str(data['ALLOC_NO']) + '%',)))
                Alloc_No_data.replace(np.NaN, "", regex=True)
                Alloc_No_data = Alloc_No_data.fillna("")
                return JsonResponse(Alloc_No_data.to_dict("records"), content_type="application/json",safe=False)
            else:
                query="select t.* from (select ALLOC_NO, ALLOC_DESC from alloc_head order by cast(alloc_no as unsigned) desc limit 50) t order by cast(alloc_no as unsigned) asc ;"
                Alloc_No_data=(pd.read_sql(query,connection))
                Alloc_No_data.replace(np.NaN, "", regex=True)
                Alloc_No_data = Alloc_No_data.fillna("")
                return JsonResponse(Alloc_No_data.to_dict("records"), content_type="application/json",safe=False) 
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})

@csrf_exempt 
def Alloc_ASY(request):
    if request.method == 'GET':
        try:
            query1="SELECT DISTINCT USER_ID AS ALLOCATOR FROM alloc_users ORDER BY ALLOCATOR ASC;"
            query2="select CODE_DESC as STATUS, CODE as STATUS_CODE from code_detail where code_type='ALTS';"
            
            Allocator=(pd.read_sql(query1,connection))
            #Allocator.fillna(value="", inplace=True)
            # Remove any rows where the 'ALLOCATOR' column has null values
            Allocator.dropna(subset=['ALLOCATOR'], inplace=True)
            Alloc_status=(pd.read_sql(query2,connection))
            Alloc_status.replace(np.NaN, "", regex=True)
            response=[Allocator.to_dict("records"),Alloc_status.to_dict("records")]
            return JsonResponse(response, content_type="application/json",safe=False) 
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})


@csrf_exempt 
def Multi_PO_Create_Table(request):
    if request.method == 'POST':
        try:
            data=json.loads(request.body)
            data=data[0]
            result = [[],False]
            result, err_msg = multi_po(connection,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None)
            res_list = []
            if len(result) == 0 and len(err_msg)>0:
                return JsonResponse({"status": 500, "message":str(err_msg)})
            if len(result) > 0:
                res_list = df_conversion(result)
            return JsonResponse(res_list, content_type="application/json",safe=False)    
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
            connection.close()