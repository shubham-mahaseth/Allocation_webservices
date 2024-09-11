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
from .connect import get_mysql_conn
from .Allocation_functions.Allocation.WHATIF_SUMMARY.retreive_wisummary_details_wrapper import retreive_wis_dtls
from .Allocation_functions.Allocation.WHATIF_SUMMARY.retreive_wisummary_into_temp_wrapper import submit_wis_dtls

from .Allocation_functions.Allocation.WHATIF_SUMMARY.populate_popreview_gtt_wrapper import populate_popreview_wrapper
from .Allocation_functions.Allocation.WHATIF_SUMMARY.retreive_alloc_head_wis_wrapper import retreive_alloc_head_wrapper
from .Allocation_functions.Allocation.WHATIF_SUMMARY.create_whatif_po_wrapper import create_whatif_po_wrapper
from .Allocation_functions.Allocation.WHATIF_SUMMARY.wrapper_qty_change import po_qty_change_wrapper

conn_global =None

def establish_connection():
    global conn_global
    #if conn_global is None:
    #    conn_generator =get_mysql_conn([])
    #    conn_global =  conn_generator.__enter__()
    I_db_connect_status = list()
    I_db_connect_status.append(0)
    conn_generator =get_mysql_conn(I_db_connect_status)
    conn_global =  conn_generator.__enter__()


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



def POType_WhatIF_table(request):
    if request.method == 'GET':
        try:
            query     = """select CODE,CODE_DESC from code_detail where CODE_TYPE="APST" order by CODE_DESC;"""
            df_result = pd.read_sql(query,connection)
            result    = df_conversion(df_result)
            if len(result) == 0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                for item in result:
                    for col in item:
                        if col == "CODE_DESC" and item["CODE_DESC"] == "Direct To Store":
                            item[col] = "Direct Store Delivery"
                return JsonResponse(result,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        finally:
                connection.close()


@csrf_exempt
def Supplier_WhatIF_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data = data[0]
            
            item_vals = data["ITEM"]
            item_vals = str(item_vals).replace("[","(").replace("]",")")

            query = """select vp.SUPPLIER SUPPLIER,
                            vp.SUPPLIER_NAME SUPPLIER_DESC,
                            isp.item ITEM
                        from sups  vp, 
                            item_sups isp
                        where vp.supplier = isp.item_supp
                                and isp.item IN {} """.format(item_vals)+"""
                        order by 1;"""
            df_result = pd.read_sql(query,conn_global)
            result    = df_conversion(df_result)
            if len(result) == 0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                #result_json = json.dumps(res_list)
                return JsonResponse(result,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})



@csrf_exempt
def Origin_country_WhatIF_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data = data[0]
            item_vals = data["ITEM"]
            item_vals = str(item_vals).replace("[","(").replace("]",")")

            query = """SELECT vp.COUNTRY_ID ORIGIN_COUNTRY_ID,
                            isp.item ITEM
                          FROM country  vp, 
                               item_sup_location isp
                         WHERE isp.item IN {} """.format(item_vals)+"""
                               AND vp.country_id = isp.origin_country_id
                         ORDER BY 1;"""
            df_result = pd.read_sql(query,conn_global)
            result    = df_conversion(df_result)
            if len(result) == 0:
                return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
            else:
                #result_json = json.dumps(res_list)
                return JsonResponse(result,content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
  


@csrf_exempt
def Retrieve_WhatIF_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if data["COUNT"] == 1:
                establish_connection()
            I_search = {"I_alloc_no" : data["ALLOC_NO"],
                        "I_po_type"  : data["PO_TYPE"],
                        "I_multi_wh" : data["MULTI_WAREHOUSE"]}
            BOOL = False
            DF1,DF2,ERROR,BOOL = retreive_wis_dtls(conn_global,I_search)
            print("WI SUMMARY : \n",)
            if BOOL == True:
                df_result1 = df_conversion(DF1)
                df_result2 = df_conversion(DF2) 
                replacement_map = {
                    "CD": "Cross Dock",
                    "DSD": "Direct Store Delivery",
                    "WH": "Warehouse"
                } 
                if len(df_result1)>0:
                    for item in df_result1:
                        if "ORDER_TYPE" in item and item["ORDER_TYPE"] in replacement_map:
                            item["ORDER_TYPE"] = replacement_map[item["ORDER_TYPE"]]

                return JsonResponse([df_result1,df_result2,''],content_type="application/json",safe=False)
            else:
                if len(ERROR) > 0:
                    return JsonResponse([[],[],ERROR],content_type="application/json",safe=False)
                return JsonResponse([[],[],''],content_type="application/json",safe=False)
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})


@csrf_exempt
def Submit_WhatIF_table(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            mycursor = conn_global.cursor()
            data1 = data[0]
            data2 = data[1]
            data3 = data[2]
            create_query1 = """CREATE TEMPORARY TABLE IF NOT EXISTS alloc_wisummary_hdr_tmp2(ALLOC_NO	        VARCHAR(50),
                                                                                        DIFF_ID	                VARCHAR(10),
                                                                                        FINAL_ALLOCATION	    NUMERIC(12),
                                                                                        FINAL_PO_QTY	        NUMERIC(12),
                                                                                        FUTURE_FULFILL_QTY      NUMERIC(12),
                                                                                        LOC_TYPE	            VARCHAR(1),
                                                                                        MULTI_WH_IND	        VARCHAR(1),
                                                                                        ORDER_NO			    VARCHAR(50),
                                                                                        ORDER_TYPE		        VARCHAR(24),
                                                                                        ORIGIN_COUNTRY_ID       VARCHAR(3),
                                                                                        PO_QTY			        NUMERIC(12),
                                                                                        SEL_IND			        VARCHAR(1),
                                                                                        SOURCE_ITEM		        VARCHAR(25),
                                                                                        SOURCE_ITEM_DESC	    VARCHAR(250),
                                                                                        STOCK_ON_HAND		    NUMERIC(12),
                                                                                        SUPPLIER			    NUMERIC(10),
                                                                                        SUPPLIER_DESC		    VARCHAR(300),
                                                                                        WH_ID				    NUMERIC(10)); """

            mycursor.execute(create_query1)
            delete_query = "delete from alloc_wisummary_hdr_tmp2"
            mycursor.execute(delete_query)
            replacement_map = {
                    "Cross Dock"   :"CD",
                    "Direct Store Delivery" :"DSD",
                    "Warehouse"    :"WH"
                    } 
            if len(data1) > 0:
                for item in data1:
                    if "ORDER_TYPE" in item and item["ORDER_TYPE"] in replacement_map:
                        item["ORDER_TYPE"] = replacement_map[item["ORDER_TYPE"]]
            D_keys = []
            for row in data1:
                for col in row:
                    if row[col] == "NULL" or row[col] == None or row[col] == "" or col == "SR_NO":
                        D_keys.append(col)
                for key in D_keys:
                    row.pop(key)
                D_keys.clear()
            
            for row in data1:
                cols=",".join(map(str, row.keys()))
                v_list=[]
                val=') VALUES('
                for v in row.values():
                    if v == None:
                        val=val+'NULL,'
                    else:
                        v_list.append(v)
                        val=val+'%s,'
                val=val[:-1]+')'
                query="insert into alloc_wisummary_hdr_tmp2(" +cols + val
                mycursor.execute(query,v_list)
                conn_global.commit()

            create_query2=""" CREATE TEMPORARY TABLE IF NOT EXISTS alloc_wisummary_dtl_tmp2(ALLOC_NO	        VARCHAR(50),
                                                                                    DIFF_ID	                    VARCHAR(10),
                                                                                    FINAL_ALLOCATION	        NUMERIC(12),
                                                                                    FUTURE_FULFILL_QTY          NUMERIC(12),
                                                                                    LOC_TYPE	                VARCHAR(1),
                                                                                    MESSAGE	                    VARCHAR(1000),
                                                                                    ORDER_NO		        	VARCHAR(50),
                                                                                    PO_QTY			            NUMERIC(12),
                                                                                    SOM_QTY			            NUMERIC(20),
                                                                                    SOURCE_ITEM	                VARCHAR(25),
                                                                                    STOCK_ON_HAND	            NUMERIC(12),
                                                                                    TRAN_ITEM	                VARCHAR(25),
                                                                                    TRAN_ITEM_DESC	            VARCHAR(250),
                                                                                    WH_ID	                    NUMERIC(10)); """
            mycursor.execute(create_query2)
            delete_query="delete from alloc_wisummary_dtl_tmp2"
            mycursor.execute(delete_query)
            D_keys=[]
            for row in data2:
                for col in row:
                    if row[col]=="NULL" or row[col]==None or row[col]=="" or col=="SR_NO":
                        D_keys.append(col)
                for key in D_keys:
                    row.pop(key)
                D_keys.clear()
            
            for row in data2:
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
                query="insert into alloc_wisummary_dtl_tmp2(" +cols + val
                mycursor.execute(query,v_list)
                conn_global.commit()
            data_funct = data3[0]
            I_search = {"I_alloc_no" : data_funct["ALLOC_NO"],
			            "I_multi_wh" : data_funct["MULTI_WAREHOUSE"]}
            result,err_msg = submit_wis_dtls(conn_global,I_search)
            if result:
                return JsonResponse({"status": 200, "message": "DATA INSERTED"})
            else: 
                return JsonResponse({"status": 500, "message": "Submit function is failed. "+str(err_msg)})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
            conn_global.close()
            connection.close()


@csrf_exempt
def populatePoPreview(request):
    if request.method == 'POST':
        try:
            data  = json.loads(request.body)
            data1 = data[0]
            data2 = data[1]
            data3 = data[2]

            po_query = '''SELECT PO_QTY
                          FROM alloc_whatif_summary 
                          where alloc_no= %s and PO_QTY > 0;'''
            create_query1 = """CREATE TEMPORARY TABLE IF NOT EXISTS alloc_wisummary_hdr_tmp2(ALLOC_NO	        VARCHAR(50),
                                                                                        DIFF_ID	                VARCHAR(10),
                                                                                        FINAL_ALLOCATION	    NUMERIC(12),
                                                                                        FINAL_PO_QTY	        NUMERIC(12),
                                                                                        FUTURE_FULFILL_QTY      NUMERIC(12),
                                                                                        LOC_TYPE	            VARCHAR(1),
                                                                                        MULTI_WH_IND	        VARCHAR(1),
                                                                                        ORDER_NO			    VARCHAR(50),
                                                                                        ORDER_TYPE		        VARCHAR(24),
                                                                                        ORIGIN_COUNTRY_ID       VARCHAR(3),
                                                                                        PO_QTY			        NUMERIC(12),
                                                                                        SEL_IND			        VARCHAR(1),
                                                                                        SOURCE_ITEM		        VARCHAR(25),
                                                                                        SOURCE_ITEM_DESC	    VARCHAR(250),
                                                                                        STOCK_ON_HAND		    NUMERIC(12),
                                                                                        SUPPLIER			    NUMERIC(10),
                                                                                        SUPPLIER_DESC		    VARCHAR(300),
                                                                                        WH_ID				    NUMERIC(10)); """
            create_query2 =""" CREATE TEMPORARY TABLE IF NOT EXISTS alloc_wisummary_dtl_tmp2(ALLOC_NO	        VARCHAR(50),
                                                                                    DIFF_ID	                    VARCHAR(10),
                                                                                    FINAL_ALLOCATION	        NUMERIC(12),
                                                                                    FUTURE_FULFILL_QTY          NUMERIC(12),
                                                                                    LOC_TYPE	                VARCHAR(1),
                                                                                    MESSAGE	                    VARCHAR(1000),
                                                                                    ORDER_NO		        	VARCHAR(50),
                                                                                    PO_QTY			            NUMERIC(12),
                                                                                    SOM_QTY			            NUMERIC(20),
                                                                                    SOURCE_ITEM	                VARCHAR(25),
                                                                                    STOCK_ON_HAND	            NUMERIC(12),
                                                                                    TRAN_ITEM	                VARCHAR(25),
                                                                                    TRAN_ITEM_DESC	            VARCHAR(250),
                                                                                    WH_ID	                    NUMERIC(10)); """

            mycursor = conn_global.cursor()
            
            mycursor.execute(create_query1)
            delete_query = "delete from alloc_wisummary_hdr_tmp2"
            mycursor.execute(delete_query)
            D_keys = []
            replacement_map = {
                    "Cross Dock"   :"CD",
                    "Direct Store Delivery" :"DSD",
                    "Warehouse"    :"WH"
                    } 
            if len(data1)>0:
                for item in data1:
                    if "ORDER_TYPE" in item and item["ORDER_TYPE"] in replacement_map:
                        item["ORDER_TYPE"] = replacement_map[item["ORDER_TYPE"]]
            for row in data1:
                for col in row:
                    if row[col]=="NULL" or row[col]==None or row[col]=="" or col=="SR_NO":
                        D_keys.append(col)
                for key in D_keys:
                    row.pop(key)
                D_keys.clear()
            
            for row in data1:
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
                query="insert into alloc_wisummary_hdr_tmp2(" +cols + val
                mycursor.execute(query,v_list)
                conn_global.commit()

            #print("hghgdf: \n:",pd.read_sql("select * from alloc_wisummary_hdr_tmp",conn_global).to_dict("records"))
            mycursor.execute(create_query2)
            delete_query="delete from alloc_wisummary_dtl_tmp2"
            mycursor.execute(delete_query)
            D_keys=[]
            for row in data2:
                for col in row:
                    if row[col]=="NULL" or row[col]==None or row[col]=="" or col=="SR_NO":
                        D_keys.append(col)
                for key in D_keys:
                    row.pop(key)
                D_keys.clear()
            
            for row in data2:
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
                query="insert into alloc_wisummary_dtl_tmp2(" +cols + val
                mycursor.execute(query,v_list)
                conn_global.commit()
            data_funct = data3[0]
            I_search = {"I_alloc_no" : data_funct["ALLOC_NO"],
			            "I_multi_wh" : data_funct["MULTI_WAREHOUSE"]}
            result,err_msg = submit_wis_dtls(conn_global,I_search)
            if result:
                poPreviewData,err_msg1 =populate_popreview_wrapper(conn_global,data_funct["ALLOC_NO"])
                print("poPreviewData \n\n\n",poPreviewData)  
                if len(poPreviewData)>0:
                    count = pd.read_sql(po_query,conn_global, params=(data_funct["ALLOC_NO"],))
                    print("CREATE PO",count)
                    poPreviewData = (poPreviewData.replace(np.NaN, "", regex=True)).to_dict("records")
                    replacement_map = {
                    "CD": "Cross Dock",
                    "DSD": "Direct Store Delivery",
                    "WH": "Warehouse"
                    } 
                    if len(poPreviewData)>0:
                        for item in poPreviewData:
                            if "PO_TYPE" in item and item["PO_TYPE"] in replacement_map:
                                item["PO_TYPE"] = replacement_map[item["PO_TYPE"]]
                    return JsonResponse([poPreviewData, False if len(count)>0 else True],content_type="application/json",safe=False)
                else: 
                    return JsonResponse({"status": 500, "message": "Retrieve PO preview is failed. "+str(err_msg1)})
            else: 
                return JsonResponse({"status": 500, "message": "Submit function is failed. "+str(err_msg)})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        finally:
            #conn_global.close()
            connection.close()

@csrf_exempt
def createPO(request):
    if request.method == 'POST':
        try:
            data  = json.loads(request.body)
            data  = data[0]
            query = '''SELECT 1
                      FROM alloc_whatif_summary  
                     WHERE alloc_no= %s 
                       AND po_qty > 0 ;      
                    '''
            val_cond = pd.read_sql(query, conn_global ,params=(data["ALLOC_NO"],)) 
            if len(val_cond) >0: 
                retrieve, err_msg1 =retreive_alloc_head_wrapper(conn_global, data["ALLOC_NO"])
                if retrieve:
                    crtPoRes,err_msg2 = create_whatif_po_wrapper(conn_global, data["ALLOC_NO"],data["ALLOCATOR"])
                    print("crtPoRes : ",crtPoRes)
                    if len(crtPoRes)>0:

                        crtPoRes = (crtPoRes.replace(np.NaN, "", regex=True)).to_dict("records")
                        replacement_map = {
                        "CD" : "Cross Dock",
                        "DSD": "Direct Store Delivery",
                        "WH" : "Warehouse"
                        } 
                        if len(crtPoRes)>0:
                            for item in crtPoRes:
                                if "PO_TYPE" in item and item["PO_TYPE"] in replacement_map:
                                    item["PO_TYPE"] = replacement_map[item["PO_TYPE"]]
                        return JsonResponse(crtPoRes,content_type="application/json",safe=False)
                    else:
                        return JsonResponse({"status": 500, "message": "PO CREATE FAILED"+str(err_msg2)})
                else:
                    return JsonResponse({"status": 500, "message": "PO CREATE FAILED"+str(err_msg1)})
            else:
                return JsonResponse({"status": 500, "message": "There is no record selected to proceed."})
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        except ValueError:
            return JsonResponse({"status": 500, "message": "error"})
        finally:
            #conn_global.close()
            connection.close()

@csrf_exempt
def update_POQty(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            I_alloc_no   =     data["ALLOC_NO"]      
            I_wh_id      =     data["WH_ID"]      
            I_src_item   =     data["SOURCE_ITEM"]     
            I_tran_item  =     data["TRAN_ITEM"]     
            I_diff_id    =     data["DIFF_ID"]    
            I_som_qty    =     data["SOM_QTY"]    
            po_qty       =     data["PO_QTY"]      
            
            #print("UPDATE ALLOC_QTY INPUT: \n",data)
           
            if "ALLOC_NO" in data:
                update_check,err_msg = po_qty_change_wrapper(conn_global,
                                                        I_alloc_no,
                                                        I_wh_id   ,  
                                                        I_src_item ,
                                                        I_tran_item,
                                                        I_diff_id ,
                                                        I_som_qty,
                                                        po_qty )
                
                if update_check == True: 
                    return JsonResponse({"status": 200, "message":"PO_QTY UPDATED"})
                else:
                    return JsonResponse({"status": 500, "message":"ERROR : UPDATE FAILED. " + str(err_msg)})                     
            else:
                return JsonResponse({"status": 500, "message":"ERROR : REQUIRED ALLOC_NO."})    
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})