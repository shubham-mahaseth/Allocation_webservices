import json
import pandas as pd
import numpy as np
from django.db import IntegrityError
from django.http import JsonResponse,HttpResponse,StreamingHttpResponse
from django.core import serializers
from datetime import datetime,date
from django.views.decorators.csrf import csrf_exempt
from django.db import connection
from .connect import get_mysql_conn
from .Allocation_functions.Allocation.ALLOCATION_DETAILS.alloc_details_wrapper import RTV_ALLOC_DTL
from .Allocation_functions.Allocation.ALLOCATION_DETAILS.alloc_loc_spread_wrapper import sprd_alloc_loc
from .Allocation_functions.Allocation.ALLOCATION_DETAILS.update_alloc_qty_wrapper import upd_sku_calc
from .Allocation_functions.Allocation.ALLOCATION_DETAILS.update_sku_calc_qty_wrapper import func
from .Allocation_functions.Allocation.ALLOCATION_DETAILS.setup_alloc_details import update_alloc_qty_dtl
from .Allocation_functions.Allocation.ALLOCATION_DETAILS.change_alloc_qty import change_alloc_qty
from .Allocation_functions.Allocation.SIZE_DETAILS.get_size_prof_wrapper import get_size_prf

from .Allocation_functions.Allocation.ALLOCATION_DETAILS.retreive_pack_wrapper import rtv_pack_dtl
from .Allocation_functions.Allocation.ALLOCATION_DETAILS.alloc_pck_comp_dtl_wrapper import alloc_pck_com_wrapper
from .Allocation_functions.Allocation.ALLOCATION_DETAILS.alloc_pck_store_dtl_wrapper import alloc_pck_store_wrapper
from .Allocation_functions.Allocation.ALLOCATION_DETAILS.restore_pck_alloc_qty_wrapper import restore_pck_wrapper
from .Allocation_functions.Allocation.ALLOCATION_DETAILS.upd_pack_alloc_qty_wrapper import upd_pck_alloc_qty


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
def allocDetails_commit(request):
    if request.method == 'POST':
        try:
            req_type = json.loads(request.body)
            req_type = req_type[0]
            print("ALLOCATION DETAILS CONNECTION : ",req_type)
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


'''
        ********************************
        ********************************
             ALLOCATION DETAILS START 
        ********************************
        ********************************
    
'''
#VALIDATION FOR ALLOC_DETAILS SCREEN TO CHECK IF DATA IS AVAILABLE BASED ON THE ALLOC_NO
@csrf_exempt
def Alloc_dtl_validation(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            rl_query="select RULE_TYPE from alloc_rule where alloc_no=%s;"
            rule_type =(pd.read_sql(rl_query,connection,params=(data['ALLOC_NO'],))).to_dict("records");
            rule_type =rule_type[0]["RULE_TYPE"]
            print("RULE_TYPE :: ",rule_type)
            establish_connection()
            mycursor = conn_global.cursor()

            #mycursor = connection.cursor()
            if "ALLOC_NO" in data:           
                
                mycursor.execute("select * from alloc_calc_item_loc where alloc_no=%s limit 1;",(data["ALLOC_NO"],))
                mycursor.fetchone()
                #mycursor.execute("select * from alloc_calc_item_loc where alloc_no= limit 1;")
                if mycursor.rowcount >0:
                    rtvData,err_msg=RTV_ALLOC_DTL(conn_global,data["ALLOC_NO"])
                    if len(rtvData) > 0:
                        df1,df2,df3 = rtvData
                        res_list=[]
                        for ind in range(len([df1,df2,df3])):
                            if [df1,df2,df3][ind].empty:
                                res_list.append([])
                            else:
                                res_list.append((([df1,df2,df3][ind]).fillna('')).to_dict("records"))
                        if len(res_list[0])==0 and len(res_list[1])==0:
                            return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
                        else:
                            res_list.append(rule_type)
                            return JsonResponse(res_list, content_type="application/json",safe=False)
                    else:
                        return JsonResponse({"status": 500, "message": str(err_msg)})
                else:
                    return JsonResponse({"status": 501, "message":"This allocation has not been calculated. Calculate allocation in order to proceed"})
            else:
                return JsonResponse({"status": 500, "message":"ERROR : REQUIRED ALLOC_NO"})    
        except Exception as error:
            return JsonResponse({"status": 500, "message": "ALLOC DETAILS : "+str(error)})




@csrf_exempt
def spread_Alloc(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if conn_global == None:
                    return JsonResponse({"status": 500, "message": "SPREAD ALLOC: CONNECTION LOST"}) 
            mycursor = conn_global.cursor()
            if len(data["UPDATE"])>0:
                U_query = "update alloc_item_details_temp set  SPREAD_QTY=%s where ALLOC_NO= %s and WH_ID= %s and SOURCE_ITEM= %s and DIFF_ID= %s; "
                for obj in data["UPDATE"]:
                    wh          = obj["WH_ID"]
                    item        = obj["SOURCE_ITEM"]
                    alloc_no    = obj["ALLOC_NO"]
                    spread_qty  = obj["SPREAD_QTY"]
                    diff_id  = obj["DIFF_ID"]
                    mycursor.execute(U_query,(spread_qty,        
                                              alloc_no,      
                                              wh , 
                                              item,diff_id,));
                    
                if "ALLOC_NO" in data:
                    update_check,err_msg = upd_sku_calc(conn_global,data["ALLOC_NO"])
                    if update_check == True:
                        spread_check,err_msg = sprd_alloc_loc(conn_global,data["ALLOC_NO"])
                        if spread_check == True:
                            rtvData,err_msg=RTV_ALLOC_DTL(conn_global,data["ALLOC_NO"])
                            if len(rtvData) >0:
                                df1,df2,df3 = rtvData
                                res_list=[]
                                for ind in range(len([df1,df2,df3])):
                                    if [df1,df2,df3][ind].empty:
                                        res_list.append([])
                                    else:
                                        res_list.append((([df1,df2,df3][ind]).fillna('')).to_dict("records"))
                                if len(res_list[0])==0 and len(res_list[1])==0:
                                    return JsonResponse(res_list, content_type="application/json",safe=False)
                                else:
                                    return JsonResponse(res_list, content_type="application/json",safe=False) 
                            else:
                                 return JsonResponse({"status": 500, "message":str(err_msg)}) 
                        else:
                             return JsonResponse({"status": 500, "message":str(err_msg)})
                    else:
                             return JsonResponse({"status": 500, "message":str(err_msg)})
                else:
                    return JsonResponse({"status": 500, "message":"ERROR : REQUIRED ALLOC_NO"})  
            else:
                return JsonResponse({"status": 500, "message":"UPDATE FAILED"})
            
        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})


@csrf_exempt
def Update_AllocQty(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            I_alloc_no   =     data["ALLOC_NO"]      
            I_wh_id      =     data["WH_ID"]      
            I_item_id    =     data["SOURCE_ITEM"]     
            I_diff_id    =     data["DIFF_ID"]    
            I_order_no   =     None    
            I_location   =     data["LOCATION_ID"]      
            I_adj_qty    =     data["ALLOC_QTY"]

            #print("UPDATE ALLOC_QTY INPUT: \n",data)
            if conn_global == None:
                return JsonResponse({"status": 500, "message": "UPDATE ALLOC_QTY: CONNECTION LOST"}) 

            if "ALLOC_NO" in data:
                update_check,err_msg = func(conn_global,I_alloc_no,
                                                I_wh_id   ,  
                                                I_item_id ,
                                                I_diff_id ,
                                                I_order_no,
                                                I_location,
                                                I_adj_qty )
                print("\n\nALLOC QTY UPDATE RETURN :: ",update_check)
                if len(update_check) >0:
                #if update_check[-1]:
                #    update_check=list(update_check)
                #    update_check.pop()
                    return JsonResponse(update_check, content_type="application/json",safe=False)
                else:
                    return JsonResponse({"status": 500, "message":str(err_msg)}) 
                    
            else:
                return JsonResponse({"status": 500, "message":"ERROR : REQUIRED ALLOC_NO"})  
            
            
        except Exception as error:
            return JsonResponse({"status": 500, "message": "UPDATE ALLOC_QTY: "+str(error)})
 
@csrf_exempt
def Size_Profile(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            print("data",data)
            if "ALLOC_NO" in data:
                I_alloc = data["ALLOC_NO"]
                I_wh = data["WH_ID"]
                I_order_no = "null"
                I_item_id = data["SOURCE_ITEM"]
                I_diff_id = data["DIFF_ID"]
                I_location = data["LOCATION_ID"]
                df_result, err_msg = get_size_prf(connection,I_alloc,I_wh,I_order_no,I_item_id,I_diff_id,I_location)
                print("SIZE_PROFILE:",df_result)
                if len(df_result)==0:
                    if len(err_msg)>0:
                        return JsonResponse({"status": 500, "message":str(err_msg)})  
                    return JsonResponse([], content_type="application/json",safe=False)
                else:
                    df_result =  df_result.fillna('')
                    result = df_result.to_dict("records")
                    return JsonResponse(result, content_type="application/json",safe=False) 
                    
            else:
                return JsonResponse({"status": 500, "message":"ERROR : REQUIRED ALLOC_NO"})  
        except Exception as error:
            return JsonResponse({"status": 500, "message": "SIZE PROFILE: "+str(error)})


@csrf_exempt
def fetch_net_need(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if conn_global == None:
                return JsonResponse({"status": 500, "message": "COPY DOWN: CONNECTION LOST"}) 
            query='SELECT NET_NEED_IND,RULE_TYPE FROM alloc_rule WHERE ALLOC_NO = %s;'
            query1="select * from alloc_item_loc_details_temp where alloc_no = %s ORDER BY coalesce(PO_NO,'$'),SOURCE_ITEM,coalesce(DIFF_ID,'$'),GROSS_NEED,STOCK_ON_HAND + future_fulfill_qty,CAST(LOCATION_ID AS UNSIGNED);"
            query2="select * from alloc_item_loc_details_temp where alloc_no = %s ORDER BY coalesce(PO_NO,'$'),SOURCE_ITEM,coalesce(DIFF_ID,'$'),NET_NEED,CAST(LOCATION_ID AS UNSIGNED);"
            if "ALLOC_NO" in data:
                result=pd.read_sql(query,conn_global,params=(data["ALLOC_NO"],))
                result = (result.to_dict("records"))[0]
                df_rslt=[]
                if "NET_NEED_IND" in result and "RULE_TYPE" in result :
                    if result["NET_NEED_IND"]=='N' and result["RULE_TYPE"]== '7':
                        df=pd.read_sql(query1,conn_global,params=(data["ALLOC_NO"],))
                        if len(df) >0:
                            df=df.fillna("")
                            df_rslt.extend(df.to_dict("records"))
                            
                    else:
                        df=pd.read_sql(query2,conn_global,params=(data["ALLOC_NO"],))
                        if len(df) >0:
                            df=df.fillna("")
                            df_rslt.extend(df.to_dict("records"))
                
                u_chk,err_msg=update_alloc_qty_dtl(conn_global,data["ALLOC_NO"])
                #print("update_alloc_qty_dtl  op :: ", u_chk,df_rslt)
                #df_temp= list()
                if len(df_rslt)==0 or not u_chk:
                    return JsonResponse({"status": 500, "message": str(err_msg)})
                    #return JsonResponse([[],u_chk], content_type="application/json",safe=False)
                else:
                    return JsonResponse([df_rslt,u_chk], content_type="application/json",safe=False) 
        except Exception as error:
            return JsonResponse({"status": 500, "message": "fetch_net_need : "+str(error)})

@csrf_exempt
def copyD_AD(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if conn_global == None:
                return JsonResponse({"status": 500, "message": "COPY DOWN: CONNECTION LOST"}) 
            if len(data.keys())>0:
                I_alloc_no    = data["ALLOC_NO"]
                I_wh_id       = data["WH_ID"]
                I_item_id     = data["SOURCE_ITEM"]                            
                I_diff_id     = data["DIFF_ID"]
                I_order_no    = None if data["PO_NO"] == "" else data["PO_NO"]
                I_location    = data["LOCATION_ID"]
                I_alloc_qty   = int(data["ALLOC_QTY"])
                I_som_qty     = data["SOM_QTY"]

                result,err_msg=change_alloc_qty(conn_global,0
                                        ,I_alloc_no 
                                        ,I_wh_id    
                                        ,I_item_id  
                                        ,I_diff_id  
                                        ,I_order_no 
                                        ,I_location 
                                        ,I_alloc_qty
                                        ,I_som_qty ) 
                if len(result) > 0:
                    return JsonResponse(result, content_type="application/json",safe=False) 
                else:
                    return JsonResponse({"status": 500, "message": str(err_msg)})  
            else:
                return JsonResponse({"status": 500, "message":"INVALID DATA"})
            
        except Exception as error:
            return JsonResponse({"status": 500, "message": "COPY DOWN : "+str(error)})


'''
        ********************************
        ********************************
             ALLOCATION DETAILS END 
        ********************************
        ********************************
    
'''

'''
        ***************************************
        ***************************************
             ALLOCATION DETAILS PACK START
        ***************************************
        ***************************************
    
'''


@csrf_exempt
def Alloc_DPack_data(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            establish_connection()
            mycursor = conn_global.cursor()
            if "ALLOC_NO" in data:
                mycursor.execute("select * from alloc_calc_item_loc where alloc_no=%s limit 1;",(data["ALLOC_NO"],))
                mycursor.fetchone()
                #mycursor.execute("select * from alloc_calc_item_loc where alloc_no= limit 1;")
                if mycursor.rowcount >0:
                    rtv_Data, err_msg=rtv_pack_dtl(conn_global,data["ALLOC_NO"])
                    if len(rtv_Data) >0:
                        df1,df2,df3= rtv_Data
                        res_list=[]
                        #O_status=3
                        for ind in range(len([df1,df2,df3])):
                            if [df1,df2,df3][ind].empty:
                                res_list.append([])
                            else:
                                res_list.append((([df1,df2,df3][ind]).fillna('')).to_dict("records"))
                        if len(res_list[0])==0 and len(res_list[1])==0:
                            return JsonResponse({"status": 500, "message":"PLEASE RE-CALCULATE"})
                        else:
                            return JsonResponse(res_list, content_type="application/json",safe=False) 
                    else:
                        return JsonResponse({"status": 500, "message":str(err_msg)})
                else:
                    return JsonResponse({"status": 501, "message":"This allocation has not been calculated. Calculate allocation in order to proceed"})
            else:
                return JsonResponse({"status": 500, "message":"ERROR : REQUIRED ALLOC_NO"})    
        except Exception as error:
            return JsonResponse({"status": 500, "message": "ALLOC DETAILS PACK: "+str(error)})

@csrf_exempt
def AD_validation(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if conn_global == None:
                return JsonResponse({"status": 500, "message":"Connection Lost"})   
            if "ALLOC_NO" in data:
                query= "SELECT max(pack_ind) as PACK_IND FROM alloc_itm_search_dtl WHERE alloc_no=%s ;"
                pack=pd.read_sql(query,connection,params=(data["ALLOC_NO"]))
                if len(pack)==0:
                    return JsonResponse({"status": 500, "message":"PLEASE RE-CALCULATE"})
                else:
                    return JsonResponse({"status": 200, "message":"SUCCESS"}) 
                
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})

@csrf_exempt
def pck_Comp_Data(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if conn_global == None:
                return JsonResponse({"status": 500, "message":"Connection Lost"})   
            if "ALLOC_NO" in data:
                df1, err_msg = alloc_pck_com_wrapper(conn_global,data["ALLOC_NO"],data["PACK_NO"])
                if len(df1)==0:
                    #print("err_msg \n :::",err_msg)
                    if len(err_msg) >0:
                        return JsonResponse({"status": 500, "message":str(err_msg)})

                    return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
                else:
                    return JsonResponse(df1.to_dict("records"), content_type="application/json",safe=False) 
            else:
                return JsonResponse({"status": 500, "message":"ERROR : REQUIRED ALLOC_NO"})    
        except Exception as error:
            return JsonResponse({"status": 500, "message": "PACK COMP:"+str(error)})

@csrf_exempt
def pck_Store_Data(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if conn_global == None:
                return JsonResponse({"status": 500, "message":"Connection Lost"})   
            if "ALLOC_NO" in data:
                wh = data["WH_ID"] if len(str(data["WH_ID"])) > 0 else 'NULL'
                df1, err_msg=alloc_pck_store_wrapper(conn_global,data["ALLOC_NO"],data["PACK_NO"],wh)
                if len(df1)==0:
                    if len(err_msg) >0:
                        return JsonResponse({"status": 500, "message":str(err_msg)})

                    return JsonResponse({"status": 500, "message":"NO DATA FOUND"})
                else:
                    return JsonResponse(df1.to_dict("records"), content_type="application/json",safe=False) 
            else:
                return JsonResponse({"status": 500, "message":"ERROR : REQUIRED ALLOC_NO"})    
        except Exception as error:
            return JsonResponse({"status": 500, "message": "PACK STORE: "+str(error)})

@csrf_exempt
def restore_ADPk(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if conn_global == None:
                return JsonResponse({"status": 500, "message":"Connection Lost"})   
            if "ALLOC_NO" in data:
                Check, err_msg = restore_pck_wrapper(conn_global,data["ALLOC_NO"],data["WH_ID"])
                if Check:
                    return JsonResponse({"status": 200, "message":"SUCCESS"})
                else:
                    return JsonResponse({"status": 500, "message": str(err_msg)}) 
            else:
                return JsonResponse({"status": 500, "message":"ERROR : REQUIRED ALLOC_NO"})    
        except Exception as error:
            return JsonResponse({"status": 500, "message":"RESTORE: "+str(error)})
@csrf_exempt
def update_ADPk(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data=data[0]
            if conn_global == None:
                return JsonResponse({"status": 500, "message":"Connection Lost"})   
            if "ALLOC_NO" in data:
                Check, err_msg = upd_pck_alloc_qty(conn_global,data["ALLOC_NO"])
                if Check:
                    return JsonResponse({"status": 200, "message":"SUCCESS"})
                else:
                    return JsonResponse({"status": 500, "message":str(err_msg)}) 
            else:
                return JsonResponse({"status": 500, "message":"ERROR : REQUIRED ALLOC_NO"})    
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
'''
        ***************************************
        ***************************************
             ALLOCATION DETAILS PACK END 
        ***************************************
        ***************************************
    
'''


