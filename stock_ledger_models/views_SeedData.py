from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json
import datetime
from django.db import connection

@csrf_exempt
def insert_into_hier1(request):
    if request.method == 'POST':
        try:
            req_data = json.loads(request.body)            
            data = req_data[0]['TABLE_DATA']
            user = req_data[0]['USER']
            
            mycursor = connection.cursor()   
            print("data seed Hier1:", data)
            required_columns = ["HIER1", "HIER1_DESC", "ACC_METHOD", "PURCHASE_TYPE", "CREATE_ID", "CREATE_DATETIME"]

            for record in data:
                # Check if all required columns are present in the current record
                if not all(column in record for column in required_columns):
                    missing_columns = [col for col in required_columns if col not in record]
                    connection.rollback()
                    return JsonResponse({"status": 500, "message": f"Missing required columns in one or more records: {', '.join(missing_columns)}"})

                # Prepare the SQL insert statement
                insert_query = """
                INSERT INTO hier1 (HIER1, HIER1_DESC, ACC_METHOD, PURCHASE_TYPE, CREATE_ID, CREATE_DATETIME)
                VALUES (%s, %s, %s, %s, %s, %s)
                """

                # Execute the insert query for the current record
                mycursor.execute(insert_query, (
                    record["HIER1"],
                    record["HIER1_DESC"],
                    record["ACC_METHOD"],
                    record["PURCHASE_TYPE"],
                    user,
                    datetime.datetime.now()  # Use current datetime if not provided
                ))

            connection.commit()
            return JsonResponse({"status": 201, "message": "Data Inserted Successfully"})

        except Exception as error:
            connection.rollback()
            return JsonResponse({"status": 500, "message": str(error)})

        finally:
            connection.close()
            
@csrf_exempt
def insert_into_hier2(request):
    if request.method == 'POST':
        try:
            req_data = json.loads(request.body)            
            data = req_data[0]['TABLE_DATA']
            user = req_data[0]['USER']
            
            mycursor = connection.cursor()   
            print("data seed hier2:", data)
            required_columns = ["HIER1", "HIER2", "HIER2_DESC", "CREATE_ID", "CREATE_DATETIME"]

            for record in data:
                # Check if all required columns are present in the current record
                if not all(column in record for column in required_columns):
                    missing_columns = [col for col in required_columns if col not in record]
                    connection.rollback()
                    return JsonResponse({"status": 500, "message": f"Missing required columns in one or more records: {', '.join(missing_columns)}"})

                # Prepare the SQL insert statement
                insert_query = """
                INSERT INTO hier2 (HIER1,HIER2, HIER2_DESC, CREATE_ID, CREATE_DATETIME)
                VALUES (%s, %s, %s, %s, %s)
                """
                
                # Execute the insert query for the current record
                mycursor.execute(insert_query, (
                    record["HIER1"],
                    record["HIER2"],
                    record["HIER2_DESC"],
                    user,
                    datetime.datetime.now()  # Use current datetime if not provided
                ))

            connection.commit()
            return JsonResponse({"status": 201, "message": "Data Inserted Successfully"})

        except Exception as error:
            connection.rollback()
            return JsonResponse({"status": 500, "message": str(error)})

        finally:
            connection.close()
            

@csrf_exempt
def insert_into_hier3(request):
    if request.method == 'POST':
        try:
            req_data = json.loads(request.body)            
            data = req_data[0]['TABLE_DATA']
            user = req_data[0]['USER']
            
            mycursor = connection.cursor()   
            print("data seed Hier3:", data)
            required_columns = ["HIER1", "HIER2","HIER3", "HIER3_DESC", "CREATE_ID", "CREATE_DATETIME"]

            for record in data:
                # Check if all required columns are present in the current record
                if not all(column in record for column in required_columns):
                    missing_columns = [col for col in required_columns if col not in record]
                    connection.rollback()
                    return JsonResponse({"status": 500, "message": f"Missing required columns in one or more records: {', '.join(missing_columns)}"})

                # Prepare the SQL insert statement
                insert_query = """
                INSERT INTO hier3 (HIER1,HIER2, HIER3, HIER3_DESC, CREATE_ID, CREATE_DATETIME)
                VALUES (%s, %s, %s, %s, %s, %s)
                """

                # Execute the insert query for the current record
                mycursor.execute(insert_query, (
                    record["HIER1"],
                    record["HIER2"],
                    record["HIER3"],
                    record["HIER3_DESC"],
                    user,
                    datetime.datetime.now()  # Use current datetime if not provided
                ))

            connection.commit()
            return JsonResponse({"status": 201, "message": "Data Inserted Successfully"})

        except Exception as error:
            connection.rollback()
            return JsonResponse({"status": 500, "message": str(error)})

        finally:
            connection.close()
            
@csrf_exempt
def insert_into_item_dtl(request):
    if request.method == 'POST':
        try:
            req_data = json.loads(request.body)            
            data = req_data[0]['TABLE_DATA']
            user = "'"+req_data[0]['USER']+"'"
            
            mycursor = connection.cursor()   
            print("data seed Item_dtl:", data)
            required_columns =   [ 
                                   "ITEM",                  "ITEM_PARENT",          "ITEM_GRANDPARENT",             "PACK_IND", 
                                   "SIMPLE_PACK_IND",       "PACK_TYPE",            "ITEM_LEVEL",                   "TRAN_LEVEL",
                                   "ITEM_AGGREGATE_IND",    "DIFF1",                "DIFF2",                        "DIFF3",
                                   "DIFF4",                 "AGGR_DIFF_ID",         "AGGR_DIFF_TYPE",               "AGGR_DIFF_COLUMN",
                                   "HIER1",                 "HIER2",                "HIER3",                        "STATUS",
                                   "ITEM_DESC",             "SELLING_UOM",          "STORE_ORD_MULT",               "SELLABLE_IND", 
                                   "ORDERABLE_IND",         "ORIGINAL_COST",        "ORIGINAL_RETAIL",              "COST_ZONE_GROUP_ID", 
                                   "RETAIL_ZONE_GROUP_ID",  "ORDER_AS_TYPE",        "CONTAINS_INNER_IND",           "USER_ATTR_1", 
                                   "USER_ATTR_VAL_1",       "USER_ATTR_2",          "USER_ATTR_VAL_2",              "USER_ATTR_3", 
                                   "USER_ATTR_VAL_3",       "CREATE_ID",            "CREATE_DATETIME",              "LAST_UPDATE_ID", 
                                   "UPDATE_DATETIME",       "UOM_CONV_FACTOR"
                                 ]
            for record in data:
                # Check if all required columns are present in the current record
                if not all(column in record for column in required_columns):
                    missing_columns = [col for col in required_columns if col not in record]
                    connection.rollback()
                    return JsonResponse({"status": 500, "message": f"Missing required columns in one or more records: {', '.join(missing_columns)}"})
                
                # Check and replace empty values with 'NULL'
                # for column in required_columns:
                #     if isinstance(record.get(column, ''), str) and len(record.get(column, '').strip()) == 0:
                #         record[column] = "NULL"
                #     elif isinstance(record.get(column, ''), str) and len(record.get(column, '').strip())  0:
                #         record[column] = "'"+record[column]+"'"
                
                for column in required_columns:
                    value = record.get(column, '')

                    if isinstance(value, (int, float)):
                        # Leave the value as is if it's an integer or a float
                        continue
                    elif isinstance(value, str):
                        stripped_value = value.strip()
        
                        # Check if the string can be converted to an integer or a float
                        if stripped_value.isdigit():  # It's an integer
                            record[column] = int(stripped_value)
                        else:
                            try:
                                float_value = float(stripped_value)
                                record[column] = float_value  # It's a float
                            except ValueError:
                                if len(stripped_value) == 0:
                                    record[column] = "NULL"
                                else:
                                    record[column] = f"'{stripped_value}'"


                # Prepare the SQL insert statement
                insert_query = """
                INSERT INTO item_dtl (ITEM,                  ITEM_PARENT,          ITEM_GRANDPARENT,             PACK_IND, 
                                   SIMPLE_PACK_IND,       PACK_TYPE,            ITEM_LEVEL,                   TRAN_LEVEL,
                                   ITEM_AGGREGATE_IND,    DIFF1,                DIFF2,                        DIFF3,
                                   DIFF4,                 AGGR_DIFF_ID,         AGGR_DIFF_TYPE,               AGGR_DIFF_COLUMN,
                                   HIER1,                 HIER2,                HIER3,                        STATUS,
                                   ITEM_DESC,             SELLING_UOM,          STORE_ORD_MULT,               SELLABLE_IND, 
                                   ORDERABLE_IND,         ORIGINAL_COST,        ORIGINAL_RETAIL,              COST_ZONE_GROUP_ID, 
                                   RETAIL_ZONE_GROUP_ID,  ORDER_AS_TYPE,        CONTAINS_INNER_IND,           USER_ATTR_1, 
                                   USER_ATTR_VAL_1,       USER_ATTR_2,          USER_ATTR_VAL_2,              USER_ATTR_3, 
                                   USER_ATTR_VAL_3,       CREATE_ID,            CREATE_DATETIME,              LAST_UPDATE_ID, 
                                   UPDATE_DATETIME,       UOM_CONV_FACTOR)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s)
                """
                date_time ="CURDATE()"
                # print("QUERY",insert_query%(
                #                     record["ITEM"],                     record["ITEM_PARENT"],               record["ITEM_GRANDPARENT"],
                #                     record["PACK_IND"],                 record["SIMPLE_PACK_IND"],           record["PACK_TYPE"],
                #                     record["ITEM_LEVEL"],               record["TRAN_LEVEL"],                record["ITEM_AGGREGATE_IND"],
                #                     record["DIFF1"],                    record["DIFF2"],                     record["DIFF3"],                                   
                #                     record["DIFF4"],                    record["AGGR_DIFF_ID"],              record["AGGR_DIFF_TYPE"],
                #                     record["AGGR_DIFF_COLUMN"],         record["HIER1"],                     record["HIER2"],
                #                     record["HIER3"],                    record["STATUS"],                    record["ITEM_DESC"],
                #                     record["SELLING_UOM"],              record["STORE_ORD_MULT"],            record["SELLABLE_IND"],
                #                     record["ORDERABLE_IND"],            record["ORIGINAL_COST"],             record["ORIGINAL_RETAIL"],
                #                     record["COST_ZONE_GROUP_ID"],       record["RETAIL_ZONE_GROUP_ID"],      record["ORDER_AS_TYPE"],
                #                     record["CONTAINS_INNER_IND"],       record["USER_ATTR_1"],               record["USER_ATTR_VAL_1"],
                #                     record["USER_ATTR_2"],              record["USER_ATTR_VAL_2"],           record["USER_ATTR_3"],
                #                     record["USER_ATTR_VAL_3"],          user,                                date_time,
                #                     user,                               date_time,             record["UOM_CONV_FACTOR"] 
                # ))
                # print("oack_ind :::::::::",record["PACK_IND"]);
                # Execute the insert query for the current record
                mycursor.execute(insert_query % (
                                    record["ITEM"],                     record["ITEM_PARENT"],               record["ITEM_GRANDPARENT"],
                                    record["PACK_IND"],                 record["SIMPLE_PACK_IND"],           record["PACK_TYPE"],
                                    record["ITEM_LEVEL"],               record["TRAN_LEVEL"],                record["ITEM_AGGREGATE_IND"],
                                    record["DIFF1"],                    record["DIFF2"],                     record["DIFF3"],                                   
                                    record["DIFF4"],                    record["AGGR_DIFF_ID"],              record["AGGR_DIFF_TYPE"],
                                    record["AGGR_DIFF_COLUMN"],         record["HIER1"],                     record["HIER2"],
                                    record["HIER3"],                    record["STATUS"],                    record["ITEM_DESC"],
                                    record["SELLING_UOM"],              record["STORE_ORD_MULT"],            record["SELLABLE_IND"],
                                    record["ORDERABLE_IND"],            record["ORIGINAL_COST"],             record["ORIGINAL_RETAIL"],
                                    record["COST_ZONE_GROUP_ID"],       record["RETAIL_ZONE_GROUP_ID"],      record["ORDER_AS_TYPE"],
                                    record["CONTAINS_INNER_IND"],       record["USER_ATTR_1"],               record["USER_ATTR_VAL_1"],
                                    record["USER_ATTR_2"],              record["USER_ATTR_VAL_2"],           record["USER_ATTR_3"],
                                    record["USER_ATTR_VAL_3"],          user,                                date_time,
                                    user,                               date_time,             record["UOM_CONV_FACTOR"] 
                ))

            connection.commit()
            return JsonResponse({"status": 201, "message": "Data Inserted Successfully"})

        except Exception as error:
            connection.rollback()
            return JsonResponse({"status": 500, "message": str(error)})

        finally:
            connection.close()

