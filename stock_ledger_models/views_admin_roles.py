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

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
import base64
import bcrypt

# Use the same secret key (in bytes) as in React for decryption
SECRET_KEY = "Allocation_Encrpytion_Proxima360"
SECRET_KEY_BYTES = SECRET_KEY.encode('utf-8')

def decrypt_password(encrypted_password):
    try: 
        # AES encryption is base64-encoded, so decode it first
        encrypted_password_bytes = base64.b64decode(encrypted_password)        
        # Create a Cipher for AES decryption (using ECB mode for simplicity)
        cipher = Cipher(algorithms.AES(SECRET_KEY_BYTES), modes.ECB(), backend=default_backend())
        decryptor = cipher.decryptor()
        # Decrypt the padded ciphertext
        padded_plain_text = decryptor.update(encrypted_password_bytes) + decryptor.finalize()
        # Unpad the plaintext (AES block size is 128 bits = 16 bytes)
        unpadder = padding.PKCS7(128).unpadder()
        plain_text_bytes = unpadder.update(padded_plain_text) + unpadder.finalize()
        return plain_text_bytes.decode('utf-8')

    except Exception as e:
        print("Error during decryption:", str(e))
        return None
    
@csrf_exempt
def encrypt_userdata(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            print("data: ",data)
            mycursor = connection.cursor()
            
            if len(data) > 0:
                for row in data:
                    user_name = row['USERNAME']
                    password  = row['PASSWORD']
                
                    print("Data:1:",user_name,password,"\n")
                
                    #encrypted_password = make_password(password)
                    
                    encrypted_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
                    
                    encrypted_password_str = encrypted_password.decode('utf-8')
                
                    query = 'update alloc_users set password = "{}"'.format(str(encrypted_password_str))+" where user_id = '{}' ;".format(user_name)
                    
                    print(query)
                    mycursor.execute(query)
            
                return JsonResponse({"status": 200, "message": "PASSWORD ENCRYPTED SUCCESSFUL"}) 
            else:
                return JsonResponse({"status": 500, "message": "NO DATA FOUND"})  
            
            # salt = bcrypt.gensalt()
            # hashed_password = bcrypt.hashpw(password.encode('utf-8'), salt)
            
            # # Encrypt the password
            # encrypted_password = make_password(password)
            
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
        

@csrf_exempt
def decrypt_userdata(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            print("data: ",data)
            mycursor = connection.cursor()
            
            if len(data) > 0:
                data = data[0]
                user_name = data['USERNAME']
                password  = decrypt_password(data['PASSWORD'])
                # query = 'select * from alloc_users where user_id = %s'
                query = """
                                SELECT au.user_id,
                                       au.user_name,
                                       au.password AS PASSWORD,
                                       au.role_id,
                                       ar.role_name
                                 FROM  alloc_users au
                                       LEFT JOIN alloc_roles ar
                                              ON ar.role_id = au.role_id
                                WHERE  au.user_id = %s;
                                """
                result = pd.read_sql(query,connection,params=(user_name,))
                if len(result) > 0:
                    result_list = result.to_dict("records")
                    result_dict = result_list[0]
                    user_val    = result_dict["role_id"]
                    print("user_val :",user_val)
                    if user_val == 0 :
                        return JsonResponse({"status": 500, "message": "This user has not been mapped to any roles."})
                    encrypted_password = result_dict.pop('PASSWORD')
                    print(result_dict)
                    if bcrypt.checkpw(password.encode('utf-8'), encrypted_password.encode('utf-8')):                        
                        # # print(query%(data["USER"],))
                        # result = pd.read_sql(query,connection,params=(user_name,))
                        # if len(result) > 0:
                        #     user_data = result.to_dict("records")
                        #     print("user_data: ",user_data)
                        return JsonResponse([{"AUTH": True,"INFO":[result_dict]}], content_type="application/json",safe=False)
                    else:
                        return JsonResponse([{"AUTH": False,"INFO":[]}], content_type="application/json",safe=False)
                else:
                    return JsonResponse({"status": 500, "message": "NO DATA FOUND"})  
            else:
                return JsonResponse({"status": 500, "message": "NO DATA FOUND"})              
        except Exception as error:
            return JsonResponse({"status": 500, "message":str(error)})
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

@csrf_exempt
def userRegistration(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            data = data[0]     
            username  = data['USERNAME']
            fname     = data['FNAME']
            lname     = data['LNAME']
            mail      = data['MAIL']
            password  = decrypt_password(data['PASSWORD'])
            
            mycursor = connection.cursor()
            encrypted_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
                    
            encrypted_password_str = encrypted_password.decode('utf-8')

            query = '''INSERT INTO alloc_users(USER_ID,USER_NAME,FIRST_NAME,LAST_NAME,MAIL,PASSWORD,ROLE_ID,CREATE_ID) 
                                        VALUES(%s,%s,%s,%s,%s,%s,'0','SYSTEM'); 
                    '''
            mycursor.execute(query,(username,username,fname,lname,mail,encrypted_password_str))

            return JsonResponse({"status": 200, "message": "USER REGISTERED SUCCESSFULLY"}, content_type="application/json", status=200,safe=False)

        except Exception as error:
            return JsonResponse({"status": 500, "message": str(error)})
        finally:
            connection.commit()        