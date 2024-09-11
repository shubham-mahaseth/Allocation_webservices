import os
import contextlib
#from google.cloud.sql.connector import Connector
#from mysql.connector.connection import MySQLConnection
#from mysql.connector import errorcode
from numpy import True_, true_divide
import pandas as pd
import warnings
from google.cloud.sql.connector import Connector  #Changes for google cloud
import sqlalchemy                                 #Changes for google cloud
import pymysql                                    #Changes for google cloud 
warnings.filterwarnings('ignore')


#Changes for google  cloud start
CLOUD_PROJECT_REGION_INSTANCE = 'allocation-433008:us-central1:allocation'
PYMYSQL= 'pymysql'
CLOUD_USER = 'root'
CLOUD_PASSWORD = 'Proxima360'
CLOUD_DATABASE = 'proxima360_dev'
#Changes for google  cloud end
#import mysqlx                                    #Changes for google cloud

# initialize Connector object                     #Changes for google cloud
connector = Connector()                           #Changes for google cloud

@contextlib.contextmanager
def get_mysql_conn(O_status):

    #print("CONNECTED TO DB")
    """Context manager to automatically close DB connection. We retrieve credentials from Environment variables""" 
    try:
        O_status[0]=0
        #Changes for google  cloud start
        conn: pymysql.connections.Connection = connector.connect(
            CLOUD_PROJECT_REGION_INSTANCE,
            PYMYSQL,
            user=CLOUD_USER,
            password=CLOUD_PASSWORD,
            db=CLOUD_DATABASE
        )
        #Changes for google cloud end
        #print("CONNECTED TO DB before yield")
        yield conn
        #print("CONNECTED TO DB!!")
        #return conn
        '''except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
            O_status[0]=1
            return conn
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
            O_status[0]=1
            return conn
        else:
            print(err)
            O_status[0]=1
            return conn'''
    finally:
        O_status[0]=1
        return 1