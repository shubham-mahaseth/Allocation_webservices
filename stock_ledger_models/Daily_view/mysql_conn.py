import os
import contextlib
import mysql.connector
from mysql.connector.connection import MySQLConnection
from mysql.connector import errorcode
from numpy import True_, true_divide
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
#from django.db import connection


os.environ['MYSQL_HOST'] = 'localhost'
os.environ['MYSQL_USER'] = 'root'
os.environ['MYSQL_PASSWORD'] = 'Satya@6650'
os.environ['MYSQL_PORT'] = '3306'
##os.environ['DATABASE'] = 'stck'
import mysqlx

@contextlib.contextmanager
def get_mysql_conn(O_status):

    #print("CONNECTED TO DB")
    """Context manager to automatically close DB connection. We retrieve credentials from Environment variables""" 
    try:
        O_status[0]=0
        
        conn = mysql.connector.connect(host=os.environ.get('MYSQL_HOST'),
                                       user=os.environ.get('MYSQL_USER'),
                                       password=os.environ.get('MYSQL_PASSWORD'),
                                       database='allocation')
        #print("CONNECTED TO DB before yield")
        yield conn
        #print("CONNECTED TO DB!!")
        #return conn
    except Exception as err:
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
            return conn