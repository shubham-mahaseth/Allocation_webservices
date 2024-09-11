import os
import contextlib
import mysql.connector
from mysql.connector import errorcode
import warnings
warnings.filterwarnings('ignore')

#############################################################
#Created By - Priyanshu Pandey                              #
#File Name - get_connection.py                              #
#Purpose - Establish database connection                    #
#############################################################

#------------------------------------------
os.environ['MYSQL_HOST']     = '35.200.198.56'#'10.37.20.162'
os.environ['MYSQL_USER']     = 'root'
os.environ['MYSQL_PASSWORD'] = 'Proxima360'#'admin'
os.environ['MYSQL_PORT']     = '3306'
os.environ['MYSQL_DB']       = 'proxima360_tst'
#------------------------------------------

#----------------------------------------------------------
# Function to Connect to your MYSQL DB
#----------------------------------------------------------
@contextlib.contextmanager
def get_mysql_conn(O_status):
    try:
        L_func_name = "get_mysql_conn"
        O_status=0
        conn = mysql.connector.connect(host=os.environ.get('MYSQL_HOST'),
                                       user=os.environ.get('MYSQL_USER'),
                                       password=os.environ.get('MYSQL_PASSWORD'),
                                       database=os.environ.get('MYSQL_DB'))
        yield conn
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print(L_func_name," Something is wrong with your user name or password")
            O_status=1
            return O_status
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print(L_func_name," Database does not exist")
            O_status=2
            return O_status
        else:
            print(err)
            O_status=4
            return O_status