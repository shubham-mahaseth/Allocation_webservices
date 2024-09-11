
#from GLOBAL_FILES.get_connection import get_mysql_conn
#from CREATE_SCREEN.remove_multi_parent_packs import remove_multi_parant_packs


#def remove_multi_pack_wrapper():
#    L_func_name="remove_multi_pack_wrapper"
#    O_status =list()
#    try:
#        I_get_mysql_conn = list()
#        I_get_mysql_conn.append(0)
#        with get_mysql_conn (I_get_mysql_conn) as conn:
#            mycursor=conn.cursor()
#            mycursor.execute("SET sql_mode = ''; ")
#            L_func = remove_multi_parant_packs(conn,O_status)
#            return L_func
                
#    except Exception as argument:
#        print("Exception occured in: ",L_func_name,argument)
#        return False


#if __name__ == "__main__":
#    daily_view = remove_multi_pack_wrapper()  
#    print(daily_view);

