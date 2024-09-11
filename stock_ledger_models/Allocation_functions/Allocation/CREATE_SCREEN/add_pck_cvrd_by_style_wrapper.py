#from GLOBAL_FILES.get_connection import get_mysql_conn
#from CREATE_SCREEN.add_pack_coverd_by_style import pack_coverd_by_style


#def pck_coverd_by_style_wrapper():
#    L_func_name="pck_coverd_by_style_wrapper"
#    O_status =list()
#    try:
#        I_get_mysql_conn = list()
#        I_get_mysql_conn.append(0)
#        with get_mysql_conn (I_get_mysql_conn) as conn:
#            mycursor=conn.cursor()
#            mycursor.execute("SET sql_mode = ''; ")
#            L_func = pack_coverd_by_style(conn,O_status)
#            return L_func
                
#    except Exception as argument:
#        print("Exception occured in: ",L_func_name,argument)
#        return False


#if __name__ == "__main__":
#    daily_view = pck_coverd_by_style_wrapper()  
#    print(daily_view);

