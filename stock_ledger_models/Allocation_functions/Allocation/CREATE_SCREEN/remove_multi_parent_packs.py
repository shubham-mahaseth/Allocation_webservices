#from pickle import TRUE
#from GLOBAL_FILES.get_connection import get_mysql_conn
#import yaml

#def remove_multi_parant_packs(conn,O_status):
#    L_func_name = "remove_multi_parant_packs"
#    try:
#        with open('GLOBAL_FILES\\pack_coverd_by_style.yaml') as fh:
#            queries      = yaml.load(fh, Loader=yaml.SafeLoader)
#        Q_delete_gtt = queries['remove_multi_parent_packs']['Q_delete_gtt']
        
#        mycursor = conn.cursor()

#        O_status = 1
#        mycursor.execute(Q_delete_gtt)
#        conn.cursor().close()
#        return True

#    except Exception as error:
#        if O_status==1:
#            print(L_func_name,":",O_status,":","Exception occured while deleting from Q_delete_gtt query  : ", error) 
#        else: 
#            print("Exception occured in: ",L_func_name.format(error),error)
#        conn.rollback()
#        return False

