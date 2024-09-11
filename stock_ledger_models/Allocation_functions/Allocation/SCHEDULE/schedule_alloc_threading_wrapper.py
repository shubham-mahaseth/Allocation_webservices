from ..SCHEDULE.setup_schedule_alloc import update_schedule_date
from ..SCHEDULE.create_alloc_from_parent_wrapper import create_alloc_from_parent_wrapper
from threading import Thread
import time
import threading


def run_schedule(conn):
    Lfun_name = "schedule"
    try:
        
        mycursor = conn.cursor()
        check_USD, err_msg = update_schedule_date(conn)
        if check_USD == False:
            conn.rollback()
            return False, Lfun_name+": "+ str(err_msg)

        parallel_threads = 10
        maxthread = 9
        thread_val = 0
        print(2)
        def process_thread(thread_no):
            print("thread values for process child",thread_no)
            check_CP, err_msg = create_alloc_from_parent_wrapper(conn, thread_no)
            if check_CP == False:
                print("after fun call")
                conn.rollback()
                return False, Lfun_name+": "+ str(err_msg)
            print("executed function")    

        while thread_val <= maxthread:
            if threading.active_count() <= parallel_threads:
                thread = Thread(target=process_thread, args=(thread_val,))
                thread.start()
                thread.join()
                break
            else:
                time.sleep(1)
                continue

        thread_val += 1
        print("executed success")
    except Exception as error:
        err_return = Lfun_name+": Exception occured: "+ str(error)
        print(err_return)
        conn.rollback()
        return False, err_return


#run_schedule()
















#from SCHEDULE.setup_schedule_alloc import update_schedule_date
#from GLOBAL_FILES.get_connection import get_mysql_conn
#from SCHEDULE.create_alloc_from_parent_wrapper import create_alloc_from_parent_wrapper
##from GLOBAL_FILES.listener_utility import myThread
#from threading import Thread
#import threading
#import time



#def run_schedule():
#    Lfun_name = "schedule"
#    try:
#        I_get_mysql_conn = list()
#        I_get_mysql_conn.append(0)
#        with get_mysql_conn (I_get_mysql_conn) as conn:
#            mycursor=conn.cursor()
#            if update_schedule_date(conn) == False:
#                conn.rollback()
#                return False

#            parallel_threads = 10
#            maxthread        = 9 
#            thread_val       = 0

#            while thread_val <= maxthread: #go to line
#                thread_no = thread_val
#                if create_alloc_from_parent_wrapper(conn,thread_no) == False:
#                    conn.rollback()
#                    return False

#                while thread_val >= parallel_threads:
#                    time.sleep(5)
#                    if thread_val <= maxthread:
#                        thread_no = thread_val
#                        if create_alloc_from_parent_wrapper(conn,thread_no) == False:
#                            conn.rollback()
#                            return False
#                thread_val = thread_val + 1
  
#    run_schedule()



        

                

                