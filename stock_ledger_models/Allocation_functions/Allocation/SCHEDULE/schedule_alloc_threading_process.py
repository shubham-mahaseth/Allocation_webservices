from ..SCHEDULE.setup_schedule_alloc import update_schedule_date
from ..SCHEDULE.create_alloc_from_parent_wrapper import create_alloc_from_parent_wrapper
from threading import Thread
import time
import threading




def run_schedule(conn):
    Lfun_name = "SCHEDULE"
    try:
        print(1)
        I_get_mysql_conn = list()
        I_get_mysql_conn.append(0)
        mycursor = conn.cursor()
        
        result, err_msg = update_schedule_date(conn)
        if result == False:
            conn.rollback()
            return False,err_msg

        print("executed update_schedule_date program success ")
        parallel_threads = 10
        maxthread = 9
        thread_val = 0
        print(2)
        def process_thread(thread_no):
            print("thread values for process child",thread_no)
            result, err_msg = create_alloc_from_parent_wrapper(conn, thread_no)
            if result== False:
                print("after fun call")
                conn.rollback()
                return False,err_msg #'ERROR: CREATE_ALLOC_FROM_PARENT_WRAPPER'
            print("executed function")  
                
        threads = []

        while thread_val <= maxthread:
            if threading.active_count() <= parallel_threads:
                thread = Thread(target=process_thread, args=(thread_val,))
                thread.start()
                threads.append(thread)
                thread.join()
                thread_val += 1
            else:
                time.sleep(1)
        L_return = 'EXECUTED SUCCESS'
        return True,L_return
    except Exception as error:
        print("EXCEPTION OCCURRED:", error)
        conn.rollback()
        return False, Lfun_name+ " - EXCEPTION OCCURRED:"+ str(error)

         