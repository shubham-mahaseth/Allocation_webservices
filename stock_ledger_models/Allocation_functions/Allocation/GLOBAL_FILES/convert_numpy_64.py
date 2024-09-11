import numpy as np
import datetime

#############################################################
# Created By - Priyanshu Pandey                             #
# File Name - convert_nupy_64.py                         #
# Purpose - converting numpy_datatype to python datatype    #
#############################################################

def convert_numpy(I_var):
    L_func_name='convert_numpy'
    try:
        O_status = 0
        O_list = tuple()
        if isinstance(I_var, bool) == True:
            if I_var == None:
                O_status = 2
                return I_var
        #converting function to INT from NUMPY_INT64
        for i in range(len(I_var)):
            O_status = 3
            if isinstance(I_var[i], np.int64) == True:
                O_status = 4
                #converting function to INT from NUMPY_INT64
                np_int = np.int64(I_var[i])
                #O_list.append(np_int.item())
                O_list =  O_list + (np_int.item(),)
            elif isinstance(I_var[i], np.float64) == True:
                O_status = 5
                #converting function to FLOAT from NUMPY_FLOAT64
                if np.isnan(I_var[i]) == True:
                    L_none = None 
                    O_list =  O_list + (L_none,)
                else:
                    np_float = np.float64(I_var[i])
                    #O_list.append(np_float.item())
                    O_list =  O_list + (np_float.item(),)
            else:
                O_status = 6
                if I_var[i] ==None:
                    O_status = 7
                    L_val = None
                    O_list =  O_list + (L_val,)
                else:
                    O_status = 8
                    #O_list.append(I_var[i])
                    O_list =  O_list + (I_var[i],)
        return O_list
                
    except Exception as argument:
        emp_list = list()
        #status
        print(O_status,"Exception occured in: ",L_func_name,argument)
        #print(L_func_name,O_status)
        return emp_list

