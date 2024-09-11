from ..LIKE_ITEM.no_of_skus import no_of_skus


def get_skus(  conn,
               I_item,
               I_diff_id,
               I_alloc_no,
               I_item_list,
               I_size_prof_ind,
               I_release_date,
               I_uda1,
               I_uda1_val,
               I_uda2,
               I_uda2_val,
               I_uda3,
               I_uda3_val):

    L_func_name="get_skus"
    O_status =list()
    try:
        L_func_call = no_of_skus(conn,
                                    O_status,
                                    I_item,
                                    I_diff_id,
                                    I_alloc_no,
                                    I_item_list,
                                    I_size_prof_ind,
                                    I_release_date,
                                    I_uda1,
                                    I_uda1_val,
                                    I_uda2,
                                    I_uda2_val,
                                    I_uda3,
                                    I_uda3_val)
        return L_func_call

    except Exception as argument:
        #print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured in "+ str(argument)
        return [],err_return


'''

if __name__ == "__main__":
 I_item = '111675721'
 I_diff_id = 'CHARCOAL01'
 I_alloc_no = None
 I_item_list = None
 I_size_prof_ind = None
 I_release_date = None
 I_uda1 = None
 I_uda1_val = None
 I_uda2 = None
 I_uda2_val = None
 I_uda3 = None
 I_uda3_val = None

L_func_call = get_skus(I_item,
               I_diff_id,
               I_alloc_no,
               I_item_list,
               I_size_prof_ind,
               I_release_date,
               I_uda1,
               I_uda1_val,
               I_uda2,
               I_uda2_val,
               I_uda3,
               I_uda3_val)
print(L_func_call)

'''
