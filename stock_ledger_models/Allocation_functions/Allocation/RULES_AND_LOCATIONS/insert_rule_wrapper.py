from ..RULES_AND_LOCATIONS.setup_rules_locations import insert_rule



#----------------------------------------------------------
# Function to populate items for allocation
#----------------------------------------------------------
def insert_rule_data(conn,I_rule_rec):
    L_func_name="insert_rule_data"
    O_status =list()
    try:
        L_func_call,err_msg = insert_rule(conn,
                                    I_rule_rec,
                                    O_status)
        return L_func_call,err_msg

    except Exception as argument:
        print("Exception occured in: ",L_func_name,argument)
        err_return = L_func_name+": "+"Exception occured :"+ str(argument)
        return False,err_return




#if __name__ == "__main__":
#    I_rule_rec = {"ALLOC_NO":9
#                  ,"TEMPLATE_NO":None
#                  ,"RULE_TYPE":'H'
#                  ,"RULE_LEVEL":'S'
#                  ,"EXACT_IND":'N'
#                  ,"SIZE_PROFILE_IND":'N'
#                  ,"CASCADE_IND":'Y'
#                  ,"NET_NEED_IND":'Y'
#                  ,"USE_RULE_LEVEL_ON_HAND_IND":None
#                  ,"INCLUDE_CLEARANCE_STOCK_IND":None
#                  ,"REGULAR_SALES_IND":None
#                  ,"PROMO_SALES_IND":None
#                  ,"CLEARANCE_SALES_IND":None
#                  ,"INCLUDE_INV_IN_MIN_IND":None
#                  ,"INCLUDE_INV_IN_MAX_IND":None
#                  ,"ON_ORDER_COMMIT_DATE":None
#                  ,"ON_ORDER_COMMIT_WEEKS":None
#                  ,"IWOS_WEEKS":None
#                  ,"WEEKS_THIS_YEAR":None
#                  ,"WEEKS_LAST_YEAR":None
#                  ,"WEEKS_FUTURE":None
#                  ,"START_DATE1":None
#                  ,"END_DATE1":None
#                  ,"START_DATE2":None
#                  ,"END_DATE2":None
#                  ,"CORPORATE_RULE_ID":None
#                  ,"INCLUDE_MID_TIER_ON_HAND_IND":None
#                  ,"ENFORCE_PRES_MIN_IND":None
#                  ,"LEAD_TIME_NEED_IND":None
#                  ,"LEAD_TIME_NEED_RULE_TYPE":None
#                  ,"LEAD_TIME_NEED_START_DATE":None
#                  ,"LEAD_TIME_NEED_END_DATE":None
#                  ,"CONVERT_TO_PACK":None
#                 }

#    l_func_call = insert_rule_data(I_rule_rec)    
#    print(l_func_call)


